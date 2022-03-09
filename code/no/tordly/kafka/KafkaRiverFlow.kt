package no.tordly.kafka

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import no.tordly.rapidsandrivers.Packet
import no.tordly.rapidsandrivers.PacketFilter
import no.tordly.rapidsandrivers.Problems
import java.time.Duration

abstract class KafkaRiverFlow(
    private val kafka: Kafka = KafkaRapid(),
    context: CoroutineDispatcher = Dispatchers.Default
) : AutoCloseable {
    private val rapidConsumer = kafka.createConsumer()
    private val rapidProducer = KafkaRapidProducer(kafka)

    abstract suspend fun filter(): PacketFilter
    abstract suspend fun packet(packet: Packet, producer: KafkaRapidProducer, warnings: Problems)
    abstract suspend fun error(producer: KafkaRapidProducer, errors: Problems)

    private fun poll(): Flow<Packet> = flow {
        try {
            rapidConsumer.subscribe(listOf(kafka.topic)).also { println("subscribed to ${kafka.topic}") }

            while (currentCoroutineContext().isActive) {
                rapidConsumer.poll(Duration.ofMillis(100))
                    .onEach { println("$it") }
                    .map { Packet(it.value()) }
                    .forEach { emit(it) }
            }
        } finally {
            rapidConsumer.unsubscribe().also { println("unsubscribed from ${kafka.topic}") }
        }
    }

    private val job = CoroutineScope(context).launch {
        while (isActive) {
            runCatching {
                poll().onEach(filter()::validate).collect {
                    when (it.hasErrors()) {
                        true -> error(rapidProducer, it.problems)
                        false -> packet(it, rapidProducer, it.problems)
                    }
                }
            }.onFailure {
                println("Failed to read topic ${kafka.topic}, $it")
                if (it is CancellationException) throw it
                delay(1_000)
            }
        }
    }

    override fun close() {
        if (!job.isCompleted) runBlocking { job.cancelAndJoin() }
        rapidConsumer.close()
    }
}
