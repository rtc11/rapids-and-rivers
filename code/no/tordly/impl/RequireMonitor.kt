package no.tordly.impl

import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import no.tordly.kafka.KafkaRapid
import no.tordly.kafka.KafkaRiverFlow
import no.tordly.kafka.KafkaRapidProducer
import no.tordly.rapidsandrivers.Packet
import no.tordly.rapidsandrivers.PacketFilter
import no.tordly.rapidsandrivers.Problems
import java.util.*

fun main() {
    RequireMonitor
    HandleSøknad
    Producer
}

object RequireMonitor : KafkaRiverFlow() {
    override suspend fun filter() = PacketFilter.create {
        require("personident")
    }

    override suspend fun packet(packet: Packet, producer: KafkaRapidProducer, warnings: Problems) {
        println("monitor: ${packet.toJson()}")
    }

    override suspend fun error(producer: KafkaRapidProducer, errors: Problems) {
        println("monitor: $errors")
    }
}

object HandleSøknad : KafkaRiverFlow() {
    override suspend fun filter() = PacketFilter.create {
        requiredValue("need", "soknad")
    }

    override suspend fun packet(packet: Packet, producer: KafkaRapidProducer, warnings: Problems) {
        println("handle-soknad: ${packet.toJson()}")
    }

    override suspend fun error(producer: KafkaRapidProducer, errors: Problems) {
        println("handle-soknad: $errors")
    }
}

object Producer : KafkaRapidProducer(KafkaRapid()) {
    init {
        runBlocking {
            launch {
                while (isActive) {
                    val keyValues = mutableMapOf(
                        "need" to "soknad",
                        "id" to UUID.randomUUID()
                    )
                    send(keyValues)
                    delay(5000)
                }
            }
        }
    }
}
