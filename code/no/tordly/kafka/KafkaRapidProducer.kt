package no.tordly.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

open class KafkaRapidProducer(private val kafka: Kafka) {
    private val producer = kafka.createProducer()

    fun send(data: MutableMap<String, *>) {
        val record = ProducerRecord(kafka.topic, UUID.randomUUID().toString(), data)
        producer.send(record) { _, exception ->
            exception?.let { println("error $exception") } ?: println("$record")
        }
    }
}
