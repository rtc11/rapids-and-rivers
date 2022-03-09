package no.tordly.kafka

import java.util.*

data class KafkaConfig(
    val brokers: String,
    val topic: String,
    val app: String,
    val security: Boolean,
    val truststorePath: String,
    val keystorePath: String,
    val credstorePsw: String,
) {
    companion object {
        fun localRapid(app: String = UUID.randomUUID().toString()) =
            KafkaConfig(
                "localhost:9092",
                "rapid",
                app = app,
                false,
                "",
                "",
                ""
            )
    }
}
