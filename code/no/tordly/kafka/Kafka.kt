package no.tordly.kafka

import com.google.gson.Gson
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
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
            KafkaConfig("localhost:9092", "rapid", app, false, "", "", "")
    }
}

interface Kafka {
    fun createConsumer(): Consumer<String, MutableMap<String, *>>
    fun createProducer(): Producer<String, MutableMap<String, *>>
    val topic: String
}

class KafkaRapid(private val config: KafkaConfig = KafkaConfig.localRapid()) : Kafka {
    override val topic: String = config.topic

    override fun createConsumer(): Consumer<String, MutableMap<String, *>> =
        KafkaConsumer(rapidsConsumerProperties(config), StringDeserializer(), MapDeserializer)

    override fun createProducer(): Producer<String, MutableMap<String, *>> =
        KafkaProducer(producerProperties(config), StringSerializer(), MapSerializer)

    private fun rapidsConsumerProperties(config: KafkaConfig) = aivenProperties(config) + mapOf(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to config.brokers,
        CommonClientConfigs.CLIENT_ID_CONFIG to "${config.app}-consumer",
        ConsumerConfig.GROUP_ID_CONFIG to "${config.app}-${UUID.randomUUID()}",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "true",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "latest",
    )

    private fun producerProperties(config: KafkaConfig) = aivenProperties(config) + mapOf(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to config.brokers,
        CommonClientConfigs.CLIENT_ID_CONFIG to "${config.app}-producer",
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "true",
    )

    private fun aivenProperties(config: KafkaConfig) = mutableMapOf<String, String>().apply {
        if (config.security) putAll(
            mapOf(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SSL",
                SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG to "JKS",
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to config.truststorePath,
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to config.credstorePsw,
                SslConfigs.SSL_KEYSTORE_TYPE_CONFIG to "PKCS12",
                SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to config.keystorePath,
                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to config.credstorePsw,
                SslConfigs.SSL_KEY_PASSWORD_CONFIG to config.credstorePsw,
                SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG to "",
            )
        )
    }

    object MapDeserializer : Deserializer<MutableMap<String, *>> {
        private val gson: Gson = Gson()
        override fun deserialize(topic: String, data: ByteArray?): MutableMap<String, *>? =
            data?.decodeToString()?.let {
                gson.fromJson<MutableMap<String, Any>>(it, MutableMap::class.java)
            }
    }

    object MapSerializer : Serializer<MutableMap<String, *>> {
        private val gson: Gson = Gson()
        override fun serialize(topic: String, data: MutableMap<String, *>?): ByteArray =
            gson.toJson(data).toByteArray()
    }
}
