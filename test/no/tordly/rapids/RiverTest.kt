package no.tordly.rapids

import com.google.gson.Gson
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import no.tordly.kafka.Kafka
import no.tordly.kafka.KafkaRapid
import no.tordly.kafka.KafkaRiverFlow
import no.tordly.kafka.KafkaRapidProducer
import no.tordly.rapidsandrivers.Packet
import no.tordly.rapidsandrivers.PacketFilter
import no.tordly.rapidsandrivers.Problems
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.*

internal class RiverTest {

    @Test
    fun `valid json extracted`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {}
        rapid.produce(SOLUTION_STRING)
        assertFalse(river.packetProblem.hasErrors())
        river.close()
    }

    @Test
    fun `required key exists`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter() = PacketFilter.create {
                require("need")
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertEquals("car_rental_offer", river.packet["need"])
        river.close()
    }

    @Test
    fun `missing required key`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter() = PacketFilter.create {
                require("missing key")
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertTrue(river.error.hasErrors())
        assertContains(river.error.toString(), "Missing required key 'missing key'")
        river.close()
    }

    @Test
    fun `required key changeable`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter(): PacketFilter = PacketFilter.create {
                require("need")
            }

            override suspend fun packet(packet: Packet, producer: KafkaRapidProducer, warnings: Problems) {
                assertEquals("car_rental_offer", packet["need"])
                packet["need"] = "airline_offer"
                assertEquals("airline_offer", packet["need"])
                super.packet(packet, producer, warnings) // call super for deferred packet to be populated
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertFalse(river.packet.hasErrors())
        river.close()
    }

    @Test
    fun `forbidden field changeable`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter(): PacketFilter = PacketFilter.create {
                forbid("key_to_be_added")
            }

            override suspend fun packet(packet: Packet, producer: KafkaRapidProducer, warnings: Problems) {
                assertNull(packet["key_to_be_added"])
                packet["key_to_be_added"] = "Bingo!"
                assertEquals("Bingo!", packet["key_to_be_added"])
                super.packet(packet, producer, warnings) // call super for deferred packet to be populated
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertFalse(river.packet.hasErrors())
        river.close()
    }

    @Test
    fun `empty string passes forbidden`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter(): PacketFilter = PacketFilter.create {
                forbid("frequent_renter")
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertFalse(river.packet.hasErrors())
        river.close()
    }

    @Test
    fun `empty array passes forbidden`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter(): PacketFilter = PacketFilter.create {
                forbid("contributing_services")
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertFalse(river.packet.hasErrors())
        river.close()
    }

    @Test
    fun `forbidden field rejected`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter(): PacketFilter = PacketFilter.create {
                forbid("need")
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertTrue(river.error.hasErrors())
        river.close()
    }

    @Test
    fun `empty array fails require`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter(): PacketFilter = PacketFilter.create {
                require("contributing_services")
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertTrue(river.error.hasErrors())
        river.close()
    }

    @Test
    fun `empty string fails require`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter(): PacketFilter = PacketFilter.create {
                require("frequent_renter")
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertTrue(river.error.hasErrors())
        river.close()
    }

    @Test
    fun `interesting fields identified`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter(): PacketFilter = PacketFilter.create {
                interestedIn("frequent_renter")
            }

            override suspend fun packet(packet: Packet, producer: KafkaRapidProducer, warnings: Problems) {
                assertFalse(warnings.hasErrors())
                packet["frequent_renter"] = "interesting value"
                assertEquals("interesting value", packet["frequent_renter"])
                super.packet(packet, producer, warnings)
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertFalse(river.packet.hasErrors())
        river.close()
    }

    @Test
    fun `changed key json`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter(): PacketFilter = PacketFilter.create {
                require("need")
            }

            override suspend fun packet(packet: Packet, producer: KafkaRapidProducer, warnings: Problems) {
                packet["need"] = "airline_offer"

                val expected: String = SOLUTION_STRING
//                    .replace("2", "3") // auto incremental system read count not implemented
                    .replace("car_rental_offer", "airline_offer")

                assertJsonEquals(expected, packet.toJson())

                super.packet(packet, producer, warnings)
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertFalse(river.packet.hasErrors())
        river.close()
    }

    @Test
    fun `manipulating json arrays`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter(): PacketFilter = PacketFilter.create {
                require("solutions")
            }

            override suspend fun packet(packet: Packet, producer: KafkaRapidProducer, warnings: Problems) {
                val solutions = packet.getList("solutions")
                assertEquals(3, solutions.size)
                super.packet(packet, producer, warnings)
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertFalse(river.packet.hasErrors())
        river.close()
    }

    @Test
    fun `require value`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun filter(): PacketFilter = PacketFilter.create {
                requiredValue("user_id", 456.0) // GSON uses doubles as default
            }
        }
        rapid.produce(SOLUTION_STRING)
        assertFalse(river.packet.hasErrors())
        river.close()
    }

    @Test
    @Ignore("not implemented yet")
    fun `read count added if missing`() {
        val rapid = TestRapid()
        val river = object : TestRiver(rapid) {
            override suspend fun packet(packet: Packet, producer: KafkaRapidProducer, warnings: Problems) {
                assertFalse(warnings.hasErrors())
                assertEquals(0.0, packet["system_read_count"])
                super.packet(packet, producer, warnings)
            }
        }
        rapid.produce("{}")
        assertFalse(river.packet.hasErrors())
        river.close()
    }

    private fun assertJsonEquals(expected: String, actual: String) {
        Assertions.assertEquals(json(expected), json(actual))
    }

    private fun json(jsonString: String): Map<*, *> {
        return Gson().fromJson(jsonString, HashMap::class.java)
    }

    private abstract class TestRiver(rapid: Kafka) : KafkaRiverFlow(rapid) {
        private val errorDeferred = CompletableDeferred<Problems>()
        private val packetProblemDeferred = CompletableDeferred<Problems>()
        private val packetDeferred = CompletableDeferred<Packet>()

        // Used for assertion
        val error: Problems get() = runBlocking { withTimeout(500) { errorDeferred.await() } }
        val packetProblem: Problems get() = runBlocking { withTimeout(500) { packetProblemDeferred.await() } }
        val packet: Packet get() = runBlocking { withTimeout(500) { packetDeferred.await() } }

        override suspend fun filter(): PacketFilter = PacketFilter()
        override suspend fun packet(packet: Packet, producer: KafkaRapidProducer, warnings: Problems) {
            packetDeferred.complete(packet)
            println("test packet: ${packet.toJson()}")

            packetProblemDeferred.complete(warnings)
            println("test packet warnings: $warnings")
        }

        override suspend fun error(producer: KafkaRapidProducer, errors: Problems) {
            errorDeferred.complete(errors)
            println("test errors: $errors")
        }
    }

    private class TestRapid(override val topic: String = "hello-rapid") : Kafka {
        override fun createConsumer() = consumer
        override fun createProducer() = MockProducer(true, StringSerializer(), KafkaRapid.MapSerializer)

        private val consumer = RapidMockConsumer()

        fun produce(json: String) = consumer.produceConsumerRecord(json)

        // Had some problems with MockConsumer regarding consumer.subscribe and manual repartitioning
        inner class RapidMockConsumer : MockConsumer<String, MutableMap<String, *>>(EARLIEST) {
            private val gson = Gson()
            private var produced: ConsumerRecord<String, MutableMap<String, *>>? = null

            fun produceConsumerRecord(json: String) {
                val jsonMap = gson.fromJson<MutableMap<String, *>>(json, MutableMap::class.java)
                produced = ConsumerRecord(topic, 0, 0, "random", jsonMap)
            }

            override fun poll(timeout: Duration): ConsumerRecords<String, MutableMap<String, *>> {
                val partition = TopicPartition(topic, 0)
                return ConsumerRecords(mutableMapOf(partition to listOfNotNull(produced))).also { produced = null }
            }
        }
    }
}

@Language("json")
private val SOLUTION_STRING = """
   {
   	"need": "car_rental_offer",
   	"user_id": 456,
   	"solutions": [{
   			"offer": "15% discount"
   		},
   		{
   			"offer": "500 extra points"
   		},
   		{
   			"offer": "free upgrade"
   		}
   	],
   	"frequent_renter": "",
   	"system_read_count": 2,
   	"contributing_services": []
   }
""".trimIndent()
