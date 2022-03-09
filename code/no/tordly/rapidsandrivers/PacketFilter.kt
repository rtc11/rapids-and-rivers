package no.tordly.rapidsandrivers

class PacketFilter {
    private val validations = mutableListOf<Validation>()

    fun require(vararg keys: String) {
        validations.add(Validation.RequiredKeys(keys.toSet()))
    }

    fun forbid(vararg keys: String) {
        validations.add(Validation.ForbiddenKeys(keys.toSet()))
    }

    fun interestedIn(vararg keys: String) {
        validations.add(Validation.InterestingKeys(keys.toSet()))
    }

    fun <V : Any> requiredValue(key: String, value: V) {
        validations.add(Validation.RequiredValue(key, value))
    }

    internal fun validate(packet: Packet) = validations.forEach { it.validate(packet) }

    private sealed interface Validation {
        fun validate(packet: Packet)

        class RequiredKeys(private val keys: Set<String>) : Validation {
            override fun validate(packet: Packet) = packet.require(keys)
        }

        class ForbiddenKeys(private val keys: Set<String>) : Validation {
            override fun validate(packet: Packet) = packet.forbid(keys)
        }

        class InterestingKeys(private val keys: Set<String>) : Validation {
            override fun validate(packet: Packet) = packet.interestedIn(keys)
        }

        class RequiredValue(private val key: String, private val value: Any) : Validation {
            override fun validate(packet: Packet) = packet.requireValue(key, value)
        }
    }

    companion object {
        fun create(block: PacketFilter.() -> Unit): PacketFilter = PacketFilter().apply(block)
    }
}
