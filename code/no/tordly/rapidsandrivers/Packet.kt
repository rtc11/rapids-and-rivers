package no.tordly.rapidsandrivers

import com.google.gson.Gson

class Packet(private val jsonMap: MutableMap<String, *>) {
    private val recognizedKeys: MutableMap<String, Any> = mutableMapOf()
    internal val problems = Problems()

    fun require(keys: Set<String>) = keys.forEach { key ->
        when (!isKeyEmpty(key) && jsonMap[key] != null) {
            true -> addAccessor(key)
            false -> problems.error("Missing required key '$key'")
        }
    }

    fun forbid(keys: Set<String>) = keys.forEach { key ->
        when (isKeyMissing(key)) {
            true -> addAccessor(key)
            false -> problems.error("Forbidden key '$key' already defined")
        }
    }

    fun interestedIn(keys: Set<String>) = keys.forEach(::addAccessor)

    fun <V : Any> requireValue(key: String, value: V) = when (isKeyMissing(key) || jsonMap[key] != value) {
        true -> problems.error("Required value missing or incompatible for cast ${jsonMap[key]} != $value")
        false -> addAccessor(key)
    }

    operator fun get(key: String): Any? = recognizedKeys[key]
    operator fun set(key: String, value: Any) = recognizedKeys.put(key, value)

    fun getList(solutionsKey: String): List<*> = get(solutionsKey) as List<*>

    fun toJson(): String = Gson().toJson(jsonMap + recognizedKeys)

    internal fun hasErrors(): Boolean = problems.hasErrors()

    private fun addAccessor(key: String) {
        if (!recognizedKeys.containsKey(key) && jsonMap.containsKey(key))
            recognizedKeys[key] = jsonMap.getValue(key)!!
    }

    private fun isKeyEmpty(key: String): Boolean = when (val value = jsonMap[key]) {
        is String -> value.isEmpty()
        is Collection<*> -> value.isEmpty()
        else -> false
    }

    private fun isKeyMissing(key: String): Boolean = jsonMap[key] == null || isKeyEmpty(key)
}
