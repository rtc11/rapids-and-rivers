package no.tordly.rapidsandrivers

class Problems {
    private val problems = mutableListOf<Problem>()

    fun info(explanation: String) = problems.add(Problem(explanation, Level.INFO)).let {}
    fun warn(explanation: String) = problems.add(Problem(explanation, Level.WARN)).let {}
    fun error(explanation: String) = problems.add(Problem(explanation, Level.ERROR)).let {}
    fun severe(explanation: String) = problems.add(Problem(explanation, Level.SEVERE)).let {}

    internal fun hasErrors(): Boolean = problems.any { it.level in listOf(Level.SEVERE, Level.ERROR) }

    data class Problem(val explanation: String, val level: Level)
    enum class Level { INFO, WARN, ERROR, SEVERE }

    override fun toString(): String {
        fun String.append(messages: List<Problem>): String = if (messages.isEmpty()) this else {
            val explanations = messages.joinToString("\n\t") { it.explanation }
            "$this \n ${messages.first().level}: \n\t$explanations"
        }

        return if (problems.isEmpty()) "No problems detected." else "Problems detected: (${problems.size})"
            .append(problems.filter { it.level == Level.SEVERE })
            .append(problems.filter { it.level == Level.ERROR })
            .append(problems.filter { it.level == Level.WARN })
            .append(problems.filter { it.level == Level.INFO })
    }
}
