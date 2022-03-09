package no.tordly.rapids

import no.tordly.rapidsandrivers.Problems
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.test.assertContains

internal class ProblemTest {
    @Test
    fun noProblemsFoundDefault() {
        val problems = Problems()
        assertFalse(problems.hasErrors())
    }

    @Test
    fun errorsDetected() {
        val problems = Problems()
        problems.error("Simple error")
        assertTrue(problems.hasErrors())
        assertContains(problems.toString(), "Simple error")
    }

    @Test
    fun severeErrorsDetected() {
        val problems = Problems()
        problems.severe("Severe error")
        assertTrue(problems.hasErrors())
        assertContains(problems.toString(), "Severe error")
    }

    @Test
    fun warningsDetected() {
        val problems = Problems()
        problems.warn("Warning explanation")
        assertFalse(problems.hasErrors())
        assertContains(problems.toString(), "Warning explanation")
    }

    @Test
    fun informationalMessagesDetected() {
        val problems = Problems()
        problems.warn("Information only message")
        assertFalse(problems.hasErrors())
        assertContains(problems.toString(), "Information only message")
    }
}
