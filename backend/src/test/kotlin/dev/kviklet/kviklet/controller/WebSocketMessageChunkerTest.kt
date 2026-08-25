package dev.kviklet.kviklet.controller

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dev.kviklet.kviklet.service.dto.LiveSessionId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WebSocketMessageChunkerTest {
    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `small messages are sent unchanged`() {
        val message = StatusMessage(
            sessionId = LiveSessionId("session"),
            consoleContent = "SELECT 1",
            observers = emptyList(),
            ref = "ref",
        )

        val serialized = objectMapper.writeValueAsString(message)

        assertEquals(listOf(serialized), WebSocketMessageChunker.encode(message, objectMapper))
    }

    @Test
    fun `large messages are split into safe frames and reconstruct exactly`() {
        val message = StatusMessage(
            sessionId = LiveSessionId("session"),
            consoleContent = "INSERT INTO test VALUES ('${"x".repeat(8_000)}')",
            observers = emptyList(),
            ref = "ref",
        )
        val frames = WebSocketMessageChunker.encode(message, objectMapper)

        assertTrue(frames.size > 1)
        assertTrue(frames.all { it.length <= MAX_WEBSOCKET_MESSAGE_SIZE })

        val chunkNodes = frames.map(objectMapper::readTree)
        assertTrue(chunkNodes.all { it.get("type").asText() == "chunk" })
        assertEquals(
            chunkNodes.map { it.get("messageId").asText() }.toSet().size,
            1,
        )

        val reconstructed = chunkNodes
            .sortedBy { it.get("index").asInt() }
            .joinToString("") { it.get("payload").asText() }
        assertEquals(objectMapper.writeValueAsString(message), reconstructed)
    }
}
