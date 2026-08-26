package dev.kviklet.kviklet.controller

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import dev.kviklet.kviklet.service.dto.LiveSessionId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
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

    @Test
    fun `request chunks are reassembled before message decoding`() {
        val serialized = """
            {"type":"update_content","content":"${"INSERT INTO test VALUES ('x');\\n".repeat(1000)}","ref":"ref"}
        """.trimIndent()
        val payloads = serialized.chunked(MAX_CHUNK_PAYLOAD_SIZE)
        val buffers = linkedMapOf<String, WebSocketChunkBuffer>()

        val reconstructed = payloads
            .mapIndexed { index, payload ->
                ChunkMessage(
                    messageId = "request",
                    index = index,
                    total = payloads.size,
                    payload = payload,
                )
            }
            .reversed()
            .mapNotNull { WebSocketMessageAssembler.append(buffers, it) }
            .single()

        assertEquals(serialized, reconstructed)
        assertEquals(
            UpdateContentMessage::class,
            objectMapper.readValue<WebSocketMessage>(reconstructed)::class,
        )
        assertTrue(buffers.isEmpty())
    }

    @Test
    fun `request chunk wire messages decode as WebSocket messages`() {
        val decoded = objectMapper.readValue<WebSocketMessage>(
            """{"type":"chunk","messageId":"request","index":0,"total":1,"payload":"{}"}""",
        )

        assertEquals(ChunkMessage::class, decoded::class)
    }

    @Test
    fun `request chunks reject oversized payloads`() {
        assertThrows(IllegalArgumentException::class.java) {
            WebSocketMessageAssembler.append(
                linkedMapOf(),
                ChunkMessage(
                    messageId = "request",
                    index = 0,
                    total = 1,
                    payload = "x".repeat(MAX_CHUNK_PAYLOAD_SIZE + 1),
                ),
            )
        }
    }
}
