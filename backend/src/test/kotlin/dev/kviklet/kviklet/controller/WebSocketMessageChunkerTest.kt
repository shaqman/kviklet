package dev.kviklet.kviklet.controller

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import dev.kviklet.kviklet.service.dto.LiveSessionId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.zip.GZIPInputStream

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

    @Test
    fun `gzip response beyond the legacy limit reconstructs exact UTF-8 JSON`() {
        val message = StatusMessage(
            sessionId = LiveSessionId("session"),
            consoleContent = "result 😀\n".repeat(700_000),
            observers = emptyList(),
            ref = "ref",
        )
        val serialized = objectMapper.writeValueAsString(message)
        assertTrue(serialized.length > MAX_REASSEMBLED_MESSAGE_SIZE)
        val frames = WebSocketMessageChunker.encode(message, objectMapper, gzip = true)
        assertTrue(frames.size < MAX_CHUNK_COUNT)
        assertTrue(frames.all { it.length <= MAX_WEBSOCKET_MESSAGE_SIZE })
        val nodes = frames.map(objectMapper::readTree)
        assertTrue(nodes.all { it.get("encoding").asText() == "gzip" })
        val compressed = Base64.getDecoder().decode(nodes.joinToString("") { it.get("payload").asText() })
        val expanded = GZIPInputStream(ByteArrayInputStream(compressed)).use {
            it.readBytes().toString(Charsets.UTF_8)
        }
        // Jackson's UTF-8 generator escapes supplementary Unicode characters
        // differently from its String generator. Compare the actual wire bytes
        // and parsed JSON so both serialization fidelity and content are checked.
        assertEquals(objectMapper.writeValueAsBytes(message).toString(Charsets.UTF_8), expanded)
        assertEquals(objectMapper.readTree(serialized), objectMapper.readTree(expanded))
        assertThrows(WebSocketResponseTooLargeException::class.java) {
            WebSocketMessageChunker.encode(message, objectMapper)
        }
    }

    @Test
    fun `gzip capable clients keep small responses unchanged`() {
        val message = ErrorResponseMessage(sessionId = LiveSessionId("session"), error = "error")
        assertEquals(
            listOf(objectMapper.writeValueAsString(message)),
            WebSocketMessageChunker.encode(message, objectMapper, gzip = true),
        )
    }

    @Test
    fun `serialization limit counts bytes and rejects overflow before writing`() {
        val bytes = ByteArrayOutputStream()
        val output = LimitedResponseOutputStream(bytes, 4)
        output.write("😀".toByteArray(Charsets.UTF_8))
        assertEquals(4, output.size)
        assertThrows(WebSocketResponseTooLargeException::class.java) { output.write(1) }
        assertEquals(4, bytes.size())
    }
}
