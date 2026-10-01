package dev.kviklet.kviklet.controller

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import dev.kviklet.kviklet.service.dto.LiveSessionId
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.util.Base64
import java.util.UUID
import java.util.zip.GZIPOutputStream

// Keep each outbound frame comfortably below the WebSocket container's default
// text-message buffer. Large SQL editor contents are split at the application
// layer instead of forcing every deployment to raise that buffer globally.
internal const val MAX_WEBSOCKET_MESSAGE_SIZE = 4 * 1024
internal const val MAX_CHUNK_PAYLOAD_SIZE = 512
internal const val MAX_CHUNK_COUNT = 10_000
internal const val MAX_PENDING_CHUNK_MESSAGES = 16
internal const val MAX_CHUNK_MESSAGE_ID_LENGTH = 128
internal const val MAX_REASSEMBLED_MESSAGE_SIZE = MAX_CHUNK_COUNT * MAX_CHUNK_PAYLOAD_SIZE
internal const val MAX_GZIP_RESPONSE_CHUNK_COUNT = 400_000
internal const val MAX_GZIP_RESPONSE_BYTES = MAX_GZIP_RESPONSE_CHUNK_COUNT * MAX_CHUNK_PAYLOAD_SIZE / 4 * 3
internal const val MAX_EXPANDED_RESPONSE_BYTES = 200 * 1024 * 1024

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ResponseChunkMessage(
    val type: String = "chunk",
    override val sessionId: LiveSessionId,
    val messageId: String,
    val index: Int,
    val total: Int,
    val payload: String,
    val encoding: String? = null,
) : ResponseMessage(sessionId)

internal class WebSocketResponseTooLargeException :
    IllegalArgumentException(
        "Response exceeds the live-session size limit. Narrow the query or download the result.",
    )

// Bound serialization before allocating a complete uncompressed JSON string.
// Retain only a small prefix so normal responses can keep their existing wire format.
internal class LimitedResponseOutputStream(private val output: OutputStream, private val limit: Int) : OutputStream() {
    var size = 0
        private set
    var smallMessage: ByteArrayOutputStream? = ByteArrayOutputStream()
        private set

    override fun write(value: Int) = write(byteArrayOf(value.toByte()), 0, 1)

    override fun write(bytes: ByteArray, offset: Int, length: Int) {
        if (length > limit - size) throw WebSocketResponseTooLargeException()
        size += length
        if (size > MAX_WEBSOCKET_MESSAGE_SIZE) smallMessage = null
        smallMessage?.write(bytes, offset, length)
        output.write(bytes, offset, length)
    }

    override fun flush() = output.flush()
    override fun close() = output.close()
}

internal data class WebSocketChunkBuffer(
    val total: Int,
    val chunks: Array<String?>,
    var received: Int = 0,
    var payloadSize: Int = 0,
)

internal object WebSocketMessageAssembler {
    fun append(buffers: MutableMap<String, WebSocketChunkBuffer>, chunk: ChunkMessage): String? {
        validate(chunk)

        val buffer = buffers[chunk.messageId] ?: run {
            if (buffers.size >= MAX_PENDING_CHUNK_MESSAGES) {
                buffers.keys.firstOrNull()?.let(buffers::remove)
            }
            WebSocketChunkBuffer(
                total = chunk.total,
                chunks = arrayOfNulls(chunk.total),
            ).also { buffers[chunk.messageId] = it }
        }

        if (buffer.total != chunk.total) {
            buffers.remove(chunk.messageId)
            throw IllegalArgumentException("WebSocket message chunk count changed")
        }

        val existingPayload = buffer.chunks[chunk.index]
        if (existingPayload != null) {
            if (existingPayload != chunk.payload) {
                buffers.remove(chunk.messageId)
                throw IllegalArgumentException("WebSocket message chunk payload changed")
            }
        } else {
            val newPayloadSize = buffer.payloadSize + chunk.payload.length
            if (newPayloadSize > MAX_REASSEMBLED_MESSAGE_SIZE) {
                buffers.remove(chunk.messageId)
                throw IllegalArgumentException("WebSocket message is too large")
            }
            buffer.chunks[chunk.index] = chunk.payload
            buffer.received += 1
            buffer.payloadSize = newPayloadSize
        }

        if (buffer.received < buffer.total) {
            return null
        }

        buffers.remove(chunk.messageId)
        return buffer.chunks.joinToString(separator = "")
    }

    private fun validate(chunk: ChunkMessage) {
        require(chunk.messageId.isNotBlank() && chunk.messageId.length <= MAX_CHUNK_MESSAGE_ID_LENGTH) {
            "Invalid WebSocket message chunk id"
        }
        require(chunk.total in 1..MAX_CHUNK_COUNT) {
            "Invalid WebSocket message chunk count"
        }
        require(chunk.index in 0 until chunk.total) {
            "Invalid WebSocket message chunk index"
        }
        require(chunk.payload.length <= MAX_CHUNK_PAYLOAD_SIZE) {
            "Invalid WebSocket message chunk payload"
        }
    }
}

internal object WebSocketMessageChunker {
    fun encode(message: ResponseMessage, objectMapper: ObjectMapper, gzip: Boolean = false): List<String> {
        val serialized: String
        if (gzip) {
            val compressed = ByteArrayOutputStream()
            val compressedOutput = LimitedResponseOutputStream(compressed, MAX_GZIP_RESPONSE_BYTES)
            val output = LimitedResponseOutputStream(GZIPOutputStream(compressedOutput), MAX_EXPANDED_RESPONSE_BYTES)
            try {
                output.use { objectMapper.writeValue(it, message) }
            } catch (error: JsonMappingException) {
                if (generateSequence<Throwable>(error) { it.cause }.any { it is WebSocketResponseTooLargeException }) {
                    throw WebSocketResponseTooLargeException()
                }
                throw error
            }
            output.smallMessage?.let { return listOf(it.toString(Charsets.UTF_8)) }
            serialized = Base64.getEncoder().encodeToString(compressed.toByteArray())
        } else {
            serialized = objectMapper.writeValueAsString(message)
            if (serialized.length <= MAX_WEBSOCKET_MESSAGE_SIZE) return listOf(serialized)
            if (serialized.length > MAX_REASSEMBLED_MESSAGE_SIZE) throw WebSocketResponseTooLargeException()
        }

        val messageId = UUID.randomUUID().toString()
        val payloads = serialized.chunked(MAX_CHUNK_PAYLOAD_SIZE)
        return payloads.mapIndexed { index, payload ->
            objectMapper.writeValueAsString(
                ResponseChunkMessage(
                    sessionId = message.sessionId,
                    messageId = messageId,
                    index = index,
                    total = payloads.size,
                    payload = payload,
                    encoding = if (gzip) "gzip" else null,
                ),
            )
        }
    }
}
