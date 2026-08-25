package dev.kviklet.kviklet.controller

import com.fasterxml.jackson.databind.ObjectMapper
import dev.kviklet.kviklet.service.dto.LiveSessionId
import java.util.UUID

// Keep each outbound frame comfortably below the WebSocket container's default
// text-message buffer. Large SQL editor contents are split at the application
// layer instead of forcing every deployment to raise that buffer globally.
internal const val MAX_WEBSOCKET_MESSAGE_SIZE = 4 * 1024
internal const val MAX_CHUNK_PAYLOAD_SIZE = 512

data class ChunkMessage(
    val type: String = "chunk",
    override val sessionId: LiveSessionId,
    val messageId: String,
    val index: Int,
    val total: Int,
    val payload: String,
) : ResponseMessage(sessionId)

internal object WebSocketMessageChunker {
    fun encode(message: ResponseMessage, objectMapper: ObjectMapper): List<String> {
        val serialized = objectMapper.writeValueAsString(message)
        if (serialized.length <= MAX_WEBSOCKET_MESSAGE_SIZE) {
            return listOf(serialized)
        }

        val messageId = UUID.randomUUID().toString()
        val payloads = serialized.chunked(MAX_CHUNK_PAYLOAD_SIZE)
        return payloads.mapIndexed { index, payload ->
            objectMapper.writeValueAsString(
                ChunkMessage(
                    sessionId = message.sessionId,
                    messageId = messageId,
                    index = index,
                    total = payloads.size,
                    payload = payload,
                ),
            )
        }
    }
}
