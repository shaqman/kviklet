package dev.kviklet.kviklet.controller

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import dev.kviklet.kviklet.db.User
import dev.kviklet.kviklet.db.UserId
import dev.kviklet.kviklet.security.UserDetailsWithId
import dev.kviklet.kviklet.service.UserService
import dev.kviklet.kviklet.service.dto.DBExecutionResult
import dev.kviklet.kviklet.service.dto.ExecutionRequestId
import dev.kviklet.kviklet.service.dto.LiveSession
import dev.kviklet.kviklet.service.dto.LiveSessionId
import dev.kviklet.kviklet.service.websocket.SessionService
import org.slf4j.LoggerFactory
import org.springframework.security.access.AccessDeniedException
import org.springframework.security.concurrent.DelegatingSecurityContextExecutorService
import org.springframework.security.core.context.SecurityContext
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Component
import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.handler.TextWebSocketHandler
import org.springframework.web.util.UriComponentsBuilder
import org.springframework.web.util.UriUtils
import java.util.LinkedHashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(value = UpdateContentMessage::class, name = "update_content"),
    JsonSubTypes.Type(value = ExecuteMessage::class, name = "execute"),
    JsonSubTypes.Type(value = CancelMessage::class, name = "cancel"),
    JsonSubTypes.Type(value = ChunkMessage::class, name = "chunk"),
)
sealed class WebSocketMessage

data class UpdateContentMessage(val content: String, val ref: String) : WebSocketMessage()

data class ExecuteMessage(val statement: String) : WebSocketMessage()

object CancelMessage : WebSocketMessage()

data class ChunkMessage(val messageId: String, val index: Int, val total: Int, val payload: String) : WebSocketMessage()

sealed class ResponseMessage(open val sessionId: LiveSessionId)
data class ErrorResponseMessage(val type: String = "error", override val sessionId: LiveSessionId, val error: String) :
    ResponseMessage(sessionId)

data class StatusMessage(
    val type: String = "status",
    override val sessionId: LiveSessionId,
    val consoleContent: String,
    val observers: List<UserResponse>,
    val ref: String,
) : ResponseMessage(sessionId)

data class ResultMessage(
    val type: String = "result",
    override val sessionId: LiveSessionId,
    val results: List<ExecutionResultResponse>,
    val event: EventResponse? = null,
) : ResponseMessage(sessionId)

data class SessionObserver(val webSocketSession: WebSocketSession, val user: User)

@Component
class SessionWebsocketHandler(
    private val sessionService: SessionService,
    private val objectMapper: ObjectMapper,
    private val userService: UserService,
) : TextWebSocketHandler() {
    private val logger = LoggerFactory.getLogger(SessionWebsocketHandler::class.java)
    private val sessionObservers = ConcurrentHashMap<LiveSessionId, MutableSet<SessionObserver>>()
    private val sessionToLiveSessionMap = ConcurrentHashMap<String, LiveSessionId>()
    private val requestChunkBuffers = ConcurrentHashMap<String, MutableMap<String, WebSocketChunkBuffer>>()

    // Executor that propagates SecurityContext to background threads
    private val queryExecutor = DelegatingSecurityContextExecutorService(
        Executors.newCachedThreadPool(),
    )

    override fun afterConnectionEstablished(session: WebSocketSession) {
        val requestId = extractRequestId(session)
        val securityContext = session.attributes["SPRING_SECURITY_CONTEXT"] as? SecurityContext
        if (securityContext != null) {
            SecurityContextHolder.setContext(securityContext)
        } else {
            throw IllegalStateException("Security context not found in WebSocket session")
        }
        val liveSession = sessionService.createOrConnectToSession(
            ExecutionRequestId(requestId),
        )
        sessionToLiveSessionMap[session.id] = liveSession.id!!
        val principal = SecurityContextHolder.getContext().authentication.principal
        val userDetailsWithId = when (principal) {
            is UserDetailsWithId -> principal
            else -> throw IllegalStateException("Expected UserDetailsWithId but got: ${principal.javaClass}")
        }
        logger.info("User id: ${userDetailsWithId.id}")
        val user = userService.getUser(UserId(userDetailsWithId.id))

        sessionObservers.computeIfAbsent(liveSession.id) { ConcurrentHashMap.newKeySet() }.add(
            SessionObserver(
                webSocketSession = session,
                user = user,
            ),
        )
        broadcastUpdate(liveSession)

        logger.info(
            "New WebSocket connection established: ${session.id}, userId: ${userDetailsWithId.id}, requestId: $requestId",
        )
    }

    override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
        requestChunkBuffers.remove(session.id)
        val liveSessionId = sessionToLiveSessionMap[session.id] ?: return
        sessionObservers[liveSessionId]?.removeIf { it.webSocketSession == session }
        if (sessionObservers[liveSessionId]?.isEmpty() == true) {
            sessionObservers.remove(liveSessionId)
        }
        sessionToLiveSessionMap.remove(session.id)
        logger.info("WebSocket connection closed: ${session.id}, status: $status")
    }

    private fun extractRequestId(session: WebSocketSession): String {
        val uri = session.uri ?: throw IllegalStateException("Session URI is null")
        val path = UriComponentsBuilder.fromUri(uri).build().pathSegments
        val segment = path.lastOrNull() ?: throw IllegalArgumentException("RequestId not found in URI")
        // pathSegments keeps percent-encoding, and ids can end in a space
        // (IdGenerator pads 21-char base58 ids), which clients send as %20.
        return UriUtils.decode(segment, Charsets.UTF_8)
    }

    override fun handleTextMessage(session: WebSocketSession, message: TextMessage) {
        val securityContext = session.attributes["SPRING_SECURITY_CONTEXT"] as? SecurityContext
        if (securityContext != null) {
            SecurityContextHolder.setContext(securityContext)
        } else {
            throw IllegalStateException("Security context not found in WebSocket session")
        }
        val liveSessionId = sessionToLiveSessionMap[session.id] ?: throw IllegalStateException("LiveSession not found")

        if (message.payload == "CONNECT") {
            val liveSession = sessionService.getSession(liveSessionId)
            broadcastUpdate(liveSession)
            return
        }

        try {
            val webSocketMessage = decodeMessage(session, message.payload) ?: return
            when (webSocketMessage) {
                is UpdateContentMessage -> {
                    val updatedSession = sessionService.updateContent(
                        liveSessionId,
                        webSocketMessage.content,
                    )
                    broadcastUpdate(updatedSession, webSocketMessage.ref)
                }

                is ExecuteMessage -> {
                    val principal = securityContext.authentication.principal
                    val userId = when (principal) {
                        is UserDetailsWithId -> principal.id

                        else -> throw IllegalStateException(
                            "Expected UserDetailsWithId but got: ${principal.javaClass}",
                        )
                    }
                    // Run execution in background thread with SecurityContext propagation
                    queryExecutor.submit {
                        try {
                            val executionResult = sessionService.executeStatement(
                                liveSessionId,
                                webSocketMessage.statement,
                                userId,
                            )
                            when (executionResult) {
                                is DBExecutionResult -> {
                                    val resultMessage = ResultMessage(
                                        sessionId = liveSessionId,
                                        results = executionResult.results.map { ExecutionResultResponse.fromDto(it) },
                                        event = executionResult.event?.let { EventResponse.fromEvent(it) },
                                    )
                                    broadcastResultMessage(liveSessionId, resultMessage)
                                }

                                else -> throw IllegalStateException(
                                    "Unsupported execution result type: $executionResult",
                                )
                            }
                        } catch (e: AccessDeniedException) {
                            logger.warn("Access denied for session: $liveSessionId", e)
                            sessionObservers[liveSessionId]?.forEach { sessionObserver ->
                                sendErrorResponseMessage(
                                    sessionObserver.webSocketSession,
                                    "You don't have permission to execute on this session",
                                    liveSessionId,
                                )
                            }
                        } catch (e: Exception) {
                            logger.error("Error executing query", e)
                            sessionObservers[liveSessionId]?.forEach { sessionObserver ->
                                sendErrorResponseMessage(
                                    sessionObserver.webSocketSession,
                                    "Error executing query: ${e.message}",
                                    liveSessionId,
                                )
                            }
                        }
                    }
                    // Handler returns immediately - can now process other messages!
                }

                is CancelMessage -> {
                    sessionService.cancelQuery(liveSessionId)
                    logger.info("Query cancelled for session: $liveSessionId")
                }

                is ChunkMessage -> {
                    throw IllegalArgumentException("Nested WebSocket message chunks are not supported")
                }
            }
        } catch (e: AccessDeniedException) {
            logger.warn("Access denied for session: $liveSessionId", e)
            sendErrorResponseMessage(session, "You don't have permission to perform this action", liveSessionId)
        } catch (e: Exception) {
            logger.error("Error processing message", e)
            sendErrorResponseMessage(session, "Error processing message", liveSessionId)
        }
    }

    private fun decodeMessage(session: WebSocketSession, payload: String): WebSocketMessage? {
        val message = objectMapper.readValue(payload, WebSocketMessage::class.java)
        if (message !is ChunkMessage) {
            return message
        }

        val buffers = requestChunkBuffers.computeIfAbsent(session.id) { LinkedHashMap() }
        val assembledPayload = synchronized(buffers) {
            WebSocketMessageAssembler.append(buffers, message)
        } ?: return null

        return objectMapper.readValue(assembledPayload, WebSocketMessage::class.java)
    }

    private fun broadcastResultMessage(sessionId: LiveSessionId, resultMessage: ResultMessage) {
        sessionObservers[sessionId]?.forEach { sessionObserver ->
            sendMessage(sessionObserver.webSocketSession, resultMessage)
        }
    }

    private fun broadcastUpdate(updatedSession: LiveSession, ref: String = "") {
        val updateMessage = StatusMessage(
            sessionId = updatedSession.id!!,
            consoleContent = updatedSession.consoleContent,
            observers = sessionObservers[updatedSession.id]?.map { UserResponse(it.user) } ?: emptyList(),
            ref = ref,
        )
        sessionObservers[updatedSession.id]?.forEach { sessionObserver ->
            sendMessage(sessionObserver.webSocketSession, updateMessage)
        }
    }

    private fun sendErrorResponseMessage(session: WebSocketSession, message: String, id: LiveSessionId) {
        val errorMessage = ErrorResponseMessage(
            sessionId = id,
            error = message,
        )
        sendMessage(session, errorMessage)
    }

    private fun sendMessage(session: WebSocketSession, message: ResponseMessage) {
        try {
            val gzip = session.uri?.let {
                UriComponentsBuilder.fromUri(it).build().queryParams.getFirst("compression") == "gzip"
            } == true
            val frames = try {
                WebSocketMessageChunker.encode(message, objectMapper, gzip)
            } catch (_: WebSocketResponseTooLargeException) {
                WebSocketMessageChunker.encode(
                    ErrorResponseMessage(
                        sessionId = message.sessionId,
                        error = if (gzip) {
                            "Response exceeds the live-session size limit. Narrow the query or download the result."
                        } else {
                            "Response is too large for this browser session. Refresh to enable compressed responses, " +
                                "or narrow the query or download the result."
                        },
                    ),
                    objectMapper,
                )
            }
            logger.info(
                "Sending ${message::class.simpleName} message to ${session.id} in ${frames.size} frame(s)",
            )
            synchronized(session) {
                frames.forEach { frame ->
                    session.sendMessage(TextMessage(frame))
                }
            }
        } catch (e: Exception) {
            logger.error("Error sending message", e)
        }
    }
}
