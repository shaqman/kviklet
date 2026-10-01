import { useEffect, useMemo, useRef, useState } from "react";
import { websocketBaseUrl } from "../api/base";
import { ExecuteResponseResult, Execute } from "../api/ExecutionRequestApi";
import {
  executeStatementMessage,
  responseMessage,
  updateContentMessage,
  cancelMessage,
} from "../api/LiveSessionApi";
import { z } from "zod";
import debounce from "lodash/debounce";
import useNotification from "./useNotification";
import {
  appendLiveSessionChunk,
  encodeLiveSessionMessage,
  decodeLiveSessionResponse,
  supportsGzipResponses,
  type ChunkBuffer,
} from "../api/LiveSessionChunks";

type ExecuteResolver = () => void;

const useLiveSession = (
  requestId: string,
  setContent: (content: string) => void,
) => {
  const ws = useRef<WebSocket | null>(null);
  const chunkBuffersRef = useRef<Map<string, ChunkBuffer>>(new Map());
  const rejectedChunkIdsRef = useRef<Set<string>>(new Set());
  const executeResolverRef = useRef<ExecuteResolver | null>(null);
  const inFlightRefsRef = useRef<Set<string>>(new Set());
  const [results, setResults] = useState<ExecuteResponseResult[] | undefined>(
    undefined,
  );
  const [isLoading, setIsLoading] = useState(false);
  const [updatedRows, setUpdatedRows] = useState<number | undefined>(undefined);
  const [websocketEvents, setWebsocketEvents] = useState<Execute[]>([]);
  const [isSyncing, setIsSyncing] = useState(false);

  const { addNotification } = useNotification();

  useEffect(() => {
    // Initialize WebSocket connection
    const compression = supportsGzipResponses() ? "?compression=gzip" : "";
    const socket = new WebSocket(
      `${websocketBaseUrl}/sql/${requestId}${compression}`,
    );
    let active = true;
    let messageQueue = Promise.resolve();

    socket.onopen = () => {
      console.log("WebSocket connection established");
    };

    socket.onmessage = (event) => {
      // Keep responses ordered while a completed gzip message decompresses.
      messageQueue = messageQueue
        .then(async () => {
          if (active)
            await handleWebSocketMessage(
              JSON.parse(event.data as string),
              () => active,
            );
        })
        .catch((err) => {
          if (!active) return;
          console.error("Failed to parse WebSocket message:", err);
          addNotification({
            type: "error",
            title: "Websocket error",
            text: "Failed to parse server response",
          });
          setIsLoading(false);
          executeResolverRef.current?.();
          executeResolverRef.current = null;
        });
    };

    socket.onerror = (error) => {
      console.error("WebSocket error:", error);
      addNotification({
        type: "error",
        title: "Websocket error",
        text: "Connection error occurred. Please try again.",
      });
      setIsLoading(false);
    };

    socket.onclose = (event) => {
      active = false;
      console.log("WebSocket connection closed", event);
      chunkBuffersRef.current.clear();
      rejectedChunkIdsRef.current.clear();
      if (!event.wasClean) {
        console.error("Connection lost unexpectedly. Please refresh the page.");
        addNotification({
          type: "error",
          title: "Websocket error",
          text: "Connection lost unexpectedly. Please refresh the page.",
        });
        setIsLoading(false);
      }
    };

    ws.current = socket;

    return () => {
      active = false;
      chunkBuffersRef.current.clear();
      rejectedChunkIdsRef.current.clear();
      if (socket.readyState === WebSocket.OPEN) {
        socket.close();
      }
    };
  }, [requestId]);

  const handleWebSocketMessage = async (
    data: unknown,
    isActive: () => boolean,
  ): Promise<void> => {
    if (!isActive()) return;
    try {
      const message = responseMessage.safeParse(data);

      if (!message.success) {
        console.error("Invalid message format:", message.error);
        addNotification({
          type: "error",
          title: "Websocket error",
          text: "Received invalid response from server",
        });
        setIsLoading(false);
        executeResolverRef.current?.();
        executeResolverRef.current = null;
        return;
      }

      const messageData = message.data;

      if (messageData.type === "chunk") {
        if (rejectedChunkIdsRef.current.has(messageData.messageId)) return;
        let assembledMessage;
        try {
          assembledMessage = appendLiveSessionChunk(
            chunkBuffersRef.current,
            messageData,
          );
          if (assembledMessage === undefined) return;
          const response = await decodeLiveSessionResponse(
            assembledMessage,
            messageData.encoding,
          );
          if (isActive())
            await handleWebSocketMessage(JSON.parse(response), isActive);
        } catch (error) {
          const rejected = rejectedChunkIdsRef.current;
          if (rejected.size >= 16) {
            const oldest = rejected.values().next();
            if (!oldest.done) rejected.delete(oldest.value);
          }
          rejected.add(messageData.messageId);
          throw error;
        }
        return;
      }

      switch (messageData.type) {
        case "status":
          // If this ref is in our in-flight set, it's our own echo - ignore it
          if (inFlightRefsRef.current.has(messageData.ref)) {
            inFlightRefsRef.current.delete(messageData.ref);
            setIsSyncing(inFlightRefsRef.current.size > 0);
            // Don't update content - this is just our own message coming back
          } else {
            // This is from another user or a real server update - accept it
            setContent(messageData.consoleContent);
          }
          break;
        case "result": {
          setResults(messageData.results);
          setIsLoading(false);
          // Add the event to websocket events if it exists
          if (messageData.event) {
            const event = messageData.event as Execute;
            setWebsocketEvents((prev) => {
              // Check if event already exists by ID
              const existingIndex = prev.findIndex((e) => e.id === event.id);
              if (existingIndex >= 0) {
                // Update existing event (in case results were added)
                const updated = [...prev];
                updated[existingIndex] = event;
                return updated;
              }
              // Add new event
              return [...prev, event];
            });
          }
          // Resolve the execute promise
          if (executeResolverRef.current) {
            executeResolverRef.current();
            executeResolverRef.current = null;
          }
          break;
        }
        case "error":
          addNotification({
            type: "error",
            title: "Query error",
            text: messageData.error,
          });
          setIsLoading(false);
          // Resolve the execute promise on error too
          if (executeResolverRef.current) {
            executeResolverRef.current();
            executeResolverRef.current = null;
          }
          break;
      }
    } catch (err) {
      if (!isActive()) return;
      console.error("Error handling WebSocket message:", err);
      addNotification({
        type: "error",
        title: "Websocket error",
        text: "Failed to process server response",
      });
      setIsLoading(false);
      executeResolverRef.current?.();
      executeResolverRef.current = null;
    }
  };

  const executeQuery = (query: string): Promise<void> => {
    return new Promise((resolve) => {
      // Store resolver to call when result arrives
      executeResolverRef.current = resolve;

      if (!query.trim()) {
        addNotification({
          type: "error",
          title: "Query error",
          text: "Query cannot be empty",
        });
        executeResolverRef.current = null;
        resolve();
        return;
      }

      if (!ws.current || ws.current.readyState !== WebSocket.OPEN) {
        addNotification({
          type: "error",
          title: "Connection error",
          text: "No connection to server. Please try again.",
        });
        executeResolverRef.current = null;
        resolve();
        return;
      }

      setIsLoading(true);
      setResults(undefined);
      setUpdatedRows(undefined);

      sendMessage(executeStatementMessage, {
        type: "execute",
        statement: query,
      });
    });
  };

  const sendMessage = <T extends z.ZodType>(schema: T, message: z.infer<T>) => {
    if (!ws.current || ws.current.readyState !== WebSocket.OPEN) {
      console.error("Connection lost. Please refresh the page.");
      addNotification({
        type: "error",
        title: "Websocket error",
        text: "Connection lost. Please refresh the page.",
      });
      setIsLoading(false);
      return;
    }

    try {
      const validatedMessage = schema.parse(message) as z.infer<T>;
      const frames = encodeLiveSessionMessage(JSON.stringify(validatedMessage));
      frames.forEach((frame) => ws.current?.send(frame));
    } catch (error) {
      if (error instanceof z.ZodError) {
        console.error("Invalid message format:", error.errors);
        addNotification({
          type: "error",
          title: "Websocket error",
          text: "Failed to send message: Invalid format",
        });
      } else {
        console.error("Error sending message:", error);
        addNotification({
          type: "error",
          title: "Websocket error",
          text: "Failed to send message to server",
        });
      }
      setIsLoading(false);
    }
  };

  const debouncedUpdateContent = useMemo(
    () =>
      debounce((content: string) => {
        const ref = crypto.randomUUID();
        inFlightRefsRef.current.add(ref);
        setIsSyncing(true);
        sendMessage(updateContentMessage, {
          type: "update_content",
          content,
          ref,
        });
      }, 300),
    [], // Empty dependencies - all used values are stable (refs, state setters, constants)
  );

  const cancelQuery = () => {
    if (!ws.current || ws.current.readyState !== WebSocket.OPEN) {
      addNotification({
        type: "error",
        title: "Connection error",
        text: "No connection to server",
      });
      return;
    }
    sendMessage(cancelMessage, { type: "cancel" });
    setIsLoading(false);
  };

  return {
    executeQuery,
    updateContent: debouncedUpdateContent,
    cancelQuery,
    isLoading,
    results,
    updatedRows,
    websocketEvents,
    isSyncing,
  };
};

export default useLiveSession;
