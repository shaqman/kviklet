export const MAX_WEBSOCKET_MESSAGE_SIZE = 4 * 1024;
export const MAX_CHUNK_COUNT = 10_000;
export const MAX_CHUNK_PAYLOAD_SIZE = 512;
const MAX_CHUNK_MESSAGE_ID_LENGTH = 128;
const MAX_PENDING_CHUNK_MESSAGES = 16;

export interface LiveSessionChunk {
  messageId: string;
  index: number;
  total: number;
  payload: string;
}

export const encodeLiveSessionMessage = (
  serializedMessage: string,
  messageId?: string,
): string[] => {
  if (serializedMessage.length <= MAX_WEBSOCKET_MESSAGE_SIZE) {
    return [serializedMessage];
  }

  const resolvedMessageId = messageId ?? crypto.randomUUID();
  if (
    !resolvedMessageId ||
    resolvedMessageId.length > MAX_CHUNK_MESSAGE_ID_LENGTH
  ) {
    throw new Error("Invalid WebSocket message chunk id");
  }

  const payloads = serializedMessage.match(
    new RegExp(`.{1,${MAX_CHUNK_PAYLOAD_SIZE}}`, "gs"),
  ) ?? [];
  if (payloads.length > MAX_CHUNK_COUNT) {
    throw new Error("WebSocket message is too large");
  }

  return payloads.map((payload, index) =>
    JSON.stringify({
      type: "chunk",
      messageId: resolvedMessageId,
      index,
      total: payloads.length,
      payload,
    }),
  );
};

export interface ChunkBuffer {
  total: number;
  chunks: Array<string | undefined>;
  received: number;
}

export const appendLiveSessionChunk = (
  buffers: Map<string, ChunkBuffer>,
  chunk: LiveSessionChunk,
): string | undefined => {
  if (
    !chunk.messageId ||
    chunk.messageId.length > MAX_CHUNK_MESSAGE_ID_LENGTH ||
    !Number.isInteger(chunk.total) ||
    chunk.total < 1 ||
    chunk.total > MAX_CHUNK_COUNT ||
    !Number.isInteger(chunk.index) ||
    chunk.index < 0 ||
    chunk.index >= chunk.total ||
    chunk.payload.length > MAX_CHUNK_PAYLOAD_SIZE
  ) {
    throw new Error("Invalid WebSocket message chunk");
  }

  let buffer = buffers.get(chunk.messageId);
  if (!buffer) {
    if (buffers.size >= MAX_PENDING_CHUNK_MESSAGES) {
      const oldestMessageId = buffers.keys().next().value;
      if (oldestMessageId !== undefined) {
        buffers.delete(oldestMessageId);
      }
    }
    buffer = {
      total: chunk.total,
      chunks: new Array<string | undefined>(chunk.total),
      received: 0,
    };
    buffers.set(chunk.messageId, buffer);
  } else if (buffer.total !== chunk.total) {
    buffers.delete(chunk.messageId);
    throw new Error("WebSocket message chunk count changed");
  }

  if (buffer.chunks[chunk.index] === undefined) {
    buffer.chunks[chunk.index] = chunk.payload;
    buffer.received += 1;
  }

  if (buffer.received < buffer.total) {
    return undefined;
  }

  buffers.delete(chunk.messageId);
  return buffer.chunks.join("");
};
