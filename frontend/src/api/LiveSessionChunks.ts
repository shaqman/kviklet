export const MAX_CHUNK_COUNT = 10_000;
const MAX_PENDING_CHUNK_MESSAGES = 16;

export interface LiveSessionChunk {
  messageId: string;
  index: number;
  total: number;
  payload: string;
}

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
    !Number.isInteger(chunk.total) ||
    chunk.total < 1 ||
    chunk.total > MAX_CHUNK_COUNT ||
    !Number.isInteger(chunk.index) ||
    chunk.index < 0 ||
    chunk.index >= chunk.total
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
