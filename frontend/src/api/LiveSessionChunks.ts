export const MAX_WEBSOCKET_MESSAGE_SIZE = 4 * 1024;
export const MAX_CHUNK_COUNT = 10_000;
export const MAX_CHUNK_PAYLOAD_SIZE = 512;
export const MAX_GZIP_RESPONSE_CHUNK_COUNT = 400_000;
export const MAX_EXPANDED_RESPONSE_BYTES = 200 * 1024 * 1024;
const MAX_BUFFERED_RESPONSE_CHARACTERS =
  MAX_GZIP_RESPONSE_CHUNK_COUNT * MAX_CHUNK_PAYLOAD_SIZE;
const MAX_CHUNK_MESSAGE_ID_LENGTH = 128;
const MAX_PENDING_CHUNK_MESSAGES = 16;

export interface LiveSessionChunk {
  messageId: string;
  index: number;
  total: number;
  payload: string;
  encoding?: "gzip";
}

export const supportsGzipResponses = (): boolean => {
  try {
    new DecompressionStream("gzip");
    return true;
  } catch {
    return false;
  }
};

export const decodeLiveSessionResponse = async (
  payload: string,
  encoding?: "gzip",
  maxExpandedBytes = MAX_EXPANDED_RESPONSE_BYTES,
): Promise<string> => {
  if (encoding === undefined) return payload;
  if (!supportsGzipResponses()) {
    throw new Error("This browser does not support compressed responses");
  }
  if (payload.length > MAX_BUFFERED_RESPONSE_CHARACTERS) {
    throw new Error("Compressed WebSocket response is too large");
  }
  const binary = atob(payload);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  const input = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
  const output = input.pipeThrough(
    new DecompressionStream("gzip") as TransformStream<Uint8Array, Uint8Array>,
  );
  const reader = output.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const parts: string[] = [];
  let size = 0;
  try {
    let current = await reader.read();
    while (!current.done) {
      const value = current.value;
      size += value.byteLength;
      if (size > Math.min(maxExpandedBytes, MAX_EXPANDED_RESPONSE_BYTES)) {
        throw new Error("Expanded WebSocket response is too large");
      }
      parts.push(decoder.decode(value, { stream: true }));
      current = await reader.read();
    }
    parts.push(decoder.decode());
    return parts.join("");
  } catch (error) {
    await reader.cancel().catch(() => undefined);
    throw error;
  } finally {
    reader.releaseLock();
  }
};

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

  const payloads =
    serializedMessage.match(
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
  payloadSize: number;
  encoding?: "gzip";
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
    chunk.total >
      (chunk.encoding === "gzip"
        ? MAX_GZIP_RESPONSE_CHUNK_COUNT
        : MAX_CHUNK_COUNT) ||
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
      const oldestMessage = buffers.keys().next();
      if (!oldestMessage.done) {
        buffers.delete(oldestMessage.value);
      }
    }
    buffer = {
      total: chunk.total,
      chunks: new Array<string | undefined>(chunk.total),
      received: 0,
      payloadSize: 0,
      encoding: chunk.encoding,
    };
    buffers.set(chunk.messageId, buffer);
  } else if (
    buffer.total !== chunk.total ||
    buffer.encoding !== chunk.encoding
  ) {
    buffers.delete(chunk.messageId);
    throw new Error("WebSocket message chunk metadata changed");
  }

  const existingPayload = buffer.chunks[chunk.index];
  if (existingPayload !== undefined && existingPayload !== chunk.payload) {
    buffers.delete(chunk.messageId);
    throw new Error("WebSocket message chunk payload changed");
  }
  if (buffer.chunks[chunk.index] === undefined) {
    let bufferedSize = chunk.payload.length;
    for (const pending of buffers.values()) bufferedSize += pending.payloadSize;
    if (bufferedSize > MAX_BUFFERED_RESPONSE_CHARACTERS) {
      buffers.delete(chunk.messageId);
      throw new Error("Buffered WebSocket responses are too large");
    }
    buffer.chunks[chunk.index] = chunk.payload;
    buffer.received += 1;
    buffer.payloadSize += chunk.payload.length;
  }

  if (buffer.received < buffer.total) {
    return undefined;
  }

  buffers.delete(chunk.messageId);
  return buffer.chunks.join("");
};
