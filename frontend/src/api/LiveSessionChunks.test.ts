import { describe, expect, it } from "vitest";
import { gzipSync } from "node:zlib";
import {
  appendLiveSessionChunk,
  encodeLiveSessionMessage,
  MAX_WEBSOCKET_MESSAGE_SIZE,
  MAX_CHUNK_PAYLOAD_SIZE,
  MAX_CHUNK_COUNT,
  MAX_GZIP_RESPONSE_CHUNK_COUNT,
  supportsGzipResponses,
  decodeLiveSessionResponse,
  type LiveSessionChunk,
  type ChunkBuffer,
} from "./LiveSessionChunks";

const chunk = (
  messageId: string,
  index: number,
  total: number,
  payload: string,
): LiveSessionChunk => ({ messageId, index, total, payload });

describe("appendLiveSessionChunk", () => {
  it("reassembles chunks in order even when they arrive out of order", () => {
    const buffers = new Map<string, ChunkBuffer>();

    expect(
      appendLiveSessionChunk(buffers, chunk("message", 1, 3, "world")),
    ).toBe(undefined);
    expect(
      appendLiveSessionChunk(buffers, chunk("message", 1, 3, "world")),
    ).toBe(undefined);
    expect(
      appendLiveSessionChunk(buffers, chunk("message", 0, 3, "hello ")),
    ).toBe(undefined);
    expect(appendLiveSessionChunk(buffers, chunk("message", 2, 3, "!"))).toBe(
      "hello world!",
    );
    expect(buffers).toHaveLength(0);
  });

  it("rejects invalid chunk indexes and totals", () => {
    expect(() =>
      appendLiveSessionChunk(new Map(), chunk("message", 2, 2, "invalid")),
    ).toThrow("Invalid WebSocket message chunk");
  });

  it("permits the larger receive budget only for negotiated gzip chunks", () => {
    const response = {
      ...chunk("gzip", 0, MAX_CHUNK_COUNT + 1, "YQ=="),
      encoding: "gzip" as const,
    };
    expect(appendLiveSessionChunk(new Map(), response)).toBeUndefined();
    expect(() =>
      appendLiveSessionChunk(new Map(), { ...response, encoding: undefined }),
    ).toThrow();
    expect(() =>
      appendLiveSessionChunk(new Map(), {
        ...response,
        total: MAX_GZIP_RESPONSE_CHUNK_COUNT + 1,
      }),
    ).toThrow();
  });

  it("rejects changed encoding and conflicting duplicate chunks", () => {
    const buffers = new Map<string, ChunkBuffer>();
    appendLiveSessionChunk(buffers, {
      ...chunk("gzip", 0, 2, "YQ=="),
      encoding: "gzip",
    });
    expect(() =>
      appendLiveSessionChunk(buffers, chunk("gzip", 1, 2, "YQ==")),
    ).toThrow("metadata changed");
    expect(buffers.size).toBe(0);
    appendLiveSessionChunk(buffers, chunk("plain", 0, 2, "first"));
    expect(() =>
      appendLiveSessionChunk(buffers, chunk("plain", 0, 2, "changed")),
    ).toThrow("payload changed");
    expect(buffers.size).toBe(0);
  });

  it("bounds the combined payload across incomplete gzip responses", () => {
    const buffers = new Map<string, ChunkBuffer>();
    const payload = "A".repeat(MAX_CHUNK_PAYLOAD_SIZE);
    for (let index = 0; index < MAX_GZIP_RESPONSE_CHUNK_COUNT - 1; index += 1) {
      appendLiveSessionChunk(buffers, {
        messageId: "first",
        index,
        total: MAX_GZIP_RESPONSE_CHUNK_COUNT,
        payload,
        encoding: "gzip",
      });
    }
    appendLiveSessionChunk(buffers, {
      messageId: "second",
      index: 0,
      total: 2,
      payload,
      encoding: "gzip",
    });
    expect(() =>
      appendLiveSessionChunk(buffers, {
        messageId: "second",
        index: 1,
        total: 2,
        payload: "A",
        encoding: "gzip",
      }),
    ).toThrow("Buffered WebSocket responses are too large");
    expect(buffers.has("first")).toBe(true);
    expect(buffers.has("second")).toBe(false);
  });
});

describe("decodeLiveSessionResponse", () => {
  it("detects native gzip support", () => {
    expect(supportsGzipResponses()).toBe(true);
  });
  it("decodes gzip UTF-8 JSON larger than the former receive limit", async () => {
    const serialized = JSON.stringify({
      type: "result",
      text: "result 😀\n".repeat(700_000),
    });
    expect(serialized.length).toBeGreaterThan(
      MAX_CHUNK_COUNT * MAX_CHUNK_PAYLOAD_SIZE,
    );
    const compressed = gzipSync(Buffer.from(serialized)).toString("base64");
    const chunks = compressed.match(/.{1,512}/g)!;
    expect(chunks.length).toBeLessThan(MAX_CHUNK_COUNT);
    const buffers = new Map<string, ChunkBuffer>();
    let assembled: string | undefined;
    for (const [index, payload] of chunks.entries()) {
      assembled = appendLiveSessionChunk(buffers, {
        messageId: "gzip",
        index,
        total: chunks.length,
        payload,
        encoding: "gzip",
      });
    }
    expect(await decodeLiveSessionResponse(assembled!, "gzip")).toBe(
      serialized,
    );
    expect(buffers.size).toBe(0);
  });

  it("preserves legacy plain responses", async () => {
    expect(await decodeLiveSessionResponse('{"type":"status"}')).toBe(
      '{"type":"status"}',
    );
  });

  it("enforces expanded UTF-8 bytes independently of the compressed size", async () => {
    const compressed = gzipSync(Buffer.from("😀".repeat(100))).toString(
      "base64",
    );
    await expect(
      decodeLiveSessionResponse(compressed, "gzip", 399),
    ).rejects.toThrow("Expanded WebSocket response is too large");
    expect(await decodeLiveSessionResponse(compressed, "gzip", 400)).toBe(
      "😀".repeat(100),
    );
  });

  it("rejects truncated gzip and invalid base64", async () => {
    const compressed = gzipSync(Buffer.from("response"))
      .subarray(0, 10)
      .toString("base64");
    await expect(
      decodeLiveSessionResponse(compressed, "gzip"),
    ).rejects.toThrow();
    await expect(decodeLiveSessionResponse("%%%", "gzip")).rejects.toThrow();
  });
});

describe("encodeLiveSessionMessage", () => {
  it("keeps the original outgoing request limit", () => {
    expect(() =>
      encodeLiveSessionMessage(
        "x".repeat(MAX_CHUNK_COUNT * MAX_CHUNK_PAYLOAD_SIZE + 1),
      ),
    ).toThrow("WebSocket message is too large");
  });
  it("leaves small messages unchanged", () => {
    const message = JSON.stringify({ type: "cancel" });

    expect(encodeLiveSessionMessage(message)).toEqual([message]);
  });

  it("splits large messages into bounded request chunks", () => {
    const message = JSON.stringify({
      type: "update_content",
      content: "INSERT INTO test VALUES " + "('x'),".repeat(2_000),
      ref: "ref",
    });

    const frames = encodeLiveSessionMessage(message, "request");
    const chunks = frames.map((frame) => JSON.parse(frame) as LiveSessionChunk);

    expect(frames.length).toBeGreaterThan(1);
    expect(
      frames.every((frame) => frame.length <= MAX_WEBSOCKET_MESSAGE_SIZE),
    ).toBe(true);
    expect(new Set(chunks.map((chunk) => chunk.messageId))).toEqual(
      new Set(["request"]),
    );
    expect(new Set(chunks.map((chunk) => chunk.total))).toEqual(
      new Set([chunks.length]),
    );
    expect(
      chunks.every((chunk) => chunk.payload.length <= MAX_CHUNK_PAYLOAD_SIZE),
    ).toBe(true);
    expect(
      chunks
        .sort((left, right) => left.index - right.index)
        .map((chunk) => chunk.payload)
        .join(""),
    ).toBe(message);
  });
});
