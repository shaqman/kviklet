import { describe, expect, it } from "vitest";
import {
  appendLiveSessionChunk,
  encodeLiveSessionMessage,
  MAX_WEBSOCKET_MESSAGE_SIZE,
  MAX_CHUNK_PAYLOAD_SIZE,
  type LiveSessionChunk,
} from "./LiveSessionChunks";

const chunk = (
  messageId: string,
  index: number,
  total: number,
  payload: string,
): LiveSessionChunk => ({ messageId, index, total, payload });

describe("appendLiveSessionChunk", () => {
  it("reassembles chunks in order even when they arrive out of order", () => {
    const buffers = new Map();

    expect(appendLiveSessionChunk(buffers, chunk("message", 1, 3, "world"))).toBe(
      undefined,
    );
    expect(appendLiveSessionChunk(buffers, chunk("message", 1, 3, "world"))).toBe(
      undefined,
    );
    expect(appendLiveSessionChunk(buffers, chunk("message", 0, 3, "hello "))).toBe(
      undefined,
    );
    expect(appendLiveSessionChunk(buffers, chunk("message", 2, 3, "!"))).toBe(
      "hello world!",
    );
    expect(buffers).toHaveLength(0);
  });

  it("rejects invalid chunk indexes and totals", () => {
    expect(() =>
      appendLiveSessionChunk(
        new Map(),
        chunk("message", 2, 2, "invalid"),
      ),
    ).toThrow("Invalid WebSocket message chunk");
  });
});

describe("encodeLiveSessionMessage", () => {
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
    const chunks = frames.map((frame) => JSON.parse(frame));

    expect(frames.length).toBeGreaterThan(1);
    expect(frames.every((frame) => frame.length <= MAX_WEBSOCKET_MESSAGE_SIZE)).toBe(
      true,
    );
    expect(new Set(chunks.map((chunk) => chunk.messageId))).toEqual(
      new Set(["request"]),
    );
    expect(new Set(chunks.map((chunk) => chunk.total))).toEqual(
      new Set([chunks.length]),
    );
    expect(chunks.every((chunk) => chunk.payload.length <= MAX_CHUNK_PAYLOAD_SIZE)).toBe(
      true,
    );
    expect(
      chunks
        .sort((left, right) => left.index - right.index)
        .map((chunk) => chunk.payload)
        .join(""),
    ).toBe(message);
  });
});
