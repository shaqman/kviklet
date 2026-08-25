import { describe, expect, it } from "vitest";
import {
  appendLiveSessionChunk,
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
