import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { gzipSync } from "node:zlib";
import useLiveSession from "./useLiveSession";

const notification = vi.hoisted(() => vi.fn());
vi.mock("./useNotification", () => ({
  default: () => ({ addNotification: notification }),
}));

class TestWebSocket {
  static OPEN = 1;
  static instances: TestWebSocket[] = [];
  readyState = TestWebSocket.OPEN;
  onopen: (() => void) | null = null;
  onmessage: ((event: MessageEvent<string>) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  send = vi.fn();
  close = vi.fn();
  constructor(readonly url: string) {
    TestWebSocket.instances.push(this);
  }
  receive(data: unknown) {
    this.onmessage?.(
      new MessageEvent("message", { data: JSON.stringify(data) }),
    );
  }
}

const status = (consoleContent: string) => ({
  type: "status",
  sessionId: "session",
  consoleContent,
  observers: [],
  ref: "ref",
});

const gzipFrames = (data: unknown) => {
  const payloads = gzipSync(Buffer.from(JSON.stringify(data)))
    .toString("base64")
    .match(/.{1,512}/g)!;
  return payloads.map((payload, index) => ({
    type: "chunk",
    sessionId: "session",
    messageId: "gzip",
    encoding: "gzip",
    index,
    total: payloads.length,
    payload,
  }));
};

beforeEach(() => {
  TestWebSocket.instances = [];
  notification.mockClear();
  vi.stubGlobal("WebSocket", TestWebSocket);
});
afterEach(() => vi.unstubAllGlobals());

describe("live-session compressed responses", () => {
  it("negotiates gzip and preserves the order of compressed and plain updates", async () => {
    const setContent = vi.fn<(content: string) => void>();
    renderHook(() => useLiveSession("request", setContent));
    const socket = TestWebSocket.instances[0];
    expect(socket.url).toContain("?compression=gzip");
    const content = "result 😀\n".repeat(700_000);
    await act(async () => {
      gzipFrames(status(content)).forEach((frame) => socket.receive(frame));
      socket.receive(status("newer update"));
      await waitFor(() => expect(setContent).toHaveBeenCalledTimes(2));
    });
    await waitFor(() => expect(setContent).toHaveBeenCalledTimes(2));
    expect(setContent.mock.calls.map(([value]) => value)).toEqual([
      content,
      "newer update",
    ]);
    expect(notification).not.toHaveBeenCalled();
  });

  it("continues using plain responses when gzip is unavailable", async () => {
    vi.stubGlobal("DecompressionStream", undefined);
    const setContent = vi.fn<(content: string) => void>();
    renderHook(() => useLiveSession("request", setContent));
    const socket = TestWebSocket.instances[0];
    expect(socket.url).not.toContain("compression=");
    await act(async () => {
      socket.receive(status("legacy update"));
      await Promise.resolve();
    });
    await waitFor(() =>
      expect(setContent).toHaveBeenCalledWith("legacy update"),
    );
  });

  it("shows one error for a rejected message and releases the pending execution", async () => {
    const { result } = renderHook(() => useLiveSession("request", vi.fn()));
    const socket = TestWebSocket.instances[0];
    let execution: Promise<void>;
    act(() => {
      execution = result.current.executeQuery("SELECT 1");
    });
    await act(async () => {
      socket.receive({
        type: "chunk",
        sessionId: "session",
        messageId: "invalid",
        index: 0,
        total: 10_001,
        payload: "A",
      });
      socket.receive({
        type: "chunk",
        sessionId: "session",
        messageId: "invalid",
        index: 1,
        total: 10_001,
        payload: "A",
      });
      await execution!;
    });
    expect(notification).toHaveBeenCalledTimes(1);
    expect(result.current.isLoading).toBe(false);
  });

  it("delivers a compressed query result and resolves execution", async () => {
    const { result } = renderHook(() => useLiveSession("request", vi.fn()));
    const socket = TestWebSocket.instances[0];
    let execution: Promise<void>;
    act(() => {
      execution = result.current.executeQuery("SELECT 1");
    });
    await act(async () => {
      gzipFrames({ type: "result", sessionId: "session", results: [] }).forEach(
        (frame) => socket.receive(frame),
      );
      await execution!;
    });
    expect(result.current.results).toEqual([]);
    expect(result.current.isLoading).toBe(false);
    expect(notification).not.toHaveBeenCalled();
  });
});
