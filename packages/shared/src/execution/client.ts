/**
 * execution/client.ts —— execution/v1 + v1.1 HTTP/WS client（sandbox 或其他 HTTP backend）。
 *
 * v1.1 增量：
 *  - execute() 按有效 mode 路由（legacy stream:true → stream）；
 *  - interactive()：POST /exec（mode=interactive）→ WS /exec/:id/ws
 *    （stdin/stdout/stderr/resize/pty JSON frame）。
 */

import { WebSocket } from "ws";
import type {
  ExecutionBackend,
  ExecutionCapabilities,
  ExecutionDoneEvent,
  ExecutionErrorEvent,
  ExecutionInteractiveHandlers,
  ExecutionInteractiveSession,
  ExecutionOutputEvent,
  ExecutionRequest,
  ExecutionResult,
  ExecutionStreamHandlers,
} from "./types.js";
import {
  ExecutionRequestError,
  resolveExecutionMode,
  validateExecutionCapabilities,
  validateExecutionRequest,
} from "./validate.js";
import { EXECUTION_WIRE } from "./wire.js";

export class ExecutionClientError extends Error {
  readonly code: string;
  readonly status?: number;
  constructor(code: string, message: string, status?: number) {
    super(message);
    this.name = "ExecutionClientError";
    this.code = code;
    this.status = status;
  }
}

export interface HttpExecutionClientOptions {
  /** 如 http://sandbox:8080（不带尾斜杠） */
  baseUrl: string;
  /** Bearer token（sandbox 共享密钥） */
  token?: string;
  fetchLike?: typeof fetch;
  /** 同步执行 stream:true 时轮询间隔 ms */
  pollIntervalMs?: number;
}

export class HttpExecutionClient implements ExecutionBackend {
  readonly id = "http-execution";

  private readonly baseUrl: string;
  private readonly token: string | undefined;
  private readonly fetchLike: typeof fetch;
  private readonly pollIntervalMs: number;

  constructor(options: HttpExecutionClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.token = options.token;
    this.fetchLike = options.fetchLike ?? fetch;
    this.pollIntervalMs = options.pollIntervalMs ?? 50;
  }

  private url(path: string): string {
    return `${this.baseUrl}${path}`;
  }

  private headers(extra?: Record<string, string>): Record<string, string> {
    return {
      "content-type": "application/json",
      ...(this.token ? { authorization: `Bearer ${this.token}` } : {}),
      ...extra,
    };
  }

  private async request<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    const res = await this.fetchLike(this.url(path), {
      method,
      headers: this.headers(),
      body: body === undefined ? undefined : JSON.stringify(body),
      signal,
    });
    const text = await res.text();
    let json: unknown = null;
    try { json = text ? JSON.parse(text) : null; } catch { json = text; }
    if (!res.ok) {
      const err = (json as { error?: string | { code?: string; message?: string } })?.error;
      const message = typeof err === "string" ? err : err?.message ?? text;
      const code = typeof err === "object" && err?.code ? err.code : EXECUTION_WIRE.errorCodes.backendUnavailable;
      throw new ExecutionClientError(code, message, res.status);
    }
    return json as T;
  }

  async getCapabilities(signal?: AbortSignal): Promise<ExecutionCapabilities> {
    const raw = await this.request<unknown>("GET", EXECUTION_WIRE.paths.capabilities, undefined, signal);
    try {
      return validateExecutionCapabilities(raw);
    } catch (error) {
      if (error instanceof ExecutionRequestError) {
        throw new ExecutionClientError(
          EXECUTION_WIRE.errorCodes.backendUnavailable,
          `backend capabilities invalid: ${error.message}`,
        );
      }
      throw error;
    }
  }

  async execute(request: ExecutionRequest, signal?: AbortSignal): Promise<ExecutionResult> {
    const mode = resolveExecutionMode(request);
    if (mode === "interactive") {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "use interactive() for mode=interactive");
    }
    if (mode === "persistent") {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.modeNotSupported, "persistent mode is not implemented on the client");
    }
    if (mode === "sync") {
      return this.request<ExecutionResult>("POST", EXECUTION_WIRE.paths.exec, request, signal);
    }
    return this.pollStreamJob(request, signal);
  }

  private async pollStreamJob(request: ExecutionRequest, signal?: AbortSignal): Promise<ExecutionResult> {
    // mode=stream 时同时携带 legacy stream:true——v1 服务端忽略 mode 只看 stream，行为不漂移。
    const submitted = await this.request<{ execId?: string; status?: string }>(
      "POST",
      EXECUTION_WIRE.paths.exec,
      { ...request, stream: true },
      signal,
    );
    if (typeof submitted.execId !== "string" || submitted.execId.length === 0) {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.backendUnavailable,
        "stream submit did not return execId (backend may not support streaming)",
      );
    }
    const jobPath = EXECUTION_WIRE.paths.job.replace(":id", encodeURIComponent(submitted.execId));
    for (;;) {
      if (signal?.aborted) {
        try { await this.cancel(submitted.execId); } catch { /* 尽力取消 */ }
        throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.cancelled, "execution cancelled");
      }
      const state = await this.request<{ status: "running" | "done"; result?: ExecutionResult }>("GET", jobPath, undefined, signal);
      if (state.status === "done" && state.result) return state.result;
      await new Promise((r) => setTimeout(r, this.pollIntervalMs));
    }
  }

  async cancel(execId: string): Promise<boolean> {
    const path = EXECUTION_WIRE.paths.cancel.replace(":id", encodeURIComponent(execId));
    const res = await this.request<{ ok: boolean }>("POST", path);
    return res.ok === true;
  }

  /**
   * 订阅流式执行：mode=stream（或 legacy stream:true），解析 SSE output/done/error 事件。
   * 返回 execId；signal abort 时尽力 cancel。
   */
  async stream(request: ExecutionRequest, handlers: ExecutionStreamHandlers, signal?: AbortSignal): Promise<string> {
    if (resolveExecutionMode(request) !== "stream") {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "stream() requires mode=stream");
    }
    const streamRequest: ExecutionRequest = { ...request, stream: true };
    const submitted = await this.request<{ execId?: string; status?: string }>(
      "POST",
      EXECUTION_WIRE.paths.exec,
      streamRequest,
      signal,
    );
    if (typeof submitted.execId !== "string" || submitted.execId.length === 0) {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.backendUnavailable,
        "stream submit did not return execId (backend may not support streaming)",
      );
    }
    const streamPath = EXECUTION_WIRE.paths.stream.replace(":id", encodeURIComponent(submitted.execId));
    const res = await this.fetchLike(this.url(streamPath), {
      method: "GET",
      headers: this.headers({ accept: "text/event-stream" }),
      signal,
    });
    if (!res.ok || !res.body) {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.backendUnavailable, `stream open failed: ${res.status}`, res.status);
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    const dispatch = (event: string, data: unknown): void => {
      if (event === EXECUTION_WIRE.events.output) {
        handlers.onOutput?.(data as ExecutionOutputEvent);
      } else if (event === EXECUTION_WIRE.events.done) {
        handlers.onDone(data as ExecutionDoneEvent);
      } else if (event === EXECUTION_WIRE.events.error) {
        const err = data as ExecutionErrorEvent;
        if (handlers.onError) {
          handlers.onError(err);
        } else {
          throw new ExecutionClientError(err.code, err.message);
        }
      }
    };
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let boundary = buffer.indexOf("\n\n");
        while (boundary >= 0) {
          const block = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const event = parseSseEvent(block);
          dispatch(event.event, event.data);
          boundary = buffer.indexOf("\n\n");
        }
      }
      // SSE 允许最后一块没有结尾空行——flush 剩余 buffer
      if (buffer.trim().length > 0) {
        const event = parseSseEvent(buffer.trim());
        dispatch(event.event, event.data);
      }
    } catch (error) {
      if (signal?.aborted) {
        try { await this.cancel(submitted.execId); } catch { /* 尽力取消 */ }
        throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.cancelled, "stream cancelled");
      }
      throw error;
    }
    return submitted.execId;
  }

  /**
   * interactive 模式：POST /exec（mode=interactive）→ WS /exec/:id/ws。
   * 返回会话句柄（writeStdin/resize/close + done Promise）。
   * 调用前先探测 capabilities：modes.interactive 非 true → MODE_NOT_SUPPORTED。
   */
  async interactive(
    request: ExecutionRequest,
    handlers: ExecutionInteractiveHandlers = {},
    signal?: AbortSignal,
  ): Promise<ExecutionInteractiveSession> {
    if (request.stream === true) {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "interactive() conflicts with legacy stream:true");
    }
    const wireRequest: ExecutionRequest =
      request.mode === undefined ? { ...request, mode: "interactive" } : { ...request };
    let normalized: ExecutionRequest;
    try {
      normalized = validateExecutionRequest(wireRequest);
    } catch (error) {
      if (error instanceof ExecutionRequestError) {
        throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, error.message);
      }
      throw error;
    }
    if (resolveExecutionMode(normalized) !== "interactive") {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "interactive() requires mode=interactive");
    }

    const capabilities = await this.getCapabilities(signal);
    if (capabilities.modes?.interactive !== true) {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.modeNotSupported,
        "backend does not support interactive mode",
      );
    }

    const submitted = await this.request<{ execId?: string; status?: string }>(
      "POST",
      EXECUTION_WIRE.paths.exec,
      normalized,
      signal,
    );
    if (typeof submitted.execId !== "string" || submitted.execId.length === 0) {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.backendUnavailable,
        "interactive submit did not return execId (backend may not support interactive mode)",
      );
    }
    const execId = submitted.execId;

    const wsBase = this.baseUrl.replace(/^http:/, "ws:").replace(/^https:/, "wss:");
    const wsPath = EXECUTION_WIRE.paths.ws.replace(":id", encodeURIComponent(execId));
    const ws = new WebSocket(`${wsBase}${wsPath}`, this.token ? { headers: { authorization: `Bearer ${this.token}` } } : undefined);

    let settled = false;
    let opened = false;
    let resolveDone!: (event: ExecutionDoneEvent) => void;
    let rejectDone!: (error: Error) => void;
    let resolveOpen!: () => void;
    let rejectOpen!: (error: Error) => void;
    const donePromise = new Promise<ExecutionDoneEvent>((resolve, reject) => {
      resolveDone = resolve;
      rejectDone = reject;
    });
    const openPromise = new Promise<void>((resolve, reject) => {
      resolveOpen = resolve;
      rejectOpen = reject;
    });

    const settleError = (error: Error) => {
      if (settled) return;
      settled = true;
      rejectDone(error);
    };

    const onAbort = () => {
      if (!opened) rejectOpen(new ExecutionClientError(EXECUTION_WIRE.errorCodes.cancelled, "interactive cancelled before ws open"));
      try { if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) ws.close(); } catch { /* 忽略 */ }
      void this.cancel(execId).catch(() => undefined);
      settleError(new ExecutionClientError(EXECUTION_WIRE.errorCodes.cancelled, "interactive cancelled"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });

    ws.on("open", () => {
      opened = true;
      resolveOpen();
      if (signal?.aborted) onAbort();
    });
    ws.on("message", (raw) => {
      let frame: unknown;
      try {
        frame = JSON.parse(raw.toString());
      } catch {
        settleError(new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "invalid WS frame: not JSON"));
        ws.close(1008);
        return;
      }
      if (typeof frame !== "object" || frame === null || Array.isArray(frame)) {
        settleError(new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "invalid WS frame shape"));
        ws.close(1008);
        return;
      }
      const type = (frame as { type?: unknown }).type;
      if (type === EXECUTION_WIRE.wsFrames.stdout || type === EXECUTION_WIRE.wsFrames.stderr) {
        const data = (frame as { data?: unknown }).data;
        if (typeof data !== "string") {
          settleError(new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, `WS ${type} frame requires string data`));
          ws.close(1008);
          return;
        }
        handlers.onOutput?.({ stream: type, data });
        return;
      }
      if (type === EXECUTION_WIRE.wsFrames.done) {
        const f = frame as { exitCode?: unknown; timedOut?: unknown; signal?: unknown };
        const event: ExecutionDoneEvent = {
          exitCode: typeof f.exitCode === "number" ? f.exitCode : null,
          timedOut: f.timedOut === true,
          ...(typeof f.signal === "string" ? { signal: f.signal } : {}),
        };
        handlers.onDone?.(event);
        settled = true;
        resolveDone(event);
        ws.close(1000);
        return;
      }
      if (type === EXECUTION_WIRE.wsFrames.error) {
        const f = frame as { code?: unknown; message?: unknown };
        settleError(new ExecutionClientError(
          typeof f.code === "string" ? f.code : EXECUTION_WIRE.errorCodes.backendUnavailable,
          typeof f.message === "string" ? f.message : "execution ws error",
        ));
        ws.close(1011);
        return;
      }
      settleError(new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, `unknown WS frame type: ${String(type)}`));
      ws.close(1008);
    });
    ws.on("close", () => {
      signal?.removeEventListener("abort", onAbort);
      if (!settled) {
        settleError(new ExecutionClientError(EXECUTION_WIRE.errorCodes.backendUnavailable, "interactive WS closed before done"));
      }
    });
    ws.on("error", (error) => {
      if (!opened) {
        rejectOpen(new ExecutionClientError(
          EXECUTION_WIRE.errorCodes.backendUnavailable,
          `interactive WS open failed: ${error.message}`,
        ));
      }
      settleError(new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.backendUnavailable,
        `interactive WS error: ${error.message}`,
      ));
    });

    // 返回会话前必须完成 WS 握手，避免 writeStdin 撞上 CONNECTING 状态。
    await openPromise;

    return {
      execId,
      writeStdin(data: string): void {
        if (typeof data !== "string") throw new TypeError("stdin data must be a string");
        if (ws.readyState !== WebSocket.OPEN) {
          throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.cancelled, "interactive WS is closed");
        }
        ws.send(JSON.stringify({ type: EXECUTION_WIRE.wsFrames.stdin, data } satisfies ExecutionWsStdinFrameLike));
      },
      resize(cols: number, rows: number): void {
        if (!Number.isInteger(cols) || !Number.isInteger(rows) || cols < 1 || rows < 1 || cols > 1000 || rows > 1000) {
          throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "resize cols/rows must be integers in 1..1000");
        }
        if (ws.readyState !== WebSocket.OPEN) {
          throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.cancelled, "interactive WS is closed");
        }
        ws.send(JSON.stringify({ type: EXECUTION_WIRE.wsFrames.resize, cols, rows } satisfies ExecutionWsResizeFrameLike));
      },
      close(): void {
        try { ws.close(1000); } catch { /* 已关闭 */ }
      },
      done: donePromise,
    };
  }
}

type ExecutionWsStdinFrameLike = { type: "stdin"; data: string };
type ExecutionWsResizeFrameLike = { type: "resize"; cols: number; rows: number };

function parseSseEvent(block: string): { event: string; data: unknown } {
  let event = "message";
  const dataLines: string[] = [];
  for (const line of block.split("\n")) {
    if (line.startsWith("event:")) event = line.slice("event:".length).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice("data:".length).trimStart());
  }
  let data: unknown = dataLines.join("\n");
  try { data = JSON.parse(dataLines.join("\n")); } catch { /* 保持原文 */ }
  return { event, data };
}
