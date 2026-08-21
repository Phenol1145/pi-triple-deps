/**
 * execution/client.ts —— execution/v1 HTTP client（sandbox 或其他 HTTP backend）。
 */

import type {
  ExecutionBackend,
  ExecutionCapabilities,
  ExecutionDoneEvent,
  ExecutionOutputEvent,
  ExecutionRequest,
  ExecutionResult,
  ExecutionStreamHandlers,
} from "./types.js";
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
    return this.request<ExecutionCapabilities>("GET", EXECUTION_WIRE.paths.capabilities, undefined, signal);
  }

  async execute(request: ExecutionRequest, signal?: AbortSignal): Promise<ExecutionResult> {
    if (!request.stream) {
      return this.request<ExecutionResult>("POST", EXECUTION_WIRE.paths.exec, request, signal);
    }
    const submitted = await this.request<{ execId: string; status: string }>(
      "POST",
      EXECUTION_WIRE.paths.exec,
      request,
      signal,
    );
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
   * 订阅流式执行：请求 stream:true，解析 SSE output/done 事件。
   * 返回 execId；signal abort 时尽力 cancel。
   */
  async stream(request: ExecutionRequest, handlers: ExecutionStreamHandlers, signal?: AbortSignal): Promise<string> {
    const streamRequest: ExecutionRequest = { ...request, stream: true };
    const submitted = await this.request<{ execId: string; status: string }>(
      "POST",
      EXECUTION_WIRE.paths.exec,
      streamRequest,
      signal,
    );
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
          if (event.event === EXECUTION_WIRE.events.output) {
            handlers.onOutput?.(event.data as ExecutionOutputEvent);
          } else if (event.event === EXECUTION_WIRE.events.done) {
            handlers.onDone(event.data as ExecutionDoneEvent);
          }
          boundary = buffer.indexOf("\n\n");
        }
      }
      // SSE 允许最后一块没有结尾空行——flush 剩余 buffer
      if (buffer.trim().length > 0) {
        const event = parseSseEvent(buffer.trim());
        if (event.event === EXECUTION_WIRE.events.output) {
          handlers.onOutput?.(event.data as ExecutionOutputEvent);
        } else if (event.event === EXECUTION_WIRE.events.done) {
          handlers.onDone(event.data as ExecutionDoneEvent);
        }
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
}

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
