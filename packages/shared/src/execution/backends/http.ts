/**
 * execution/backends/http.ts —— HttpExecutionBackend（P0 descriptor + v1.1 模式协商）。
 *
 * 在 HttpExecutionClient（纯传输）之上补 engine 注册视角：
 *  - id / descriptor 身份
 *  - profile 固定：客户端不得自我提升，请求统一以 descriptor.profile 发出
 *  - capabilities 缓存（TTL）+ version / sandbox-untrusted 安全不变量 fail-closed
 *  - descriptor.pathMapping 默认注入（请求自带 mapping 优先）
 *  - v1.1：capabilities.modes 位图预检（MODE_NOT_SUPPORTED）+ interactive() 透传
 */

import { ExecutionClientError, HttpExecutionClient } from "../client.js";
import type {
  ExecutionBackend,
  ExecutionBackendDescriptor,
  ExecutionCapabilities,
  ExecutionInteractiveHandlers,
  ExecutionInteractiveSession,
  ExecutionRequest,
  ExecutionResult,
  ExecutionStreamHandlers,
} from "../types.js";
import { resolveExecutionMode, resolveExecutionModes, validateExecutionBackendDescriptor } from "../validate.js";
import { EXECUTION_WIRE } from "../wire.js";

export interface HttpExecutionBackendOptions {
  /** 后端注册描述（构造时结构校验；未知字段 fail-closed） */
  descriptor: ExecutionBackendDescriptor;
  /** Bearer token（由调用方从 descriptor.tokenEnv 解析后注入） */
  token?: string;
  fetchLike?: typeof fetch;
  /** 同步执行 stream:true 时轮询间隔 ms（透传 HttpExecutionClient） */
  pollIntervalMs?: number;
  /** capabilities 缓存 TTL ms；默认 30_000；0 = 每次执行前重新探测 */
  capabilitiesTtlMs?: number;
}

export class HttpExecutionBackend implements ExecutionBackend {
  readonly id: string;
  readonly descriptor: ExecutionBackendDescriptor;

  private readonly client: HttpExecutionClient;
  private readonly capabilitiesTtlMs: number;
  private capabilitiesCache: { capabilities: ExecutionCapabilities; at: number } | undefined;

  constructor(options: HttpExecutionBackendOptions) {
    this.descriptor = validateExecutionBackendDescriptor(options.descriptor);
    this.id = this.descriptor.id;
    this.client = new HttpExecutionClient({
      baseUrl: this.descriptor.url,
      token: options.token,
      fetchLike: options.fetchLike,
      pollIntervalMs: options.pollIntervalMs,
    });
    if (
      options.capabilitiesTtlMs !== undefined &&
      (!Number.isFinite(options.capabilitiesTtlMs) || options.capabilitiesTtlMs < 0)
    ) {
      throw new TypeError("capabilitiesTtlMs must be a non-negative number");
    }
    this.capabilitiesTtlMs = options.capabilitiesTtlMs ?? 30_000;
  }

  async getCapabilities(signal?: AbortSignal): Promise<ExecutionCapabilities> {
    const cached = this.capabilitiesCache;
    if (cached && Date.now() - cached.at < this.capabilitiesTtlMs) return cached.capabilities;
    const capabilities = await this.client.getCapabilities(signal);
    this.assertCompatibleCapabilities(capabilities);
    this.capabilitiesCache = { capabilities, at: Date.now() };
    return capabilities;
  }

  async execute(request: ExecutionRequest, signal?: AbortSignal): Promise<ExecutionResult> {
    const pinned = this.pinProfile(request);
    if (resolveExecutionMode(pinned) === "interactive") {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.invalidRequest,
        "use interactive() for mode=interactive",
      );
    }
    const capabilities = await this.getCapabilities(signal);
    this.assertRequestAgainstCapabilities(pinned, capabilities);
    return this.client.execute(pinned, signal);
  }

  /** 流式执行（mode=stream / legacy stream:true + SSE）；返回 execId。 */
  async stream(
    request: ExecutionRequest,
    handlers: ExecutionStreamHandlers,
    signal?: AbortSignal,
  ): Promise<string> {
    const pinned = this.pinProfile(request);
    if (resolveExecutionMode(pinned) !== "stream") {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "stream() requires mode=stream");
    }
    const capabilities = await this.getCapabilities(signal);
    this.assertRequestAgainstCapabilities(pinned, capabilities);
    return this.client.stream(pinned, handlers, signal);
  }

  /** interactive 模式：capabilities.modes.interactive 预检后透传 WS 会话。 */
  async interactive(
    request: ExecutionRequest,
    handlers?: ExecutionInteractiveHandlers,
    signal?: AbortSignal,
  ): Promise<ExecutionInteractiveSession> {
    const pinned = this.pinProfile(request);
    const capabilities = await this.getCapabilities(signal);
    if (resolveExecutionModes(capabilities).interactive !== true) {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.modeNotSupported,
        `backend ${this.id} does not support interactive mode`,
      );
    }
    return this.client.interactive(pinned, handlers, signal);
  }

  async cancel(execId: string): Promise<boolean> {
    return this.client.cancel(execId);
  }

  /** 客户端不得自我提升 profile；统一按 descriptor.profile 发出。 */
  private pinProfile(request: ExecutionRequest): ExecutionRequest {
    if (request.profile !== undefined && request.profile !== this.descriptor.profile) {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.invalidRequest,
        `client may not self-promote profile: request=${request.profile}, backend=${this.descriptor.profile}`,
      );
    }
    const pinned: ExecutionRequest = { ...request, profile: this.descriptor.profile };
    if (pinned.pathMapping === undefined && this.descriptor.pathMapping !== undefined) {
      pinned.pathMapping = this.descriptor.pathMapping;
    }
    return pinned;
  }

  private assertCompatibleCapabilities(capabilities: ExecutionCapabilities): void {
    const version = capabilities?.version;
    if (version !== EXECUTION_WIRE.versions.v1 && version !== EXECUTION_WIRE.versions.v1_1) {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.backendUnavailable,
        `backend ${this.id} capabilities version mismatch: expected ${EXECUTION_WIRE.versions.v1} or ${EXECUTION_WIRE.versions.v1_1}, got ${String(version)}`,
      );
    }
    if (this.descriptor.profile === "sandbox-untrusted") {
      if (capabilities.uidIsolation !== true || capabilities.egressLocked !== true) {
        throw new ExecutionClientError(
          EXECUTION_WIRE.errorCodes.backendUnavailable,
          `backend ${this.id} declared profile=sandbox-untrusted but capabilities uidIsolation=${String(capabilities.uidIsolation)} egressLocked=${String(capabilities.egressLocked)}`,
        );
      }
    }
  }

  private assertRequestAgainstCapabilities(
    request: ExecutionRequest,
    capabilities: ExecutionCapabilities,
  ): void {
    const mode = resolveExecutionMode(request);
    if (mode === "persistent" || !resolveExecutionModes(capabilities)[mode]) {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.modeNotSupported,
        `backend ${this.id} does not support mode=${mode}`,
      );
    }
    if (request.pathMapping !== undefined && capabilities.pathMapping !== true) {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.invalidRequest,
        `backend ${this.id} does not support pathMapping`,
      );
    }
  }
}
