/**
 * execution/sessions.ts —— persistent 会话管理器（P4，wire 已定稿）。
 *
 * 职责（服务端唯一实现）：
 *  - sessionId（HTTP 面，随机 UUID）→ backend 私有 token（永不出 HTTP）；
 *  - 租约生命周期：create 定 lease；每次 execute 自动续租；过期访问 = SESSION_EXPIRED；
 *  - 快照登记与 reset 引用的存在性校验（SNAPSHOT_NOT_FOUND）；
 *  - release 幂等：released 后继续使用 = SESSION_EXPIRED。
 *
 * 执行面（sandbox kernel-host / 本地执行器）只实现 ExecutionSessionBackend，
 * 不重复维护 wire 状态机。
 */

import { randomUUID } from "node:crypto";
import type {
  ExecutionResult,
  ExecutionSession,
  ExecutionSessionBackend,
  ExecutionSessionCreateRequest,
  ExecutionSessionCreateResponse,
  ExecutionSessionExecuteRequest,
  ExecutionSessionExecuteResult,
  ExecutionSessionSnapshot,
  ExecutionSessionSnapshotRequest,
  ExecutionSessionResetRequest,
} from "./types.js";
import {
  validateExecutionSessionCreateRequest,
  validateExecutionSessionExecuteRequest,
  validateExecutionSessionResetRequest,
  validateExecutionSessionSnapshotRequest,
  EXECUTION_SESSION_LIMITS,
} from "./validate.js";
import { EXECUTION_WIRE } from "./wire.js";
import { ExecutionClientError } from "./client.js";

interface SessionSnapshotRecord {
  readonly snapshotId: string;
  readonly tag?: string;
  readonly createdAt: number;
}

interface SessionRecord {
  readonly sessionId: string;
  readonly token: string;
  status: "active" | "released" | "expired";
  readonly createdAt: number;
  expiresAt: number;
  readonly leaseMs: number;
  lastResult?: { exitCode: number | null; completedAt: number };
  readonly snapshots: Map<string, SessionSnapshotRecord>;
}

export interface ExecutionSessionManagerOptions {
  readonly backend: ExecutionSessionBackend;
  readonly clock?: () => number;
  readonly sessionDefaults?: { timeoutMs?: number; maxStdoutBytes?: number; maxStderrBytes?: number };
  readonly onSessionStatusChange?: (sessionId: string, status: "active" | "released" | "expired") => void;
}

function requireSessionBackendMethods(backend: ExecutionSessionBackend): void {
  for (const method of ["createSession", "execute", "snapshot", "reset", "release"] as const) {
    if (typeof backend[method] !== "function") {
      throw new TypeError(`ExecutionSessionBackend is missing ${method}()`);
    }
  }
}

function wireError(code: string, message: string, status?: number): ExecutionClientError {
  return new ExecutionClientError(code, message, status);
}

export class ExecutionSessionManager {
  private readonly backend: ExecutionSessionBackend;
  private readonly clock: () => number;
  private readonly defaults: Required<NonNullable<ExecutionSessionManagerOptions["sessionDefaults"]>>;
  private readonly onStatusChange: ExecutionSessionManagerOptions["onSessionStatusChange"];
  private readonly sessions = new Map<string, SessionRecord>();
  private closed = false;

  constructor(options: ExecutionSessionManagerOptions) {
    requireSessionBackendMethods(options.backend);
    this.backend = options.backend;
    this.clock = options.clock ?? Date.now;
    this.defaults = {
      timeoutMs: options.sessionDefaults?.timeoutMs ?? 30_000,
      maxStdoutBytes: options.sessionDefaults?.maxStdoutBytes ?? 1024 * 1024,
      maxStderrBytes: options.sessionDefaults?.maxStderrBytes ?? 1024 * 1024,
    };
    this.onStatusChange = options.onSessionStatusChange;
  }

  private assertOpen(): void {
    if (this.closed) throw wireError(EXECUTION_WIRE.errorCodes.serverMisconfigured, "session manager is closed");
  }

  private record(sessionId: string): SessionRecord | undefined {
    const rec = this.sessions.get(sessionId);
    if (!rec) return undefined;
    if (rec.status === "active" && this.clock() >= rec.expiresAt) {
      rec.status = "expired";
      this.onStatusChange?.(sessionId, "expired");
      void Promise.resolve(this.backend.release(rec.token)).catch(() => undefined);
    }
    return rec;
  }

  private active(sessionId: string): SessionRecord {
    const rec = this.record(sessionId);
    if (!rec) throw wireError(EXECUTION_WIRE.errorCodes.notFound, "session not found", 404);
    if (rec.status !== "active") {
      throw wireError(EXECUTION_WIRE.errorCodes.sessionExpired, `session is ${rec.status}`);
    }
    return rec;
  }

  private toWire(rec: SessionRecord): ExecutionSession {
    return {
      sessionId: rec.sessionId,
      status: rec.status,
      createdAt: rec.createdAt,
      expiresAt: rec.expiresAt,
      leaseMs: rec.leaseMs,
      ...(rec.lastResult ? { lastResult: { ...rec.lastResult } } : {}),
      snapshotCount: rec.snapshots.size,
    };
  }

  async create(raw: unknown, backendContext?: unknown): Promise<ExecutionSessionCreateResponse> {
    this.assertOpen();
    const request: ExecutionSessionCreateRequest = validateExecutionSessionCreateRequest(raw);
    const leaseMs = request.leaseMs ?? EXECUTION_SESSION_LIMITS.defaultLeaseMs;
    const token = await this.backend.createSession(backendContext);
    const now = this.clock();
    const rec: SessionRecord = {
      sessionId: randomUUID(),
      token,
      status: "active",
      createdAt: now,
      expiresAt: now + leaseMs,
      leaseMs,
      snapshots: new Map(),
    };
    this.sessions.set(rec.sessionId, rec);
    this.onStatusChange?.(rec.sessionId, "active");
    return { sessionId: rec.sessionId, status: "active", createdAt: rec.createdAt, expiresAt: rec.expiresAt, leaseMs };
  }

  get(sessionId: string): ExecutionSession {
    const rec = this.record(sessionId);
    if (!rec) throw wireError(EXECUTION_WIRE.errorCodes.notFound, "session not found", 404);
    return this.toWire(rec);
  }

  async execute(sessionId: string, raw: unknown, backendContext?: unknown): Promise<ExecutionSessionExecuteResult> {
    this.assertOpen();
    const rec = this.active(sessionId);
    const request: ExecutionSessionExecuteRequest = validateExecutionSessionExecuteRequest(raw, this.defaults);
    // 每次 execute 自动续租（先续后执行——长任务期间租约不回退）。
    rec.expiresAt = this.clock() + rec.leaseMs;
    const result: ExecutionResult = await this.backend.execute(rec.token, request, backendContext);
    rec.lastResult = { exitCode: result.exitCode ?? null, completedAt: this.clock() };
    return { ...result, sessionId: rec.sessionId };
  }

  async snapshot(sessionId: string, raw: unknown): Promise<ExecutionSessionSnapshot> {
    this.assertOpen();
    const rec = this.active(sessionId);
    const request: ExecutionSessionSnapshotRequest = validateExecutionSessionSnapshotRequest(raw);
    const snapshot = await this.backend.snapshot(rec.token, request);
    const record: SessionSnapshotRecord = {
      snapshotId: snapshot.snapshotId,
      ...(request.tag !== undefined ? { tag: request.tag } : {}),
      createdAt: this.clock(),
    };
    rec.snapshots.set(record.snapshotId, record);
    return {
      sessionId: rec.sessionId,
      snapshotId: record.snapshotId,
      ...(record.tag !== undefined ? { tag: record.tag } : {}),
      createdAt: record.createdAt,
      ...(snapshot.state !== undefined ? { state: snapshot.state } : {}),
    };
  }

  async reset(sessionId: string, raw: unknown): Promise<{ ok: true }> {
    this.assertOpen();
    const rec = this.active(sessionId);
    const request: ExecutionSessionResetRequest = validateExecutionSessionResetRequest(raw);
    if (request.snapshotId !== undefined && !rec.snapshots.has(request.snapshotId)) {
      throw wireError(EXECUTION_WIRE.errorCodes.snapshotNotFound, `snapshot not found: ${request.snapshotId}`, 404);
    }
    await this.backend.reset(rec.token, request.snapshotId);
    return { ok: true };
  }

  async release(sessionId: string): Promise<{ ok: true }> {
    this.assertOpen();
    const rec = this.sessions.get(sessionId);
    if (!rec) throw wireError(EXECUTION_WIRE.errorCodes.notFound, "session not found", 404);
    if (rec.status === "active") {
      await this.backend.release(rec.token);
      rec.status = "released";
      this.onStatusChange?.(sessionId, "released");
    }
    return { ok: true };
  }

  /** 服务关闭：释放全部仍未 released 的会话（幂等，绝不复用已过期 token）。 */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await Promise.allSettled([...this.sessions.values()].map((rec) => Promise.resolve().then(async () => {
      if (rec.status === "active") {
        try { await this.backend.release(rec.token); } catch { /* close 尽力而为 */ }
        rec.status = "released";
      }
    })));
  }
}
