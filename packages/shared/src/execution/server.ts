/**
 * execution/server.ts —— ExecutionHttpServer（execution/v1.1 服务端唯一实现）。
 *
 * HTTP + WS、模式路由（sync/stream/interactive/persistent）、Bearer 校验（常数时间比较）、
 * 结构化错误信封。persistent 模式由可选 `sessions` 后端驱动（P4）：
 * 未装配 sessions 时 capabilities 声明 persistent:true 仍 fail-closed 拒绝。
 *
 * 驱动面 = ExecutionJobBackend + ExecutionSessionBackend：
 *  - sync        → backend.execute()
 *  - stream      → backend.startJob() + GET /exec/:id/stream（SSE）
 *  - interactive → backend.startJob() + GET /exec/:id/ws（WS JSON frame）
 *  - persistent  → sessions.* + /sessions（租约/snapshot/reset/release 由会话管理器维护）
 */

import { createServer, type IncomingMessage, type ServerResponse, type Server as HttpServer } from "node:http";
import type { Duplex } from "node:stream";
import { createHash, timingSafeEqual } from "node:crypto";
import { WebSocket, WebSocketServer } from "ws";
import { ExecutionClientError } from "./client.js";
import type {
  ExecutionBackend,
  ExecutionCapabilities,
  ExecutionErrorEvent,
  ExecutionJob,
  ExecutionJobBackend,
  ExecutionJobOutput,
  ExecutionOutputEvent,
  ExecutionPathMapping,
  ExecutionProfile,
  ExecutionRequest,
  ExecutionResult,
  ExecutionSessionBackend,
  ExecutionWsServerFrame,
} from "./types.js";
import {
  ExecutionRequestError,
  EXECUTION_LIMITS,
  resolveExecutionMode,
  resolveExecutionModes,
  validateExecutionCapabilities,
  validateExecutionRequest,
} from "./validate.js";
import { EXECUTION_WIRE } from "./wire.js";
import { ExecutionSessionManager } from "./sessions.js";

type ServerBackend = ExecutionBackend & { startJob?: ExecutionJobBackend["startJob"] };

export interface ExecutionHttpServerOptions {
  /** 执行后端；stream/interactive 需要实现 startJob() */
  backend: ServerBackend;
  /**
   * 对外能力声明。缺省直接用 backend.getCapabilities()；
   * tool containers / 本地执行器可显式提升为 execution/v1.1 位图。
   */
  capabilities?: ExecutionCapabilities | (() => ExecutionCapabilities | Promise<ExecutionCapabilities>);
  /** Bearer 共享密钥；getter 每次请求读取（支持测试注入/env 热更新） */
  token?: string | (() => string | undefined);
  /** persistent 会话后端；装配后服务端把 capabilities.modes.persistent 声明为 true（P4） */
  sessions?: ExecutionSessionBackend;
  /** 会话 execute 缺省（超时/输出上限） */
  sessionDefaults?: { timeoutMs?: number; maxStdoutBytes?: number; maxStderrBytes?: number };
  /** 强制信任档：请求自报其他 profile 一律 INVALID_REQUEST（不得自我提升） */
  profile?: ExecutionProfile;
  /** server 默认路径映射（请求自带 mapping 优先） */
  pathMapping?: ExecutionPathMapping;
  defaults?: { timeoutMs?: number; maxStdoutBytes?: number; maxStderrBytes?: number };
  /** JSON body 上限（默认 1MB） */
  maxBodyBytes?: number;
  /** WS 单帧上限（默认 1MB） */
  maxWsPayloadBytes?: number;
}

interface ServerJobEntry {
  job: ExecutionJob;
  outputs: ExecutionJobOutput[];
  result?: ExecutionResult;
  error?: ExecutionErrorEvent;
  done: boolean;
  listeners: Set<ServerJobListener>;
  cleanupTimer: ReturnType<typeof setTimeout> | null;
}

type ServerJobEvent =
  | { type: "output"; event: ExecutionOutputEvent }
  | { type: "done"; result: ExecutionResult }
  | { type: "error"; event: ExecutionErrorEvent };

type ServerJobListener = (event: ServerJobEvent) => void;

const JOB_KEEP_MS = 60_000;

export class ExecutionHttpServer {
  readonly httpServer: HttpServer;

  private readonly backend: ServerBackend;
  private readonly capabilitiesOption: ExecutionHttpServerOptions["capabilities"];
  private readonly tokenOption: ExecutionHttpServerOptions["token"];
  private readonly sessionManager: ExecutionSessionManager | undefined;
  private readonly profile: ExecutionProfile | undefined;
  private readonly pathMapping: ExecutionPathMapping | undefined;
  private readonly defaults: Required<NonNullable<ExecutionHttpServerOptions["defaults"]>>;
  private readonly maxBodyBytes: number;
  private readonly jobs = new Map<string, ServerJobEntry>();
  private readonly wss: WebSocketServer;
  private capabilitiesPromise: Promise<ExecutionCapabilities> | undefined;
  private closing = false;

  constructor(options: ExecutionHttpServerOptions) {
    if (!options.backend || typeof options.backend.execute !== "function" || typeof options.backend.getCapabilities !== "function") {
      throw new TypeError("ExecutionHttpServer requires a backend with execute() and getCapabilities()");
    }
    this.backend = options.backend;
    this.capabilitiesOption = options.capabilities;
    this.tokenOption = options.token;
    this.sessionManager = options.sessions !== undefined
      ? new ExecutionSessionManager({ backend: options.sessions, sessionDefaults: options.sessionDefaults })
      : undefined;
    this.profile = options.profile;
    this.pathMapping = options.pathMapping;
    this.maxBodyBytes = options.maxBodyBytes ?? 1024 * 1024;
    this.defaults = {
      timeoutMs: options.defaults?.timeoutMs ?? 30_000,
      maxStdoutBytes: options.defaults?.maxStdoutBytes ?? 1024 * 1024,
      maxStderrBytes: options.defaults?.maxStderrBytes ?? 1024 * 1024,
    };
    this.wss = new WebSocketServer({ noServer: true, maxPayload: options.maxWsPayloadBytes ?? 1024 * 1024 });
    this.httpServer = createServer((req, res) => {
      void this.routeHttp(req, res).catch((error) => {
        if (!res.headersSent) this.sendError(res, error);
        else res.destroy();
      });
    });
    this.httpServer.on("upgrade", (req, socket, head) => this.handleUpgrade(req, socket, head));
  }

  /** 监听并返回实际端口；缺省 127.0.0.1:0（测试/动态端口注册表用） */
  async listen(port = 0, host = "127.0.0.1"): Promise<number> {
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => reject(error);
      this.httpServer.once("error", onError);
      this.httpServer.listen(port, host, () => {
        this.httpServer.removeListener("error", onError);
        resolve();
      });
    });
    const address = this.httpServer.address();
    if (!address || typeof address === "string") throw new Error("http server has no port address");
    return address.port;
  }

  /** 关闭：释放全部会话、断开 WS、尽力取消全部在飞任务、关闭 HTTP server */
  async close(): Promise<void> {
    this.closing = true;
    await this.sessionManager?.close();
    for (const ws of this.wss.clients) {
      try { ws.close(1001, "server shutdown"); } catch { /* 忽略 */ }
    }
    await Promise.allSettled([...this.jobs.values()].map((entry) => Promise.resolve().then(() => entry.job.cancel())));
    for (const entry of this.jobs.values()) {
      if (entry.cleanupTimer) clearTimeout(entry.cleanupTimer);
    }
    this.jobs.clear();
    await new Promise<void>((resolve) => this.httpServer.close(() => resolve()));
  }

  /* ── HTTP 路由 ──────────────────────────────────────────────────── */

  private async routeHttp(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const pathname = new URL(req.url ?? "/", "http://localhost").pathname;

    if (req.method === "GET" && pathname === EXECUTION_WIRE.paths.health) {
      this.sendJson(res, 200, { status: "ok" });
      return;
    }

    if (!this.enforceAuth(req, res)) return;

    if (req.method === "GET" && pathname === EXECUTION_WIRE.paths.capabilities) {
      const capabilities = await this.capabilitiesOr503(res);
      if (!capabilities) return;
      this.sendJson(res, 200, capabilities);
      return;
    }

    if (req.method === "POST" && pathname === EXECUTION_WIRE.paths.sessions) {
      await this.handleSessionCreate(req, res);
      return;
    }

    const sessionMatch = matchSessionPath(pathname);
    if (sessionMatch) {
      await this.routeSession(sessionMatch, req, res);
      return;
    }

    if (req.method === "POST" && pathname === EXECUTION_WIRE.paths.exec) {
      await this.handleExec(req, res);
      return;
    }

    const match = matchExecPath(pathname);
    if (!match) {
      this.sendError(res, new ExecutionClientError(EXECUTION_WIRE.errorCodes.notFound, `not found: ${pathname}`, 404));
      return;
    }
    if (match.suffix === undefined && req.method === "GET") {
      await this.handleJobState(match.id, res);
      return;
    }
    if (match.suffix === "stream" && req.method === "GET") {
      await this.handleStream(match.id, req, res);
      return;
    }
    if (match.suffix === "cancel" && req.method === "POST") {
      await this.handleCancel(match.id, res);
      return;
    }
    this.sendError(res, new ExecutionClientError(EXECUTION_WIRE.errorCodes.notFound, `not found: ${pathname}`, 404));
  }

  private sessionOrNotConfigured(): ExecutionSessionManager {
    if (!this.sessionManager) {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.modeNotSupported,
        "persistent sessions are not configured on this backend",
      );
    }
    return this.sessionManager;
  }

  private async handleSessionCreate(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const manager = this.sessionOrNotConfigured();
    const raw = await this.readJsonBody(req, res);
    if (raw === undefined) return;
    try {
      this.sendJson(res, 200, await manager.create(raw));
    } catch (error) {
      this.sendError(res, this.asWireRequestError(error));
    }
  }

  private async routeSession(
    match: NonNullable<ReturnType<typeof matchSessionPath>>,
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<void> {
    const manager = this.sessionOrNotConfigured();
    const { id, suffix } = match;
    if (suffix === undefined && req.method === "GET") {
      try {
        this.sendJson(res, 200, manager.get(id));
      } catch (error) {
        this.sendError(res, this.asWireRequestError(error));
      }
      return;
    }
    const readAnd = async <T>(fn: (raw: unknown) => Promise<T> | T): Promise<void> => {
      const raw = await this.readJsonBody(req, res);
      if (raw === undefined) return;
      try {
        this.sendJson(res, 200, await fn(raw));
      } catch (error) {
        this.sendError(res, this.asWireRequestError(error));
      }
    };
    if (suffix === "execute" && req.method === "POST") return readAnd((raw) => manager.execute(id, raw));
    if (suffix === "snapshot" && req.method === "POST") return readAnd((raw) => manager.snapshot(id, raw));
    if (suffix === "reset" && req.method === "POST") return readAnd((raw) => manager.reset(id, raw));
    if (suffix === "release" && req.method === "POST") return readAnd(() => manager.release(id));
    this.sendError(res, new ExecutionClientError(EXECUTION_WIRE.errorCodes.notFound, `not found: ${req.url ?? ""}`, 404));
  }

  private asWireRequestError(error: unknown): ExecutionClientError | Error {
    if (error instanceof ExecutionClientError) return error;
    if (error instanceof ExecutionRequestError) {
      return new ExecutionClientError(error.code, error.message);
    }
    return error instanceof Error ? error : new Error(String(error));
  }

  private async handleExec(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const raw = await this.readJsonBody(req, res);
    if (raw === undefined) return; // readJsonBody 已响应

    let normalized: ExecutionRequest;
    try {
      normalized = validateExecutionRequest(raw, this.defaults);
    } catch (error) {
      this.sendError(res, error instanceof ExecutionRequestError
        ? new ExecutionClientError(error.code, error.message)
        : error);
      return;
    }

    // 信任档固定：客户端不得自我提升
    if (this.profile !== undefined) {
      if (normalized.profile !== undefined && normalized.profile !== this.profile) {
        this.sendError(res, new ExecutionClientError(
          EXECUTION_WIRE.errorCodes.invalidRequest,
          `client may not self-promote profile: request=${normalized.profile}, backend=${this.profile}`,
        ));
        return;
      }
      normalized.profile = this.profile;
    }
    if (normalized.pathMapping === undefined && this.pathMapping !== undefined) {
      normalized.pathMapping = this.pathMapping;
    }

    const capabilities = await this.capabilitiesOr503(res);
    if (!capabilities) return;

    const mode = resolveExecutionMode(normalized);
    if (!resolveExecutionModes(capabilities)[mode]) {
      this.sendError(res, new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.modeNotSupported,
        `mode=${mode} is not supported by this backend`,
      ));
      return;
    }

    if (mode === "sync") {
      try {
        const result = await this.backend.execute(normalized);
        this.sendJson(res, 200, result);
      } catch (error) {
        this.sendError(res, error);
      }
      return;
    }

    if (typeof this.backend.startJob !== "function") {
      this.sendError(res, new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.serverMisconfigured,
        `backend lacks startJob() for mode=${mode}`,
      ));
      return;
    }

    let job: ExecutionJob;
    try {
      job = await this.backend.startJob(normalized);
    } catch (error) {
      this.sendError(res, error);
      return;
    }
    if (!job || typeof job.subscribe !== "function") {
      this.sendError(res, new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.serverMisconfigured,
        "backend.startJob() returned an invalid job handle",
      ));
      return;
    }
    if (mode === "interactive" && typeof job.writeStdin !== "function") {
      void Promise.resolve(job.cancel()).catch(() => undefined);
      this.sendError(res, new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.modeNotSupported,
        "backend job does not accept stdin; interactive unavailable",
      ));
      return;
    }

    this.registerJob(job);
    this.sendJson(res, 200, { execId: job.execId, status: "running" });
  }

  private async handleJobState(id: string, res: ServerResponse): Promise<void> {
    const entry = this.jobs.get(id);
    if (!entry) {
      this.sendError(res, new ExecutionClientError(EXECUTION_WIRE.errorCodes.notFound, "job not found", 404));
      return;
    }
    if (!entry.done) {
      this.sendJson(res, 200, { status: "running", execId: entry.job.execId });
      return;
    }
    this.sendJson(res, 200, { status: "done", execId: entry.job.execId, result: entry.result });
  }

  private async handleStream(id: string, req: IncomingMessage, res: ServerResponse): Promise<void> {
    const entry = this.jobs.get(id);
    if (!entry) {
      this.sendError(res, new ExecutionClientError(EXECUTION_WIRE.errorCodes.notFound, "job not found", 404));
      return;
    }
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    res.flushHeaders?.();

    const writeEvent = (event: string, data: unknown) => {
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    };
    for (const output of entry.outputs) {
      writeEvent(EXECUTION_WIRE.events.output, output);
    }

    const finishWithEntry = (): void => {
      if (entry.error) {
        writeEvent(EXECUTION_WIRE.events.error, entry.error);
      } else if (entry.result) {
        writeEvent(EXECUTION_WIRE.events.done, {
          exitCode: entry.result.exitCode ?? null,
          signal: entry.result.signal ?? null,
          timedOut: entry.result.timedOut,
        });
      }
      res.end();
    };
    if (entry.done) {
      finishWithEntry();
      return;
    }

    const remove = this.addListener(entry, (event) => {
      if (event.type === "output") writeEvent(EXECUTION_WIRE.events.output, event.event);
      else finishWithEntry();
    });
    req.on("close", remove);
  }

  private async handleCancel(id: string, res: ServerResponse): Promise<void> {
    const entry = this.jobs.get(id);
    if (!entry) {
      this.sendError(res, new ExecutionClientError(EXECUTION_WIRE.errorCodes.notFound, "job not found", 404));
      return;
    }
    await Promise.resolve(entry.job.cancel()).catch(() => undefined);
    this.sendJson(res, 200, { ok: true });
  }

  /* ── job 注册与生命周期 ─────────────────────────────────────────── */

  private registerJob(job: ExecutionJob): void {
    const entry: ServerJobEntry = {
      job,
      outputs: [...job.outputSnapshot()],
      result: job.getResult(),
      done: job.status === "done",
      listeners: new Set(),
      cleanupTimer: null,
    };
    this.jobs.set(job.execId, entry);
    job.subscribe({
      onOutput: (event) => {
        entry.outputs.push(event);
        for (const listener of [...entry.listeners]) listener({ type: "output", event });
      },
      onDone: (result) => {
        entry.result = result;
        entry.done = true;
        for (const listener of [...entry.listeners]) listener({ type: "done", result });
        this.scheduleCleanup(entry);
      },
      onError: (event) => {
        entry.error = event;
        entry.done = true;
        for (const listener of [...entry.listeners]) listener({ type: "error", event });
        this.scheduleCleanup(entry);
      },
    });
    if (entry.done) this.scheduleCleanup(entry);
  }

  private addListener(entry: ServerJobEntry, listener: ServerJobListener): () => void {
    entry.listeners.add(listener);
    return () => entry.listeners.delete(listener);
  }

  private scheduleCleanup(entry: ServerJobEntry): void {
    if (entry.cleanupTimer) return;
    entry.cleanupTimer = setTimeout(() => {
      entry.cleanupTimer = null;
      this.jobs.delete(entry.job.execId);
    }, JOB_KEEP_MS);
    entry.cleanupTimer.unref();
  }

  /* ── WebSocket（interactive） ───────────────────────────────────── */

  private handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void {
    const pathname = new URL(req.url ?? "/", "http://localhost").pathname;
    const match = matchExecPath(pathname);
    if (!match || match.suffix !== "ws") {
      rejectUpgrade(socket, 404, EXECUTION_WIRE.errorCodes.notFound, "ws endpoint not found");
      return;
    }
    const auth = this.checkAuth(req);
    if (!auth.ok) {
      rejectUpgrade(socket, auth.status, auth.code, auth.message);
      return;
    }
    const entry = this.jobs.get(match.id);
    if (!entry) {
      rejectUpgrade(socket, 404, EXECUTION_WIRE.errorCodes.notFound, "job not found");
      return;
    }
    this.wss.handleUpgrade(req, socket, head, (ws) => this.attachInteractive(ws, entry));
  }

  private attachInteractive(ws: WebSocket, entry: ServerJobEntry): void {
    const send = (frame: ExecutionWsServerFrame) => {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(frame));
    };

    for (const output of entry.outputs) {
      send({ type: output.stream, data: output.data });
    }
    const finish = (): void => {
      if (entry.error) {
        send({ type: EXECUTION_WIRE.wsFrames.error, code: entry.error.code, message: entry.error.message });
        ws.close(1011, "execution error");
      } else if (entry.result) {
        send({
          type: EXECUTION_WIRE.wsFrames.done,
          exitCode: entry.result.exitCode ?? null,
          ...(entry.result.signal !== undefined && entry.result.signal !== null ? { signal: entry.result.signal } : {}),
          timedOut: entry.result.timedOut,
        });
        ws.close(1000, "done");
      } else {
        send({ type: EXECUTION_WIRE.wsFrames.error, code: EXECUTION_WIRE.errorCodes.internalError, message: "job finished without result" });
        ws.close(1011, "internal error");
      }
    };
    if (entry.done) {
      finish();
      return;
    }

    const remove = this.addListener(entry, (event) => {
      if (event.type === "output") {
        send({ type: event.event.stream, data: event.event.data });
      } else {
        finish();
      }
    });

    ws.on("message", (raw) => {
      this.handleWsFrame(entry, ws, send, raw.toString());
    });
    ws.on("close", remove);
    ws.on("error", remove);
  }

  private handleWsFrame(
    entry: ServerJobEntry,
    ws: WebSocket,
    send: (frame: ExecutionWsServerFrame) => void,
    raw: string,
  ): void {
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      send({ type: EXECUTION_WIRE.wsFrames.error, code: EXECUTION_WIRE.errorCodes.invalidRequest, message: "WS frame must be JSON" });
      ws.close(1008, "invalid frame");
      return;
    }
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      send({ type: EXECUTION_WIRE.wsFrames.error, code: EXECUTION_WIRE.errorCodes.invalidRequest, message: "WS frame must be an object" });
      ws.close(1008, "invalid frame");
      return;
    }
    const frame = parsed as Record<string, unknown>;
    if (frame.type === EXECUTION_WIRE.wsFrames.stdin) {
      const data = frame.data;
      if (typeof data !== "string") {
        send({ type: EXECUTION_WIRE.wsFrames.error, code: EXECUTION_WIRE.errorCodes.invalidRequest, message: "stdin frame requires string data" });
        ws.close(1008, "invalid frame");
        return;
      }
      entry.job.writeStdin?.(data);
      return;
    }
    if (frame.type === EXECUTION_WIRE.wsFrames.resize) {
      const cols = frame.cols;
      const rows = frame.rows;
      if (!Number.isInteger(cols) || !Number.isInteger(rows) || (cols as number) < 1 || (rows as number) < 1) {
        send({ type: EXECUTION_WIRE.wsFrames.error, code: EXECUTION_WIRE.errorCodes.invalidRequest, message: "resize cols/rows must be positive integers" });
        ws.close(1008, "invalid frame");
        return;
      }
      if ((cols as number) > EXECUTION_LIMITS.maxPtyCols || (rows as number) > EXECUTION_LIMITS.maxPtyRows) {
        send({ type: EXECUTION_WIRE.wsFrames.error, code: EXECUTION_WIRE.errorCodes.invalidRequest, message: "resize exceeds pty size limit" });
        ws.close(1008, "invalid frame");
        return;
      }
      if (typeof entry.job.resize !== "function") {
        send({ type: EXECUTION_WIRE.wsFrames.error, code: EXECUTION_WIRE.errorCodes.modeNotSupported, message: "backend does not support pty resize" });
        return;
      }
      entry.job.resize(cols as number, rows as number);
      return;
    }
    send({ type: EXECUTION_WIRE.wsFrames.error, code: EXECUTION_WIRE.errorCodes.invalidRequest, message: `unknown WS frame type: ${String(frame.type)}` });
    ws.close(1008, "invalid frame");
  }

  /* ── 认证 / 通用 ────────────────────────────────────────────────── */

  private checkAuth(req: IncomingMessage): { ok: true } | { ok: false; status: number; code: string; message: string } {
    const secret = typeof this.tokenOption === "function" ? this.tokenOption() : this.tokenOption;
    if (!secret) {
      return {
        ok: false,
        status: 503,
        code: EXECUTION_WIRE.errorCodes.serverMisconfigured,
        message: "execution server token is not configured",
      };
    }
    const header = req.headers.authorization ?? "";
    const token = header.startsWith("Bearer ") ? header.slice("Bearer ".length) : header;
    if (!safeEqualStrings(token, secret)) {
      return {
        ok: false,
        status: 401,
        code: EXECUTION_WIRE.errorCodes.unauthorized,
        message: "unauthorized",
      };
    }
    return { ok: true };
  }

  private enforceAuth(req: IncomingMessage, res: ServerResponse): boolean {
    const auth = this.checkAuth(req);
    if (!auth.ok) {
      this.sendJson(res, auth.status, { error: { code: auth.code, message: auth.message } });
      return false;
    }
    return true;
  }

  private async capabilitiesOr503(res: ServerResponse): Promise<ExecutionCapabilities | undefined> {
    try {
      return await this.getCapabilities();
    } catch (error) {
      this.sendJson(res, 503, {
        error: {
          code: EXECUTION_WIRE.errorCodes.serverMisconfigured,
          message: `server capabilities invalid: ${error instanceof Error ? error.message : String(error)}`,
        },
      });
      return undefined;
    }
  }

  private getCapabilities(): Promise<ExecutionCapabilities> {
    if (!this.capabilitiesPromise) {
      this.capabilitiesPromise = (async () => {
        try {
          const source = this.capabilitiesOption;
          const raw = source === undefined
            ? await this.backend.getCapabilities()
            : typeof source === "function"
              ? await source()
              : source;
          const validated = validateExecutionCapabilities(raw);
          const capabilities = this.sessionManager !== undefined
            ? this.withPersistentAdvertised(validated)
            : validated;
          this.assertServerCanHonor(capabilities);
          return capabilities;
        } catch (error) {
          this.capabilitiesPromise = undefined;
          throw error;
        }
      })();
    }
    return this.capabilitiesPromise;
  }

  /**
   * 装配 sessions 后端后，persistent 就是服务端真实能力：
   *  - v1.1 capabilities：modes.persistent 强制置 true（声明必须可兑现）；
   *  - execution/v1 capabilities：升级为 v1.1 位图（sync 恒真，stream 取 streaming）。
   */
  private withPersistentAdvertised(capabilities: ExecutionCapabilities): ExecutionCapabilities {
    const modes = resolveExecutionModes(capabilities);
    if (modes.persistent) return capabilities;
    const merged = { ...modes, persistent: true };
    if (capabilities.version === EXECUTION_WIRE.versions.v1_1) {
      return { ...capabilities, modes: merged };
    }
    return { ...capabilities, version: EXECUTION_WIRE.versions.v1_1, modes: merged };
  }

  /** 声明了就必须可兑现；persistent 未装配 sessions 时声明即 fail-closed。 */
  private assertServerCanHonor(capabilities: ExecutionCapabilities): void {
    const modes = resolveExecutionModes(capabilities);
    if (modes.persistent && this.sessionManager === undefined) {
      throw new TypeError("capabilities declare modes.persistent=true but ExecutionHttpServer has no sessions backend");
    }
    if ((modes.stream || modes.interactive) && typeof this.backend.startJob !== "function") {
      throw new TypeError("capabilities declare stream/interactive but backend has no startJob()");
    }
    if (this.closing) throw new Error("server is closing");
  }

  private async readJsonBody(req: IncomingMessage, res: ServerResponse): Promise<unknown> {
    const chunks: Buffer[] = [];
    let size = 0;
    let tooLarge = false;
    await new Promise<void>((resolve, reject) => {
      req.on("data", (chunk: Buffer) => {
        size += chunk.length;
        if (size > this.maxBodyBytes) {
          tooLarge = true;
          resolve();
          return;
        }
        chunks.push(chunk);
      });
      req.on("end", resolve);
      req.on("error", reject);
    });
    if (tooLarge) {
      this.sendError(res, new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "request body too large", 413));
      return undefined;
    }
    if (chunks.length === 0) {
      this.sendError(res, new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "request body required"));
      return undefined;
    }
    try {
      return JSON.parse(Buffer.concat(chunks).toString("utf8"));
    } catch {
      this.sendError(res, new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "request body must be valid JSON"));
      return undefined;
    }
  }

  private sendError(res: ServerResponse, error: unknown): void {
    if (error instanceof ExecutionClientError) {
      this.sendJson(res, statusForCode(error.code, error.status), { error: { code: error.code, message: error.message } });
      return;
    }
    this.sendJson(res, 500, {
      error: {
        code: EXECUTION_WIRE.errorCodes.internalError,
        message: error instanceof Error ? error.message : String(error),
      },
    });
  }

  private sendJson(res: ServerResponse, status: number, body: unknown): void {
    if (res.headersSent) return;
    res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    res.end(JSON.stringify(body));
  }
}

/* ── helpers ──────────────────────────────────────────────────────── */

function matchSessionPath(pathname: string): { id: string; suffix: "execute" | "snapshot" | "reset" | "release" | undefined } | null {
  const m = /^\/sessions\/([^/]+)(?:\/(execute|snapshot|reset|release))?$/.exec(pathname);
  if (!m) return null;
  let id: string;
  try {
    id = decodeURIComponent(m[1]!);
  } catch {
    return null;
  }
  if (id.length === 0) return null;
  return { id, suffix: m[2] as "execute" | "snapshot" | "reset" | "release" | undefined };
}

function matchExecPath(pathname: string): { id: string; suffix: "stream" | "cancel" | "ws" | undefined } | null {
  const m = /^\/exec\/([^/]+)(?:\/(stream|cancel|ws))?$/.exec(pathname);
  if (!m) return null;
  let id: string;
  try {
    id = decodeURIComponent(m[1]!);
  } catch {
    return null;
  }
  if (id.length === 0) return null;
  return { id, suffix: m[2] as "stream" | "cancel" | "ws" | undefined };
}

function safeEqualStrings(a: string, b: string): boolean {
  const ha = createHash("sha256").update(a, "utf8").digest();
  const hb = createHash("sha256").update(b, "utf8").digest();
  return timingSafeEqual(ha, hb);
}

function statusForCode(code: string, explicitStatus?: number): number {
  if (explicitStatus !== undefined) return explicitStatus;
  switch (code) {
    case EXECUTION_WIRE.errorCodes.unauthorized: return 401;
    case EXECUTION_WIRE.errorCodes.notFound: return 404;
    case EXECUTION_WIRE.errorCodes.backendUnavailable: return 502;
    case EXECUTION_WIRE.errorCodes.serverMisconfigured: return 503;
    default: return 400;
  }
}

function rejectUpgrade(socket: Duplex, status: number, code: string, message: string): void {
  const body = JSON.stringify({ error: { code, message } });
  socket.write(
    `HTTP/1.1 ${status} ${status === 401 ? "Unauthorized" : status === 404 ? "Not Found" : "Error"}\r\n` +
    "Content-Type: application/json\r\n" +
    `Content-Length: ${Buffer.byteLength(body)}\r\n` +
    "Connection: close\r\n\r\n" +
    body,
  );
  socket.destroy();
}
