import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import {
  EXECUTION_LIMITS,
  EXECUTION_PROTOCOL_VERSION,
  EXECUTION_PROTOCOL_VERSION_V11,
  EXECUTION_SESSION_LIMITS,
  EXECUTION_WIRE,
  ExecutionBackendDescriptorError,
  ExecutionClientError,
  ExecutionHttpServer,
  HttpExecutionBackend,
  HttpExecutionClient,
  resolveExecutionMode,
  resolveExecutionModes,
  validateExecutionCapabilities,
  validateExecutionRequest,
  validateExecutionSessionCreateRequest,
  validateExecutionSessionExecuteRequest,
  validateExecutionSessionResetRequest,
  validateExecutionSessionSnapshotRequest,
  type ExecutionCapabilities,
  type ExecutionJob,
  type ExecutionJobBackend,
  type ExecutionJobHandlers,
  type ExecutionJobOutput,
  type ExecutionRequest,
  type ExecutionResult,
  type ExecutionWsServerFrame,
} from "@away_from/shared/execution";

const TOKEN = "v11-test-secret";

const V11_CAPS: ExecutionCapabilities = {
  version: EXECUTION_PROTOCOL_VERSION_V11,
  streaming: true,
  cancel: true,
  cwdWhitelist: false,
  uidIsolation: false,
  egressLocked: false,
  pathMapping: true,
  modes: { sync: true, stream: true, interactive: true, persistent: false },
};

const V1_SANDBOX_CAPS: ExecutionCapabilities = {
  version: EXECUTION_PROTOCOL_VERSION,
  streaming: true,
  cancel: true,
  cwdWhitelist: true,
  uidIsolation: true,
  egressLocked: true,
  pathMapping: false,
};

/* ── fake job backend（server 驱动面） ─────────────────────────────── */

class FakeJob implements ExecutionJob {
  readonly execId = randomUUID();
  status: "running" | "done" = "running";
  readonly stdin: string[] = [];
  readonly resizes: Array<{ cols: number; rows: number }> = [];
  cancelled = false;
  readonly resizeSupported: boolean;
  private readonly outputs: ExecutionJobOutput[] = [];
  private readonly handlers = new Set<ExecutionJobHandlers>();
  private result: ExecutionResult | undefined;

  writeStdin = (data: string): void => {
    this.stdin.push(data);
  };

  resize: ((cols: number, rows: number) => void) | undefined;

  constructor(options: { resizeSupported?: boolean } = {}) {
    this.resizeSupported = options.resizeSupported ?? true;
    this.resize = this.resizeSupported
      ? (cols, rows) => { this.resizes.push({ cols, rows }); }
      : undefined;
  }

  subscribe(handlers: ExecutionJobHandlers): () => void {
    this.handlers.add(handlers);
    return () => this.handlers.delete(handlers);
  }

  outputSnapshot(): ExecutionJobOutput[] {
    return [...this.outputs];
  }

  getResult(): ExecutionResult | undefined {
    return this.result;
  }

  cancel(): boolean {
    this.cancelled = true;
    return true;
  }

  emit(stream: "stdout" | "stderr", data: string): void {
    this.outputs.push({ stream, data });
    for (const h of [...this.handlers]) h.onOutput?.({ stream, data });
  }

  finish(result: Partial<ExecutionResult> = {}): void {
    this.result = { stdout: "", stderr: "", exitCode: 0, timedOut: false, ...result, execId: this.execId };
    this.status = "done";
    for (const h of [...this.handlers]) h.onDone?.(this.result!);
  }
}

class FakeExecutionBackend implements ExecutionJobBackend {
  readonly id = "fake";
  readonly capabilities: ExecutionCapabilities;
  readonly jobs = new Map<string, FakeJob>();
  readonly executed: ExecutionRequest[] = [];
  readonly started: ExecutionRequest[] = [];
  onStart: ((job: FakeJob, request: ExecutionRequest) => void) | undefined;
  makeJob: ((request: ExecutionRequest) => FakeJob) | undefined;

  constructor(capabilities: ExecutionCapabilities) {
    this.capabilities = capabilities;
  }

  async getCapabilities(): Promise<ExecutionCapabilities> {
    return this.capabilities;
  }

  async execute(request: ExecutionRequest): Promise<ExecutionResult> {
    this.executed.push(request);
    return { stdout: `sync:${String(request.cmd)}`, stderr: "", exitCode: 0, timedOut: false };
  }

  async startJob(request: ExecutionRequest): Promise<FakeJob> {
    this.started.push(request);
    const job = this.makeJob?.(request) ?? new FakeJob();
    this.jobs.set(job.execId, job);
    this.onStart?.(job, request);
    return job;
  }
}

const openServers: ExecutionHttpServer[] = [];
afterEach(async () => {
  await Promise.allSettled(openServers.splice(0).map((s) => s.close()));
});

async function startServer(options: {
  backend: FakeExecutionBackend;
  token?: string;
  capabilities?: ExecutionCapabilities;
  profile?: ExecutionRequest["profile"];
  pathMapping?: ExecutionRequest["pathMapping"];
}): Promise<{ server: ExecutionHttpServer; baseUrl: string }> {
  const server = new ExecutionHttpServer({
    backend: options.backend,
    token: options.token ?? TOKEN,
    capabilities: options.capabilities ?? options.backend.capabilities,
    profile: options.profile,
    pathMapping: options.pathMapping,
  });
  openServers.push(server);
  const port = await server.listen(0, "127.0.0.1");
  return { server, baseUrl: `http://127.0.0.1:${port}` };
}

async function postExec(baseUrl: string, body: unknown, token = TOKEN) {
  return fetch(`${baseUrl}${EXECUTION_WIRE.paths.exec}`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
    body: JSON.stringify(body),
  });
}

/* ── wire / validate（v1.1 模式框架契约） ──────────────────────────── */

describe("execution/v1.1 wire 契约", () => {
  it("v1.1 常量：mode、WS 路径/帧、persistent 路径、MODE_NOT_SUPPORTED", () => {
    expect(EXECUTION_PROTOCOL_VERSION_V11).toBe("execution/v1.1");
    expect(EXECUTION_WIRE.paths.ws).toBe("/exec/:id/ws");
    expect(EXECUTION_WIRE.wsFrames).toMatchObject({ stdin: "stdin", stdout: "stdout", resize: "resize", done: "done" });
    expect(EXECUTION_WIRE.paths.sessions).toBe("/sessions");
    expect(EXECUTION_WIRE.paths.sessionExecute).toBe("/sessions/:id/execute");
    expect(EXECUTION_WIRE.paths.sessionSnapshot).toBe("/sessions/:id/snapshot");
    expect(EXECUTION_WIRE.paths.sessionReset).toBe("/sessions/:id/reset");
    expect(EXECUTION_WIRE.paths.sessionRelease).toBe("/sessions/:id/release");
    expect(EXECUTION_WIRE.errorCodes.modeNotSupported).toBe("MODE_NOT_SUPPORTED");
  });

  it("v1.0 客户端 fail-closed：EXECUTION_WIRE.version 保持 execution/v1，v1.1 capabilities 令旧版本检查失配", () => {
    // 旧客户端实现是 `capabilities.version !== EXECUTION_WIRE.version → 拒绝`。
    // 该常量不变，因此旧客户端必然拒绝 v1.1 backend（部署顺序编排的前置条件）。
    expect(EXECUTION_PROTOCOL_VERSION).toBe("execution/v1");
    expect(V11_CAPS.version).not.toBe(EXECUTION_PROTOCOL_VERSION);
    expect(V11_CAPS.version === EXECUTION_PROTOCOL_VERSION).toBe(false);
  });

  it("mode 显式声明与 legacy stream 映射（不向 v1 请求注入 mode）", () => {
    const explicit = validateExecutionRequest({ cmd: "x", mode: "stream" });
    expect(explicit.mode).toBe("stream");
    expect(resolveExecutionMode(explicit)).toBe("stream");

    const legacy = validateExecutionRequest({ cmd: "x", stream: true });
    expect(legacy.mode).toBeUndefined();
    expect(resolveExecutionMode(legacy)).toBe("stream");

    expect(resolveExecutionMode(validateExecutionRequest({ cmd: "x" }))).toBe("sync");
  });

  it("mode/stream 冲突与非法 mode/pty fail-closed", () => {
    const rejects = [
      { cmd: "x", mode: "sync", stream: true },
      { cmd: "x", mode: "stream", stream: false },
      { cmd: "x", mode: "interactive", stream: true },
      { cmd: "x", mode: "blast" },
      { cmd: "x", mode: "persistent" }, // 结构合法（wire 定稿），下面 pty 才非法
    ];
    expect(() => validateExecutionRequest(rejects[0]!)).toThrow(/conflicts/);
    expect(() => validateExecutionRequest(rejects[1]!)).toThrow(/conflicts/);
    expect(() => validateExecutionRequest(rejects[2]!)).toThrow(/conflicts/);
    expect(() => validateExecutionRequest(rejects[3]!)).toThrow(/mode must be one of/);
    expect(validateExecutionRequest(rejects[4]!).mode).toBe("persistent");

    expect(() => validateExecutionRequest({ cmd: "x", pty: { cols: 80 } })).toThrow(/only allowed with mode=interactive/);
    expect(() => validateExecutionRequest({ cmd: "x", mode: "interactive", pty: { cols: 0, rows: 24 } })).toThrow(/cols/);
    const pty = validateExecutionRequest({ cmd: "x", mode: "interactive", pty: { cols: 80, rows: 24, term: "xterm-256color" } }).pty;
    expect(pty).toEqual({ cols: 80, rows: 24, term: "xterm-256color" });
  });

  it("capabilities v1/v1.1 校验与模式位图推导", () => {
    expect(validateExecutionCapabilities(V1_SANDBOX_CAPS).modes).toBeUndefined();
    expect(resolveExecutionModes(V1_SANDBOX_CAPS)).toEqual({
      sync: true, stream: true, interactive: false, persistent: false,
    });
    expect(resolveExecutionModes(V11_CAPS)).toEqual(V11_CAPS.modes);
    expect(() => validateExecutionCapabilities({ ...V11_CAPS, version: "execution/v0" })).toThrow(/version/);
    expect(() => validateExecutionCapabilities({ ...V11_CAPS, modes: undefined })).toThrow(/modes/);
    expect(() => validateExecutionCapabilities({ ...V11_CAPS, modes: { sync: true } })).toThrow(/modes.stream/);
  });

  it("persistent wire 规范定稿：session 请求校验 + limits", () => {
    expect(EXECUTION_SESSION_LIMITS).toMatchObject({ minLeaseMs: 5_000, maxLeaseMs: 86_400_000, defaultLeaseMs: 600_000 });
    expect(validateExecutionSessionCreateRequest({ leaseMs: 60_000 })).toEqual({ leaseMs: 60_000 });
    expect(() => validateExecutionSessionCreateRequest({ leaseMs: 1 })).toThrow(/leaseMs/);
    expect(() => validateExecutionSessionCreateRequest({ mode: "sync" })).toThrow(/unknown session create request field/);

    const exec = validateExecutionSessionExecuteRequest({ cmd: "ls", cwd: "/tmp", env: { A: "1" } }, { timeoutMs: 5_000 });
    expect(exec).toMatchObject({ cmd: "ls", cwd: "/tmp", env: { A: "1" }, timeoutMs: 5_000 });
    expect(() => validateExecutionSessionExecuteRequest({ cmd: "ls", mode: "stream" })).toThrow(/unknown session execute request field/);

    expect(validateExecutionSessionSnapshotRequest({ tag: "before-migration" })).toEqual({ tag: "before-migration" });
    expect(() => validateExecutionSessionSnapshotRequest({ tag: "" })).toThrow(/tag/);
    expect(validateExecutionSessionResetRequest({ snapshotId: "snap-1" })).toEqual({ snapshotId: "snap-1" });
    expect(() => validateExecutionSessionResetRequest({ snapshotId: "" })).toThrow(/snapshotId/);
  });
});

/* ── ExecutionHttpServer（sync / auth / 模式路由） ─────────────────── */

describe("ExecutionHttpServer sync + 认证 + 模式路由", () => {
  it("/health 免认证；/capabilities 与 /exec 要求 Bearer", async () => {
    const backend = new FakeExecutionBackend(V11_CAPS);
    const { baseUrl } = await startServer({ backend });

    const health = await fetch(`${baseUrl}${EXECUTION_WIRE.paths.health}`);
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ status: "ok" });

    const unauth = await fetch(`${baseUrl}${EXECUTION_WIRE.paths.capabilities}`);
    expect(unauth.status).toBe(401);
    expect(await unauth.json()).toMatchObject({ error: { code: EXECUTION_WIRE.errorCodes.unauthorized } });

    const caps = await fetch(`${baseUrl}${EXECUTION_WIRE.paths.capabilities}`, {
      headers: { authorization: `Bearer ${TOKEN}` },
    });
    expect(caps.status).toBe(200);
    expect(await caps.json()).toEqual(V11_CAPS);
  });

  it("token 未配置 → 503 SERVER_MISCONFIGURED（fail-closed）", async () => {
    const backend = new FakeExecutionBackend(V11_CAPS);
    const { baseUrl } = await startServer({ backend, token: "" });
    const res = await fetch(`${baseUrl}${EXECUTION_WIRE.paths.capabilities}`);
    expect(res.status).toBe(503);
    expect(await res.json()).toMatchObject({ error: { code: EXECUTION_WIRE.errorCodes.serverMisconfigured } });
  });

  it("sync 执行：profile 固定 + pathMapping 注入 + 结果形状", async () => {
    const backend = new FakeExecutionBackend(V11_CAPS);
    const mapping = { hostRoot: "/data/workspaces", execRoot: "/Users/me/workspaces" };
    const { baseUrl } = await startServer({ backend, profile: "host", pathMapping: mapping });

    const res = await postExec(baseUrl, { cmd: "make", mode: "sync", cwd: "/data/workspaces/proj" });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ stdout: "sync:make", exitCode: 0, timedOut: false });
    expect(backend.executed[0]).toMatchObject({
      cmd: "make",
      mode: "sync",
      profile: "host",
      cwd: "/data/workspaces/proj",
      pathMapping: mapping,
    });
  });

  it("客户端自我提升 profile → 400 INVALID_REQUEST，backend 不被调用", async () => {
    const backend = new FakeExecutionBackend(V11_CAPS);
    const { baseUrl } = await startServer({ backend, profile: "sandbox-untrusted" });
    const res = await postExec(baseUrl, { cmd: "x", mode: "sync", profile: "host" });
    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({ error: { code: EXECUTION_WIRE.errorCodes.invalidRequest } });
    expect(backend.executed).toHaveLength(0);
  });

  it("persistent 请求 → 400 MODE_NOT_SUPPORTED；声明 persistent:true 的 server fail-closed", async () => {
    const backend = new FakeExecutionBackend(V11_CAPS);
    const { baseUrl } = await startServer({ backend });
    const res = await postExec(baseUrl, { cmd: "x", mode: "persistent" });
    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({ error: { code: EXECUTION_WIRE.errorCodes.modeNotSupported } });
    expect(backend.jobs.size).toBe(0);

    const caps: ExecutionCapabilities = { ...V11_CAPS, modes: { sync: true, stream: true, interactive: true, persistent: true } };
    const { baseUrl: badUrl } = await startServer({ backend, capabilities: caps });
    const bad = await fetch(`${badUrl}${EXECUTION_WIRE.paths.capabilities}`, { headers: { authorization: `Bearer ${TOKEN}` } });
    expect(bad.status).toBe(503);
    expect(await bad.json()).toMatchObject({ error: { code: EXECUTION_WIRE.errorCodes.serverMisconfigured } });
  });
});

/* ── stream（SSE） ─────────────────────────────────────────────────── */

describe("ExecutionHttpServer stream（SSE）", () => {
  it("mode=stream → execId → SSE output/done；GET /exec/:id 回放", async () => {
    const backend = new FakeExecutionBackend(V11_CAPS);
    backend.onStart = (job) => {
      job.emit("stdout", "early\n");
      setTimeout(() => {
        job.emit("stdout", "late\n");
        job.finish({ stdout: "early\nlate\n", exitCode: 0 });
      }, 30);
    };
    const { baseUrl } = await startServer({ backend });

    const submitted = await postExec(baseUrl, { cmd: "seq 2", mode: "stream" });
    expect(submitted.status).toBe(200);
    const { execId, status } = (await submitted.json()) as { execId: string; status: string };
    expect(status).toBe("running");
    expect(execId).toBeTruthy();

    const sse = await fetch(`${baseUrl}/exec/${execId}/stream`, { headers: { authorization: `Bearer ${TOKEN}` } });
    expect(sse.status).toBe(200);
    const text = await sse.text();
    expect(text).toContain("event: output");
    expect(text).toContain('"stream":"stdout"');
    expect(text).toContain("early\\n");
    expect(text).toContain("late\\n");
    expect(text).toContain("event: done");
    expect(text).toContain('"exitCode":0');

    const state = await fetch(`${baseUrl}/exec/${execId}`, { headers: { authorization: `Bearer ${TOKEN}` } });
    const stateBody = (await state.json()) as { status: string; result: ExecutionResult };
    expect(stateBody.status).toBe("done");
    expect(stateBody.result).toMatchObject({ exitCode: 0, stdout: "early\nlate\n", execId });
  });

  it("legacy stream:true（无 mode）映射为 stream；client.stream 全链路消费", async () => {
    const backend = new FakeExecutionBackend(V11_CAPS);
    backend.onStart = (job) => {
      job.emit("stdout", "ok\n");
      job.finish({ stdout: "ok\n" });
    };
    const { baseUrl } = await startServer({ backend });

    const outputs: string[] = [];
    let done = false;
    const client = new HttpExecutionClient({ baseUrl, token: TOKEN });
    const execId = await client.stream({ cmd: "echo ok", stream: true }, {
      onOutput: (e) => outputs.push(e.data),
      onDone: () => { done = true; },
    });
    expect(execId).toBeTruthy();
    expect(outputs.join("")).toBe("ok\n");
    expect(done).toBe(true);
    expect(backend.started[0]).toMatchObject({ stream: true });
  });
});

/* ── interactive（WS） ─────────────────────────────────────────────── */

describe("ExecutionHttpServer interactive（WS）", () => {
  it("HttpExecutionClient.interactive 全链路：pty 请求 → stdin → stdout → done", async () => {
    const backend = new FakeExecutionBackend(V11_CAPS);
    backend.onStart = (job) => {
      const original = job.writeStdin.bind(job);
      job.writeStdin = (data: string) => {
        original(data);
        job.emit("stdout", `echo:${data}`);
      };
    };
    const { baseUrl } = await startServer({ backend });

    const outputs: string[] = [];
    const client = new HttpExecutionClient({ baseUrl, token: TOKEN });
    const session = await client.interactive(
      { cmd: "sh", mode: "interactive", pty: { cols: 80, rows: 24, term: "xterm-256color" } },
      { onOutput: (e) => outputs.push(e.data) },
    );
    expect(session.execId).toBeTruthy();

    session.writeStdin("hello\n");
    session.resize(120, 40);
    const job = backend.jobs.get(session.execId)!;
    // resize 经 WS 送达后再触发完成，保证帧顺序可测
    setTimeout(() => job.finish({ stdout: "echo:hello\n", exitCode: 0 }), 30);

    const done = await session.done;
    expect(done).toMatchObject({ exitCode: 0, timedOut: false });
    expect(outputs.join("")).toBe("echo:hello\n");
    expect(job.stdin).toEqual(["hello\n"]);
    expect(job.resizes).toEqual([{ cols: 120, rows: 40 }]);
    expect(backend.started[0]).toMatchObject({ mode: "interactive", pty: { cols: 80, rows: 24, term: "xterm-256color" } });
  });

  it("WS 帧契约：启动前输出补发 + stdin/resize/done JSON frame", async () => {
    const backend = new FakeExecutionBackend(V11_CAPS);
    backend.onStart = (job) => {
      job.emit("stdout", "early\n");
    };
    const { baseUrl } = await startServer({ backend });

    const submitted = await postExec(baseUrl, { cmd: "sh", mode: "interactive", pty: { cols: 80, rows: 24 } });
    const { execId } = (await submitted.json()) as { execId: string };

    const frames: ExecutionWsServerFrame[] = [];
    const ws = new WebSocket(`ws://127.0.0.1:${new URL(baseUrl).port}${EXECUTION_WIRE.paths.ws.replace(":id", execId)}`, {
      headers: { authorization: `Bearer ${TOKEN}` },
    });
    const closed = new Promise<void>((resolve) => ws.on("close", () => resolve()));
    ws.on("message", (raw) => frames.push(JSON.parse(raw.toString()) as ExecutionWsServerFrame));

    await new Promise<void>((resolve, reject) => {
      ws.on("open", resolve);
      ws.on("error", reject);
    });
    ws.send(JSON.stringify({ type: "stdin", data: "hi\n" }));
    ws.send(JSON.stringify({ type: "resize", cols: 100, rows: 30 }));
    const job = backend.jobs.get(execId)!;
    setTimeout(() => job.finish({ stdout: "early\n", exitCode: 0 }), 30);
    await closed;

    expect(frames[0]).toEqual({ type: "stdout", data: "early\n" });
    expect(frames.at(-1)).toMatchObject({ type: "done", exitCode: 0, timedOut: false });
    expect(job.stdin).toEqual(["hi\n"]);
    expect(job.resizes).toEqual([{ cols: 100, rows: 30 }]);
  });

  it("WS 握手认证失败 → 401；任务不存在 → 404", async () => {
    const backend = new FakeExecutionBackend(V11_CAPS);
    const { baseUrl } = await startServer({ backend });
    const submitted = await postExec(baseUrl, { cmd: "sh", mode: "interactive" });
    const { execId } = (await submitted.json()) as { execId: string };
    const port = new URL(baseUrl).port;

    const badToken = new WebSocket(`ws://127.0.0.1:${port}/exec/${execId}/ws`, { headers: { authorization: "Bearer wrong" } });
    await expect(new Promise<void>((_, reject) => {
      badToken.on("error", (error) => reject(error));
    })).rejects.toThrow(/401/);

    const missing = new WebSocket(`ws://127.0.0.1:${port}/exec/nope/ws`, { headers: { authorization: `Bearer ${TOKEN}` } });
    await expect(new Promise<void>((_, reject) => {
      missing.on("error", (error) => reject(error));
    })).rejects.toThrow(/404/);
  });

  it("backend 不支持 resize → WS error 帧（MODE_NOT_SUPPORTED），连接保持", async () => {
    const backend = new FakeExecutionBackend(V11_CAPS);
    backend.makeJob = () => new FakeJob({ resizeSupported: false });
    const { baseUrl } = await startServer({ backend });

    const submitted = await postExec(baseUrl, { cmd: "sh", mode: "interactive" });
    const { execId } = (await submitted.json()) as { execId: string };
    const port = new URL(baseUrl).port;
    const frames: ExecutionWsServerFrame[] = [];
    const ws = new WebSocket(`ws://127.0.0.1:${port}/exec/${execId}/ws`, { headers: { authorization: `Bearer ${TOKEN}` } });
    await new Promise<void>((resolve, reject) => {
      ws.on("open", resolve);
      ws.on("error", reject);
    });
    ws.on("message", (raw) => frames.push(JSON.parse(raw.toString()) as ExecutionWsServerFrame));
    ws.send(JSON.stringify({ type: "resize", cols: 100, rows: 30 }));
    await new Promise((r) => setTimeout(r, 50));
    expect(frames).toEqual([{ type: "error", code: EXECUTION_WIRE.errorCodes.modeNotSupported, message: expect.any(String) }]);
    expect(ws.readyState).toBe(WebSocket.OPEN);
    backend.jobs.get(execId)!.finish({ exitCode: 0 });
    await new Promise<void>((resolve) => ws.on("close", () => resolve()));
  });
});

/* ── HttpExecutionBackend v1.1 协商 ────────────────────────────────── */

describe("HttpExecutionBackend v1/v1.1 协商", () => {
  it("v1.1 capabilities 被接受（不再版本失配）；modes.sync=false → MODE_NOT_SUPPORTED", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const jsonResponse = (status: number, body: unknown) =>
      ({ ok: status < 300, status, text: async () => JSON.stringify(body), json: async () => body }) as unknown as Response;
    const fetchLike = (async (url: string, init: RequestInit) => {
      calls.push({ url, init });
      if (url.endsWith(EXECUTION_WIRE.paths.capabilities)) return jsonResponse(200, V11_CAPS);
      return jsonResponse(200, { stdout: "ok", stderr: "", exitCode: 0, timedOut: false });
    }) as unknown as typeof fetch;

    const backend = new HttpExecutionBackend({
      descriptor: { id: "v11", url: "http://v11:8080", profile: "host" },
      fetchLike,
      capabilitiesTtlMs: 0,
    });
    await expect(backend.execute({ cmd: "x", mode: "sync" })).resolves.toMatchObject({ stdout: "ok" });

    const noSync = new HttpExecutionBackend({
      descriptor: { id: "v11-nosync", url: "http://v11:8080", profile: "host" },
      fetchLike: (async (url: string) => {
        if (url.endsWith(EXECUTION_WIRE.paths.capabilities)) {
          return jsonResponse(200, { ...V11_CAPS, modes: { ...V11_CAPS.modes!, sync: false } });
        }
        return jsonResponse(200, {});
      }) as unknown as typeof fetch,
      capabilitiesTtlMs: 0,
    });
    await expect(noSync.execute({ cmd: "x", mode: "sync" })).rejects.toMatchObject({
      name: "ExecutionClientError",
      code: EXECUTION_WIRE.errorCodes.modeNotSupported,
    });
  });

  it("interactive 预检：modes.interactive=false → MODE_NOT_SUPPORTED 且不发 /exec", async () => {
    const calls: string[] = [];
    const fetchLike = (async (url: string) => {
      calls.push(url);
      const body = url.endsWith(EXECUTION_WIRE.paths.capabilities)
        ? { ...V11_CAPS, modes: { ...V11_CAPS.modes!, interactive: false } }
        : { stdout: "ok", stderr: "", exitCode: 0, timedOut: false };
      return { ok: true, status: 200, text: async () => JSON.stringify(body), json: async () => body } as unknown as Response;
    }) as unknown as typeof fetch;
    const backend = new HttpExecutionBackend({
      descriptor: { id: "v11", url: "http://v11:8080", profile: "host" },
      fetchLike,
      capabilitiesTtlMs: 0,
    });
    await expect(backend.interactive({ cmd: "sh", mode: "interactive" })).rejects.toMatchObject({
      code: EXECUTION_WIRE.errorCodes.modeNotSupported,
    });
    expect(calls.filter((u) => u.endsWith(EXECUTION_WIRE.paths.exec))).toHaveLength(0);
  });

  it("descriptor 校验不受 v1.1 影响（P0 golden 回归）", () => {
    expect(() => validateExecutionRequest({ cmd: "x" })).not.toThrow();
    expect(() => new HttpExecutionBackend({ descriptor: { id: "x", url: "http://x", profile: "host", unknown: 1 as never } }))
      .toThrow(ExecutionBackendDescriptorError);
    expect(EXECUTION_LIMITS.maxOutputBytes).toBe(4 * 1024 * 1024);
  });
});
