import { afterEach, describe, expect, it } from "vitest";
import {
  EXECUTION_PROTOCOL_VERSION,
  EXECUTION_PROTOCOL_VERSION_V11,
  EXECUTION_SESSION_LIMITS,
  EXECUTION_WIRE,
  ExecutionHttpServer,
  HttpExecutionClient,
  type ExecutionCapabilities,
  type ExecutionRequest,
  type ExecutionResult,
  type ExecutionSessionBackend,
  type ExecutionSessionExecuteRequest,
  type ExecutionSessionSnapshotRequest,
} from "@away_from/shared/execution";

const TOKEN = "persistent-test-secret";

const BASE_CAPS: ExecutionCapabilities = {
  version: EXECUTION_PROTOCOL_VERSION_V11,
  streaming: false,
  cancel: false,
  cwdWhitelist: false,
  uidIsolation: false,
  egressLocked: false,
  pathMapping: false,
  modes: { sync: true, stream: false, interactive: false, persistent: false },
};

const V1_CAPS: ExecutionCapabilities = {
  version: EXECUTION_PROTOCOL_VERSION,
  streaming: false,
  cancel: false,
  cwdWhitelist: false,
  uidIsolation: false,
  egressLocked: false,
  pathMapping: false,
};

class FakeExecutionBackend {
  readonly id = "fake";
  readonly capabilities = BASE_CAPS;
  readonly executed: ExecutionRequest[] = [];
  async getCapabilities(): Promise<ExecutionCapabilities> {
    return this.capabilities;
  }
  async execute(request: ExecutionRequest): Promise<ExecutionResult> {
    this.executed.push(request);
    return { stdout: `sync:${String(request.cmd)}`, stderr: "", exitCode: 0, timedOut: false };
  }
}

/** persistent 后端：token 编号 + 状态机（reset/snapshot/release 记录可断言） */
class FakeSessionBackend implements ExecutionSessionBackend {
  private next = 1;
  readonly created: string[] = [];
  readonly executed: Array<{ token: string; request: ExecutionSessionExecuteRequest }> = [];
  readonly snapshots: Array<{ token: string; request: ExecutionSessionSnapshotRequest }> = [];
  readonly resets: Array<{ token: string; snapshotId?: string }> = [];
  readonly released: string[] = [];
  private readonly states = new Map<string, { counters: number[] }>();

  async createSession(context?: unknown): Promise<string> {
    const token = `tok-${this.next++}`;
    this.created.push(token);
    this.states.set(token, { counters: [], context });
    return token;
  }

  async execute(token: string, request: ExecutionSessionExecuteRequest, _context?: unknown): Promise<ExecutionResult> {
    this.executed.push({ token, request });
    const state = this.states.get(token);
    if (!state) throw new Error("unknown backend token");
    const current = state.counters[state.counters.length - 1] ?? 0;
    state.counters[state.counters.length - 1] = current + 1;
    return { stdout: `${token}:${current + 1}:${String(request.cmd)}`, stderr: "", exitCode: current + 1 >= 3 ? 0 : 0, timedOut: false };
  }

  async snapshot(token: string, request: ExecutionSessionSnapshotRequest): Promise<{ snapshotId: string }> {
    this.snapshots.push({ token, request });
    return { snapshotId: `snap-${token}-${this.snapshots.length}` };
  }

  async reset(token: string, snapshotId?: string): Promise<void> {
    this.resets.push({ token, snapshotId });
  }

  async release(token: string): Promise<void> {
    this.released.push(token);
    this.states.delete(token);
  }
}

const openServers: ExecutionHttpServer[] = [];
afterEach(async () => {
  await Promise.allSettled(openServers.splice(0).map((s) => s.close()));
});

async function startServer(sessions?: ExecutionSessionBackend, capabilities = BASE_CAPS): Promise<{ server: ExecutionHttpServer; baseUrl: string }> {
  const server = new ExecutionHttpServer({
    backend: new FakeExecutionBackend(),
    token: TOKEN,
    capabilities,
    sessions,
  });
  openServers.push(server);
  const port = await server.listen(0, "127.0.0.1");
  return { server, baseUrl: `http://127.0.0.1:${port}` };
}

function auth(token = TOKEN): Record<string, string> {
  return { "content-type": "application/json", authorization: `Bearer ${token}` };
}

describe("persistent 会话 wire（P4：server + client）", () => {
  it("装配 sessions 后 capabilities 自动声明 persistent=true（v1.1 合并 / v1 升级）", async () => {
    const backend = new FakeSessionBackend();
    const { baseUrl } = await startServer(backend);
    const res = await fetch(`${baseUrl}${EXECUTION_WIRE.paths.capabilities}`, { headers: auth() });
    expect(res.status).toBe(200);
    const caps = await res.json() as ExecutionCapabilities;
    expect(caps.version).toBe(EXECUTION_PROTOCOL_VERSION_V11);
    expect(caps.modes).toEqual({ sync: true, stream: false, interactive: false, persistent: true });

    const { baseUrl: v1Url } = await startServer(backend, V1_CAPS);
    const v1 = await fetch(`${v1Url}${EXECUTION_WIRE.paths.capabilities}`, { headers: auth() });
    expect(v1.status).toBe(200);
    const v1Caps = await v1.json() as ExecutionCapabilities;
    expect(v1Caps.version).toBe(EXECUTION_PROTOCOL_VERSION_V11);
    expect(v1Caps.modes?.persistent).toBe(true);
  });

  it("create → get → execute 续租 → snapshot/reset/release 全链，后端 token 不出 wire", async () => {
    const backend = new FakeSessionBackend();
    const { baseUrl } = await startServer(backend);

    const created = await fetch(`${baseUrl}${EXECUTION_WIRE.paths.sessions}`, {
      method: "POST", headers: auth(), body: JSON.stringify({ leaseMs: 30_000 }),
    });
    expect(created.status).toBe(200);
    const createBody = await created.json() as { sessionId: string; leaseMs: number; status: string; expiresAt: number; createdAt: number };
    expect(createBody).toMatchObject({ status: "active", leaseMs: 30_000 });
    expect(createBody.expiresAt - createBody.createdAt).toBe(30_000);
    expect(createBody.sessionId).not.toContain("tok-");

    const got = await fetch(`${baseUrl}/sessions/${createBody.sessionId}`, { headers: auth() });
    expect(got.status).toBe(200);
    expect(await got.json()).toMatchObject({ sessionId: createBody.sessionId, status: "active", snapshotCount: 0 });

    const exec = await fetch(`${baseUrl}/sessions/${createBody.sessionId}/execute`, {
      method: "POST", headers: auth(), body: JSON.stringify({ cmd: ["echo", "hi"], timeoutMs: 1000 }),
    });
    expect(exec.status).toBe(200);
    const execBody = await exec.json() as { sessionId: string; stdout: string; exitCode: number };
    expect(execBody).toMatchObject({ sessionId: createBody.sessionId, exitCode: 0 });
    expect(execBody.stdout).toContain("tok-1");
    expect(backend.executed[0]?.request.cmd).toEqual(["echo", "hi"]);

    const snapshot = await fetch(`${baseUrl}/sessions/${createBody.sessionId}/snapshot`, {
      method: "POST", headers: auth(), body: JSON.stringify({ tag: "t1" }),
    });
    expect(snapshot.status).toBe(200);
    const snapBody = await snapshot.json() as { sessionId: string; snapshotId: string; tag?: string };
    expect(snapBody).toMatchObject({ sessionId: createBody.sessionId, tag: "t1" });
    expect(snapBody.snapshotId).toContain("snap-");

    const reset = await fetch(`${baseUrl}/sessions/${createBody.sessionId}/reset`, {
      method: "POST", headers: auth(), body: JSON.stringify({ snapshotId: snapBody.snapshotId }),
    });
    expect(reset.status).toBe(200);
    expect(await reset.json()).toEqual({ ok: true });
    expect(backend.resets[0]).toEqual({ token: "tok-1", snapshotId: snapBody.snapshotId });

    const release = await fetch(`${baseUrl}/sessions/${createBody.sessionId}/release`, {
      method: "POST", headers: auth(), body: JSON.stringify({}),
    });
    expect(release.status).toBe(200);
    expect(await release.json()).toEqual({ ok: true });

    const releasedGet = await fetch(`${baseUrl}/sessions/${createBody.sessionId}`, { headers: auth() });
    expect(await releasedGet.json()).toMatchObject({ status: "released" });

    const afterRelease = await fetch(`${baseUrl}/sessions/${createBody.sessionId}/execute`, {
      method: "POST", headers: auth(), body: JSON.stringify({ cmd: "x" }),
    });
    expect(afterRelease.status).toBe(400);
    expect(await afterRelease.json()).toMatchObject({ error: { code: EXECUTION_WIRE.errorCodes.sessionExpired } });
  });

  it("未知会话 404；引用不存在快照 404 SNAPSHOT_NOT_FOUND；坏请求 INVALID_REQUEST", async () => {
    const { baseUrl } = await startServer(new FakeSessionBackend());
    const unknown = await fetch(`${baseUrl}/sessions/nope`, { headers: auth() });
    expect(unknown.status).toBe(404);

    const created = await fetch(`${baseUrl}${EXECUTION_WIRE.paths.sessions}`, { method: "POST", headers: auth(), body: "{}" });
    const { sessionId } = await created.json() as { sessionId: string };
    const badReset = await fetch(`${baseUrl}/sessions/${sessionId}/reset`, {
      method: "POST", headers: auth(), body: JSON.stringify({ snapshotId: "ghost" }),
    });
    expect(badReset.status).toBe(404);
    expect(await badReset.json()).toMatchObject({ error: { code: EXECUTION_WIRE.errorCodes.snapshotNotFound } });

    const badLease = await fetch(`${baseUrl}${EXECUTION_WIRE.paths.sessions}`, {
      method: "POST", headers: auth(), body: JSON.stringify({ leaseMs: 1 }),
    });
    expect(badLease.status).toBe(400);
    expect(await badLease.json()).toMatchObject({ error: { code: EXECUTION_WIRE.errorCodes.invalidRequest } });
  });

  it("未装配 sessions：/sessions → MODE_NOT_SUPPORTED；声明 persistent:true 仍 fail-closed", async () => {
    const caps: ExecutionCapabilities = { ...BASE_CAPS, modes: { sync: true, stream: false, interactive: false, persistent: true } };
    const { baseUrl } = await startServer(undefined, caps);
    const capsRes = await fetch(`${baseUrl}${EXECUTION_WIRE.paths.capabilities}`, { headers: auth() });
    expect(capsRes.status).toBe(503);

    const create = await fetch(`${baseUrl}${EXECUTION_WIRE.paths.sessions}`, { method: "POST", headers: auth(), body: "{}" });
    expect(create.status).toBe(400);
    expect(await create.json()).toMatchObject({ error: { code: EXECUTION_WIRE.errorCodes.modeNotSupported } });
  });

  it("认证与客户端：HttpExecutionClient 会话 API 全链 + execute(mode=persistent) 引导到会话 API", async () => {
    const backend = new FakeSessionBackend();
    const { baseUrl } = await startServer(backend);
    const client = new HttpExecutionClient({ baseUrl, token: TOKEN });

    const created = await client.createSession({ leaseMs: EXECUTION_SESSION_LIMITS.defaultLeaseMs });
    expect(created.status).toBe("active");
    const got = await client.getSession(created.sessionId);
    expect(got).toMatchObject({ sessionId: created.sessionId, snapshotCount: 0 });

    const exec = await client.sessionExecute(created.sessionId, { cmd: "echo client" });
    expect(exec.stdout).toContain("tok-1");
    expect(exec.sessionId).toBe(created.sessionId);

    const snap = await client.sessionSnapshot(created.sessionId, { tag: "client" });
    expect(snap.tag).toBe("client");
    expect(await client.sessionReset(created.sessionId, { snapshotId: snap.snapshotId })).toEqual({ ok: true });
    expect(await client.sessionRelease(created.sessionId)).toEqual({ ok: true });
    await expect(client.getSession(created.sessionId)).resolves.toMatchObject({ status: "released" });

    await expect(client.execute({ cmd: "x", mode: "persistent" })).rejects.toMatchObject({
      code: EXECUTION_WIRE.errorCodes.invalidRequest,
    });
  });

  it("后端私有上下文透传（create context 不出 wire body）", async () => {
    const backend = new FakeSessionBackend();
    const manager = new (await import("@away_from/shared/execution")).ExecutionSessionManager({ backend });
    const created = await manager.create({}, { lang: "python", taskId: "task-1" });
    expect(backend.created).toContain("tok-1");
    expect(created.sessionId).not.toContain("task-1");
    expect((backend as unknown as { states: Map<string, { context?: unknown }> }).states.get("tok-1")?.context)
      .toEqual({ lang: "python", taskId: "task-1" });
    await manager.close();
  });

  it("租约过期：execute 抛 SESSION_EXPIRED 并释放后端会话", async () => {
    const backend = new FakeSessionBackend();
    let now = 1_000_000;
    const server = new ExecutionHttpServer({
      backend: new FakeExecutionBackend(),
      token: TOKEN,
      capabilities: BASE_CAPS,
      sessions: backend,
      sessionDefaults: { timeoutMs: 1_000 },
    });
    // 用带时钟的 manager 单测过期语义（server 装配用真实时钟，这里直接构造 manager）
    openServers.push(server);
    const { ExecutionSessionManager } = await import("@away_from/shared/execution");
    const manager = new ExecutionSessionManager({ backend, clock: () => now });
    const created = await manager.create({ leaseMs: 10_000 });
    now += 5_000;
    expect(await manager.execute(created.sessionId, { cmd: "x" })).toMatchObject({ sessionId: created.sessionId });
    now += 11_000;
    await expect(manager.execute(created.sessionId, { cmd: "y" })).rejects.toMatchObject({ code: EXECUTION_WIRE.errorCodes.sessionExpired });
    expect(manager.get(created.sessionId).status).toBe("expired");
    expect(backend.released).toContain("tok-1");
    await manager.close();
  });
});
