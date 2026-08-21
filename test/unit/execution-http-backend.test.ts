import { describe, expect, it } from "vitest";
import {
  EXECUTION_WIRE,
  ExecutionBackendDescriptorError,
  HttpExecutionBackend,
  validateExecutionBackendDescriptor,
  type ExecutionBackendDescriptor,
  type ExecutionCapabilities,
} from "@away_from/shared/execution";

const HOST_CAPABILITIES: ExecutionCapabilities = {
  version: EXECUTION_WIRE.version,
  streaming: false,
  cancel: false,
  cwdWhitelist: false,
  uidIsolation: false,
  egressLocked: false,
  pathMapping: true,
};

const SANDBOX_CAPABILITIES: ExecutionCapabilities = {
  version: EXECUTION_WIRE.version,
  streaming: true,
  cancel: true,
  cwdWhitelist: true,
  uidIsolation: true,
  egressLocked: true,
  pathMapping: false,
};

const RESULT = { stdout: "ok", stderr: "", exitCode: 0, timedOut: false };

function jsonResponse(status: number, body: unknown) {
  return {
    ok: status < 300,
    status,
    text: async () => JSON.stringify(body),
    json: async () => body,
  } as unknown as Response;
}

type RouteHandler = (url: string, init: RequestInit) => Response;

function fetchWith(handler: RouteHandler, calls: Array<{ url: string; init: RequestInit }>) {
  return (async (url: string, init: RequestInit) => {
    calls.push({ url, init });
    return handler(url, init);
  }) as unknown as typeof fetch;
}

describe("execution/v1 backend descriptor（P0 golden wire）", () => {
  it("golden JSON → 规范化 descriptor（url 去尾斜杠）", () => {
    const input = JSON.parse(`{
      "id": "local-lean",
      "url": "http://host.docker.internal:8787/",
      "profile": "host",
      "tokenEnv": "LOCAL_EXEC_SHARED_SECRET",
      "pathMapping": { "hostRoot": "/data/workspaces", "execRoot": "/Users/me/pi-triple-pth/.pi-platform-data/workspaces" },
      "required": true
    }`);
    expect(validateExecutionBackendDescriptor(input)).toEqual({
      id: "local-lean",
      url: "http://host.docker.internal:8787",
      profile: "host",
      tokenEnv: "LOCAL_EXEC_SHARED_SECRET",
      pathMapping: { hostRoot: "/data/workspaces", execRoot: "/Users/me/pi-triple-pth/.pi-platform-data/workspaces" },
      required: true,
    });
  });

  it("非法 descriptor fail-closed", () => {
    const base: ExecutionBackendDescriptor = { id: "sandbox", url: "http://sandbox:8080", profile: "sandbox-untrusted" };
    const bad = (patch: Record<string, unknown>) => () =>
      validateExecutionBackendDescriptor({ ...base, ...patch });
    expect(bad({ unknown: 1 })).toThrow(ExecutionBackendDescriptorError);
    expect(bad({ id: "Bad_id" })).toThrow(/id must match/);
    expect(bad({ url: "ftp://x" })).toThrow(/http or https/);
    expect(bad({ url: "http://x:8080?q=1" })).toThrow(/query or fragment/);
    expect(bad({ profile: "root" })).toThrow(/profile must be one of/);
    expect(bad({ tokenEnv: "NOT-A-NAME" })).toThrow(/environment variable name/);
    expect(bad({ pathMapping: { hostRoot: "/a" } })).toThrow(/pathMapping must be/);
    expect(bad({ required: "yes" })).toThrow(/required must be a boolean/);
    expect(bad({ url: "not a url" })).toThrow(/absolute URL/);
  });
});

describe("HttpExecutionBackend（P0：id/descriptor/capabilities/profile）", () => {
  it("构造即结构校验；id 与 descriptor.id 一致", () => {
    const backend = new HttpExecutionBackend({ descriptor: { id: "sandbox", url: "http://sandbox:8080", profile: "sandbox-untrusted" } });
    expect(backend.id).toBe("sandbox");
    expect(backend.descriptor.url).toBe("http://sandbox:8080");
    expect(() => new HttpExecutionBackend({ descriptor: { id: "", url: "http://x", profile: "host" } }))
      .toThrow(ExecutionBackendDescriptorError);
  });

  it("profile 固定：请求自报其他 profile 直接拒绝，且不发任何请求", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const backend = new HttpExecutionBackend({
      descriptor: { id: "sandbox", url: "http://sandbox:8080", profile: "sandbox-untrusted" },
      fetchLike: fetchWith(() => jsonResponse(500, {}), calls),
    });
    await expect(backend.execute({ cmd: "true", profile: "host" })).rejects.toMatchObject({
      name: "ExecutionClientError",
      code: EXECUTION_WIRE.errorCodes.invalidRequest,
    });
    expect(calls).toHaveLength(0);
  });

  it("缺省 profile 时强制以 descriptor.profile 发出；token 透传", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const fetchLike = fetchWith((url) => {
      if (url.endsWith(EXECUTION_WIRE.paths.capabilities)) return jsonResponse(200, SANDBOX_CAPABILITIES);
      return jsonResponse(200, RESULT);
    }, calls);
    const backend = new HttpExecutionBackend({
      descriptor: { id: "sandbox", url: "http://sandbox:8080", profile: "sandbox-untrusted" },
      token: "s3cret",
      fetchLike,
    });
    await backend.execute({ cmd: "true" });
    const exec = calls.find((c) => c.url.endsWith(EXECUTION_WIRE.paths.exec))!;
    expect(JSON.parse(exec.init.body as string)).toMatchObject({ cmd: "true", profile: "sandbox-untrusted" });
    expect((exec.init.headers as Record<string, string>).authorization).toBe("Bearer s3cret");
  });

  it("capabilities 缓存：TTL 内只探测一次，TTL=0 每次重新探测", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const fetchLike = fetchWith((url) => {
      if (url.endsWith(EXECUTION_WIRE.paths.capabilities)) return jsonResponse(200, HOST_CAPABILITIES);
      return jsonResponse(200, RESULT);
    }, calls);
    const backend = new HttpExecutionBackend({
      descriptor: { id: "local", url: "http://host:8787", profile: "host" },
      fetchLike,
      capabilitiesTtlMs: 60_000,
    });
    await backend.execute({ cmd: "a" });
    await backend.execute({ cmd: "b" });
    expect(calls.filter((c) => c.url.endsWith(EXECUTION_WIRE.paths.capabilities))).toHaveLength(1);

    const calls2: Array<{ url: string; init: RequestInit }> = [];
    const backend2 = new HttpExecutionBackend({
      descriptor: { id: "local", url: "http://host:8787", profile: "host" },
      fetchLike: fetchWith((url) => {
        if (url.endsWith(EXECUTION_WIRE.paths.capabilities)) return jsonResponse(200, HOST_CAPABILITIES);
        return jsonResponse(200, RESULT);
      }, calls2),
      capabilitiesTtlMs: 0,
    });
    await backend2.execute({ cmd: "a" });
    await backend2.execute({ cmd: "b" });
    expect(calls2.filter((c) => c.url.endsWith(EXECUTION_WIRE.paths.capabilities))).toHaveLength(2);
  });

  it("capabilities version 不匹配 → backend 不可用（fail-closed）", async () => {
    const backend = new HttpExecutionBackend({
      descriptor: { id: "sandbox", url: "http://sandbox:8080", profile: "sandbox-untrusted" },
      fetchLike: fetchWith(() => jsonResponse(200, { ...SANDBOX_CAPABILITIES, version: "execution/v0" }), []),
    });
    await expect(backend.execute({ cmd: "true" })).rejects.toMatchObject({
      code: EXECUTION_WIRE.errorCodes.backendUnavailable,
    });
  });

  it("sandbox-untrusted 安全不变量：uidIsolation/egressLocked 缺失 → backend 不可用", async () => {
    const caps = { ...SANDBOX_CAPABILITIES, uidIsolation: false };
    const backend = new HttpExecutionBackend({
      descriptor: { id: "sandbox", url: "http://sandbox:8080", profile: "sandbox-untrusted" },
      fetchLike: fetchWith(() => jsonResponse(200, caps), []),
    });
    await expect(backend.getCapabilities()).rejects.toMatchObject({
      code: EXECUTION_WIRE.errorCodes.backendUnavailable,
    });
  });

  it("host profile 不强制 uidIsolation/egressLocked", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const backend = new HttpExecutionBackend({
      descriptor: { id: "local", url: "http://host:8787", profile: "host" },
      fetchLike: fetchWith((url) => {
        if (url.endsWith(EXECUTION_WIRE.paths.capabilities)) return jsonResponse(200, HOST_CAPABILITIES);
        return jsonResponse(200, RESULT);
      }, calls),
    });
    await expect(backend.execute({ cmd: "true" })).resolves.toMatchObject({ stdout: "ok" });
  });

  it("descriptor.pathMapping 默认注入；请求自带 mapping 优先", async () => {
    const descriptorMapping = { hostRoot: "/data/workspaces", execRoot: "/Users/me/workspaces" };
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const backend = new HttpExecutionBackend({
      descriptor: {
        id: "local", url: "http://host:8787", profile: "host", pathMapping: descriptorMapping,
      },
      fetchLike: fetchWith((url) => {
        if (url.endsWith(EXECUTION_WIRE.paths.capabilities)) return jsonResponse(200, HOST_CAPABILITIES);
        return jsonResponse(200, RESULT);
      }, calls),
    });
    await backend.execute({ cmd: "lake build" });
    const first = calls.find((c) => c.url.endsWith(EXECUTION_WIRE.paths.exec))!;
    expect(JSON.parse(first.init.body as string).pathMapping).toEqual(descriptorMapping);

    const own = { hostRoot: "/data/other", execRoot: "/Users/me/other" };
    await backend.execute({ cmd: "lake build", pathMapping: own });
    const second = calls.filter((c) => c.url.endsWith(EXECUTION_WIRE.paths.exec))[1]!;
    expect(JSON.parse(second.init.body as string).pathMapping).toEqual(own);
  });

  it("capabilities 不支持的 stream/pathMapping 在发执行请求前拒绝", async () => {
    const caps = { ...HOST_CAPABILITIES, pathMapping: false };
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const backend = new HttpExecutionBackend({
      descriptor: { id: "local", url: "http://host:8787", profile: "host", pathMapping: { hostRoot: "/a", execRoot: "/b" } },
      fetchLike: fetchWith((url) => {
        if (url.endsWith(EXECUTION_WIRE.paths.capabilities)) return jsonResponse(200, caps);
        return jsonResponse(200, RESULT);
      }, calls),
    });
    await expect(backend.execute({ cmd: "true" })).rejects.toMatchObject({
      code: EXECUTION_WIRE.errorCodes.invalidRequest,
    });
    await expect(backend.stream({ cmd: "true" }, { onDone: () => {} })).rejects.toMatchObject({
      code: EXECUTION_WIRE.errorCodes.invalidRequest,
    });
    expect(calls.filter((c) => c.url.endsWith(EXECUTION_WIRE.paths.exec))).toHaveLength(0);
  });
});
