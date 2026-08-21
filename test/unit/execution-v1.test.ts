import { describe, expect, it } from "vitest";
import {
  EXECUTION_PROTOCOL_VERSION,
  EXECUTION_WIRE,
  validateExecutionRequest,
  ExecutionRequestError,
} from "@away_from/shared/execution";

describe("execution/v1 wire 契约（P0 冻结）", () => {
  it("版本与 sandbox 现有路由/事件名一致", () => {
    expect(EXECUTION_PROTOCOL_VERSION).toBe("execution/v1");
    expect(EXECUTION_WIRE.paths.exec).toBe("/exec");
    expect(EXECUTION_WIRE.paths.job).toBe("/exec/:id");
    expect(EXECUTION_WIRE.paths.stream).toBe("/exec/:id/stream");
    expect(EXECUTION_WIRE.paths.health).toBe("/health");
    expect(EXECUTION_WIRE.events.output).toBe("output");
    expect(EXECUTION_WIRE.events.done).toBe("done");
  });

  it("接受 sandbox 当前全部合法 payload 形状", () => {
    expect(validateExecutionRequest({ cmd: "ls -la" })).toMatchObject({ cmd: "ls -la" });
    expect(validateExecutionRequest({ cmd: ["ls", "-la"], cwd: "/data/workspaces/x" })).toMatchObject({
      cmd: ["ls", "-la"],
      cwd: "/data/workspaces/x",
    });
    const withAll = validateExecutionRequest({
      cmd: "make",
      cwd: "/data/workspaces/proj",
      env: { CC: "tcc" },
      timeoutMs: 10_000,
      maxStdoutBytes: 1024,
      maxStderrBytes: 2048,
      stream: true,
      profile: "sandbox-untrusted",
    });
    expect(withAll).toEqual({
      cmd: "make",
      cwd: "/data/workspaces/proj",
      env: { CC: "tcc" },
      timeoutMs: 10_000,
      maxStdoutBytes: 1024,
      maxStderrBytes: 2048,
      stream: true,
      profile: "sandbox-untrusted",
    });
  });

  it("默认值展开与 4MB 上限（sandbox 语义）", () => {
    const normalized = validateExecutionRequest({ cmd: "true" }, {
      timeoutMs: 30_000,
      maxStdoutBytes: 1024 * 1024,
      maxStderrBytes: 1024 * 1024,
    });
    expect(normalized.timeoutMs).toBe(30_000);
    expect(normalized.maxStdoutBytes).toBe(1024 * 1024);
    expect(() =>
      validateExecutionRequest({ cmd: "true", maxStdoutBytes: 5 * 1024 * 1024 }),
    ).toThrowError(ExecutionRequestError);
  });

  it("拒绝非法请求（错误码 INVALID_REQUEST）", () => {
    for (const bad of [
      null,
      {},
      { cmd: "" },
      { cmd: [] },
      { cmd: [1] },
      { cmd: "true", timeoutMs: 0 },
      { cmd: "true", timeoutMs: -1 },
      { cmd: "true", maxStdoutBytes: 0 },
      { cmd: "true", cwd: "" },
      { cmd: "true", env: { A: 1 } },
      { cmd: "true", stream: "yes" },
      { cmd: "true", profile: "root" },
      { cmd: "true", pathMapping: { hostRoot: "/h" } },
    ]) {
      try {
        validateExecutionRequest(bad);
        throw new Error(`should reject: ${JSON.stringify(bad)}`);
      } catch (error) {
        expect(error).toBeInstanceOf(ExecutionRequestError);
        expect((error as ExecutionRequestError).code).toBe(EXECUTION_WIRE.errorCodes.invalidRequest);
      }
    }
  });

  it("pathMapping 一等字段校验通过", () => {
    expect(
      validateExecutionRequest({
        cmd: "python3 driver.py",
        pathMapping: { hostRoot: "/Users/me/repo", execRoot: "/works/repo" },
      }).pathMapping,
    ).toEqual({ hostRoot: "/Users/me/repo", execRoot: "/works/repo" });
  });
});
