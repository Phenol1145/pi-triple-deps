import { describe, expect, it, vi } from "vitest";
import {
  EXECUTION_WIRE,
  ExecutionClientError,
  HttpExecutionClient,
} from "@away_from/shared/execution";

function jsonResponse(status: number, body: unknown, ok = status < 300) {
  return {
    ok,
    status,
    text: async () => JSON.stringify(body),
    json: async () => body,
  } as unknown as Response;
}

describe("execution/v1 HTTP client", () => {
  it("同步 execute → POST /exec 并解析 ExecutionResult", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const fakeFetch = (async (url: string, init: RequestInit) => {
      calls.push({ url, init });
      return jsonResponse(200, { stdout: "ok", stderr: "", exitCode: 0, timedOut: false });
    }) as unknown as typeof fetch;
    const client = new HttpExecutionClient({ baseUrl: "http://sandbox:8080/", token: "s", fetchLike: fakeFetch });
    const result = await client.execute({ cmd: "echo ok" });
    expect(result).toMatchObject({ stdout: "ok", exitCode: 0 });
    expect(calls[0]!.url).toBe("http://sandbox:8080/exec");
    expect((calls[0]!.init.headers as Record<string, string>).authorization).toBe("Bearer s");
  });

  it("stream:true execute → 轮询 GET /exec/:id 直到 done", async () => {
    let polls = 0;
    const fakeFetch = (async (url: string, init: RequestInit) => {
      if (init.method === "POST") return jsonResponse(200, { execId: "job-1", status: "running" });
      polls += 1;
      return jsonResponse(
        200,
        polls >= 2
          ? { status: "done", result: { stdout: "done", stderr: "", exitCode: 0, timedOut: false } }
          : { status: "running" },
      );
    }) as unknown as typeof fetch;
    const client = new HttpExecutionClient({ baseUrl: "http://sandbox:8080", fetchLike: fakeFetch, pollIntervalMs: 1 });
    const result = await client.execute({ cmd: "make", stream: true });
    expect(result).toMatchObject({ stdout: "done" });
    expect(polls).toBe(2);
  });

  it("stream() 解析 SSE output/done 事件", async () => {
    const sse = [
      `event: ${EXECUTION_WIRE.events.output}`,
      `data: ${JSON.stringify({ stream: "stdout", data: "hi" })}`,
      "",
      `event: ${EXECUTION_WIRE.events.done}`,
      `data: ${JSON.stringify({ exitCode: 0, timedOut: false })}`,
      "",
    ].join("\n");
    const encoder = new TextEncoder();
    const fakeFetch = (async (url: string, init: RequestInit) => {
      if (init.method === "POST") return jsonResponse(200, { execId: "job-2", status: "running" });
      return {
        ok: true,
        status: 200,
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(encoder.encode(sse));
            controller.close();
          },
        }),
      } as unknown as Response;
    }) as unknown as typeof fetch;
    const client = new HttpExecutionClient({ baseUrl: "http://sandbox:8080", fetchLike: fakeFetch });
    const outputs: string[] = [];
    let done: unknown;
    const execId = await client.stream(
      { cmd: "echo hi", stream: true },
      {
        onOutput: (e) => outputs.push(e.data),
        onDone: (e) => { done = e; },
      },
    );
    expect(execId).toBe("job-2");
    expect(outputs).toEqual(["hi"]);
    expect(done).toEqual({ exitCode: 0, timedOut: false });
  });

  it("cancel → POST /exec/:id/cancel；错误响应转 ExecutionClientError", async () => {
    const fakeFetch = (async (url: string, init: RequestInit) => {
      if (url.endsWith("/cancel")) return jsonResponse(200, { ok: true });
      return jsonResponse(400, { error: { code: EXECUTION_WIRE.errorCodes.invalidRequest, message: "bad" } });
    }) as unknown as typeof fetch;
    const client = new HttpExecutionClient({ baseUrl: "http://sandbox:8080", fetchLike: fakeFetch });
    await expect(client.cancel("job-x")).resolves.toBe(true);
    await expect(client.execute({ cmd: "true" })).rejects.toMatchObject({
      name: "ExecutionClientError",
      code: EXECUTION_WIRE.errorCodes.invalidRequest,
      status: 400,
    });
  });
});
