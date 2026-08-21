/**
 * execution/local-backend.ts —— execution/v1 LocalBackend（host）。
 *
 * 信任模型：host。调用方即本地可信用户/工具；不提供 UID 降权与 cwd 白名单，
 * 但保持 execution/v1 的共性硬约束：超时杀进程组、输出上限截断。
 */

import { spawn } from "node:child_process";
import { StringDecoder } from "node:string_decoder";
import os from "node:os";
import type {
  ExecutionBackend,
  ExecutionCapabilities,
  ExecutionRequest,
  ExecutionResult,
} from "../types.js";
import { EXECUTION_WIRE } from "../wire.js";
import { validateExecutionRequest } from "../validate.js";
import { ExecutionClientError } from "../client.js";

export interface LocalBackendOptions {
  defaultTimeoutMs?: number;
  maxStdoutBytes?: number;
  maxStderrBytes?: number;
}

const CAPABILITIES: ExecutionCapabilities = {
  version: EXECUTION_WIRE.version,
  streaming: false,
  cancel: false,
  cwdWhitelist: false,
  uidIsolation: false,
  egressLocked: false,
  pathMapping: false,
};

export class LocalBackend implements ExecutionBackend {
  readonly id = "local";
  private readonly defaults: Required<LocalBackendOptions>;

  constructor(options: LocalBackendOptions = {}) {
    this.defaults = {
      defaultTimeoutMs: options.defaultTimeoutMs ?? 30_000,
      maxStdoutBytes: options.maxStdoutBytes ?? 1024 * 1024,
      maxStderrBytes: options.maxStderrBytes ?? 1024 * 1024,
    };
  }

  async getCapabilities(): Promise<ExecutionCapabilities> {
    return CAPABILITIES;
  }

  async execute(input: ExecutionRequest, signal?: AbortSignal): Promise<ExecutionResult> {
    const req = validateExecutionRequest(input, {
      timeoutMs: this.defaults.defaultTimeoutMs,
      maxStdoutBytes: this.defaults.maxStdoutBytes,
      maxStderrBytes: this.defaults.maxStderrBytes,
    });
    if (req.stream) {
      throw new ExecutionClientError(
        EXECUTION_WIRE.errorCodes.backendUnavailable,
        "local backend does not support streaming",
      );
    }
    if (req.profile !== undefined && req.profile !== "host") {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "local backend only accepts profile=host");
    }
    if (req.pathMapping) {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "local backend does not support pathMapping");
    }

    const cmdArray = Array.isArray(req.cmd) ? req.cmd : ["bash", "-lc", req.cmd];
    return new Promise<ExecutionResult>((resolve, reject) => {
      const child = spawn(cmdArray[0]!, cmdArray.slice(1), {
        cwd: req.cwd,
        env: req.env ? { ...process.env, ...req.env } : process.env,
        detached: true,
        stdio: ["ignore", "pipe", "pipe"],
      });

      const decOut = new StringDecoder("utf-8");
      const decErr = new StringDecoder("utf-8");
      let stdout = "";
      let stderr = "";
      let stdoutBytes = 0;
      let stderrBytes = 0;
      let truncated: ExecutionResult["truncated"];
      let timedOut = false;

      const killForLimit = (field: "stdout" | "stderr") => {
        if (truncated) return;
        truncated = { field, originalLen: field === "stdout" ? stdoutBytes : stderrBytes, keptLen: 0 };
        try { process.kill(-child.pid!, "SIGKILL"); } catch { /* 已退出 */ }
      };

      const handleData = (buf: Buffer, field: "stdout" | "stderr") => {
        const text = field === "stdout" ? decOut.write(buf) : decErr.write(buf);
        if (!text) return;
        const limit = field === "stdout" ? req.maxStdoutBytes! : req.maxStderrBytes!;
        const used = field === "stdout" ? stdoutBytes : stderrBytes;
        const keep = text.slice(0, Math.max(0, limit - used));
        if (field === "stdout") { stdoutBytes += keep.length; stdout += keep; } else { stderrBytes += keep.length; stderr += keep; }
        if (keep.length < text.length) killForLimit(field);
      };

      child.stdout!.on("data", (buf: Buffer) => handleData(buf, "stdout"));
      child.stderr!.on("data", (buf: Buffer) => handleData(buf, "stderr"));

      const onAbort = () => {
        try { process.kill(-child.pid!, "SIGKILL"); } catch { /* 已退出 */ }
      };
      signal?.addEventListener("abort", onAbort, { once: true });

      const timer = setTimeout(() => {
        timedOut = true;
        try { process.kill(-child.pid!, "SIGKILL"); } catch { /* 已退出 */ }
      }, req.timeoutMs);
      timer.unref();

      child.on("error", (error) => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        stdout += decOut.end() ?? "";
        stderr += decErr.end() ?? "";
        stderr += `${stderr ? "\n" : ""}spawn error: ${error.message}`;
        reject(new ExecutionClientError(EXECUTION_WIRE.errorCodes.backendUnavailable, `spawn failed: ${error.message}`));
      });

      child.on("close", (code, signalName) => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        stdout += decOut.end() ?? "";
        stderr += decErr.end() ?? "";
        if (truncated) {
          truncated.originalLen = fieldLen(truncated.field, stdoutBytes, stderrBytes);
          truncated.keptLen = truncated.field === "stdout" ? stdout.length : stderr.length;
        }
        resolve({
          stdout,
          stderr,
          exitCode: code ?? (signalName ? 128 + signalNumber(signalName) : null),
          signal: signalName,
          timedOut,
          ...(truncated ? { truncated } : {}),
        });
      });
    });
  }
}

function fieldLen(field: "stdout" | "stderr", stdoutBytes: number, stderrBytes: number): number {
  return field === "stdout" ? stdoutBytes : stderrBytes;
}

function signalNumber(signalName: string): number {
  return (os.constants.signals as Record<string, number>)[signalName] ?? 1;
}
