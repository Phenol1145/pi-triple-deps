/**
 * execution/docker-exec-backend.ts —— execution/v1 DockerExecBackend（dev-container）。
 *
 * 信任模型：dev-container（可信开发容器）。用 `docker compose exec -T <service>` 执行；
 * 宿主路径经 pathMapping 翻译到容器路径。docker exec 无法可靠终止容器内进程组——
 * cancel/streaming 能力声明 false（符合 §7 的“不假装支持”原则）。
 */

import { execFile } from "node:child_process";
import type {
  ExecutionBackend,
  ExecutionCapabilities,
  ExecutionRequest,
  ExecutionResult,
} from "../types.js";
import { EXECUTION_WIRE } from "../wire.js";
import { resolveExecutionMode, validateExecutionRequest } from "../validate.js";
import { ExecutionClientError } from "../client.js";

export interface DockerExecBackendOptions {
  /** 直接 docker exec 目标容器（如 v13-asm-toolchain）；与 compose 模式二选一 */
  containerName?: string;
  /** compose 模式：compose 文件/项目/服务 */
  composeFile?: string;
  projectName?: string;
  service?: string;
  /** 宿主根 → 容器根路径翻译（必需；否则 cwd 原样透传） */
  pathMapping?: { hostRoot: string; execRoot: string };
  defaultTimeoutMs?: number;
  maxStdoutBytes?: number;
  maxStderrBytes?: number;
  /** 可注入 runner（测试）；默认 execFile("docker", args) */
  run?: (cmd: string, args: string[]) => Promise<{ code: number | null; stdout: string; stderr: string; timedOut?: boolean }>;
}

const CAPABILITIES: ExecutionCapabilities = {
  version: EXECUTION_WIRE.version,
  streaming: false,
  cancel: false,
  cwdWhitelist: false,
  uidIsolation: false,
  egressLocked: false,
  pathMapping: true,
};

export class DockerExecBackend implements ExecutionBackend {
  readonly id = "docker-exec";
  private readonly opts: Required<Pick<DockerExecBackendOptions, "service" | "projectName" | "composeFile" | "defaultTimeoutMs" | "maxStdoutBytes" | "maxStderrBytes">> & Pick<DockerExecBackendOptions, "containerName" | "pathMapping" | "run">;

  constructor(options: DockerExecBackendOptions = {}) {
    this.opts = {
      containerName: options.containerName,
      composeFile: options.composeFile ?? "docker-compose.yaml",
      projectName: options.projectName ?? "pi-triple-ptl",
      service: options.service ?? "dev",
      defaultTimeoutMs: options.defaultTimeoutMs ?? 30_000,
      maxStdoutBytes: options.maxStdoutBytes ?? 1024 * 1024,
      maxStderrBytes: options.maxStderrBytes ?? 1024 * 1024,
      pathMapping: options.pathMapping,
      run: options.run,
    };
  }

  async getCapabilities(): Promise<ExecutionCapabilities> {
    return CAPABILITIES;
  }

  async execute(input: ExecutionRequest): Promise<ExecutionResult> {
    const req = validateExecutionRequest(input, {
      timeoutMs: this.opts.defaultTimeoutMs,
      maxStdoutBytes: this.opts.maxStdoutBytes,
      maxStderrBytes: this.opts.maxStderrBytes,
    });
    const mode = resolveExecutionMode(req);
    if (mode !== "sync") {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.modeNotSupported, `docker-exec backend does not support mode=${mode}`);
    }
    if (req.profile !== undefined && req.profile !== "dev-container") {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, "docker-exec backend only accepts profile=dev-container");
    }

    const cwd = this.translateCwd(req.cwd, req.pathMapping ?? this.opts.pathMapping);
    const argv = Array.isArray(req.cmd) ? req.cmd : ["bash", "-lc", req.cmd];
    const envArgs = Object.entries(req.env ?? {}).flatMap(([key, value]) => ["-e", `${key}=${value}`]);
    const args = this.opts.containerName
      ? ["exec", ...envArgs, ...(cwd ? ["-w", cwd] : []), this.opts.containerName, ...argv]
      : [
          "compose", "-f", this.opts.composeFile,
          "--project-name", this.opts.projectName,
          "exec", "-T",
          ...envArgs,
          ...(cwd ? ["-w", cwd] : []),
          this.opts.service,
          ...argv,
        ];

    const run = this.opts.run ?? defaultRun(req.timeoutMs!);
    const r = await run("docker", args);
    const truncated = truncate(r, req.maxStdoutBytes!, req.maxStderrBytes!);
    return {
      stdout: truncated.stdout,
      stderr: truncated.stderr,
      exitCode: r.code,
      timedOut: r.timedOut === true,
      ...(truncated.truncated ? { truncated: truncated.truncated } : {}),
    };
  }

  private translateCwd(cwd: string | undefined, mapping: DockerExecBackendOptions["pathMapping"]): string | undefined {
    if (!cwd) return undefined;
    if (!mapping) return cwd;
    if (!cwd.startsWith(mapping.hostRoot)) {
      throw new ExecutionClientError(EXECUTION_WIRE.errorCodes.invalidRequest, `cwd outside pathMapping hostRoot: ${cwd}`);
    }
    return mapping.execRoot + cwd.slice(mapping.hostRoot.length);
  }
}

function defaultRun(timeoutMs: number) {
  return (cmd: string, args: string[]): Promise<{ code: number | null; stdout: string; stderr: string; timedOut?: boolean }> =>
    new Promise((resolve, reject) => {
      let forcedTimedOut = false;
      let child: ReturnType<typeof execFile>;
      const timer = setTimeout(() => {
        forcedTimedOut = true;
        child?.kill("SIGKILL");
      }, timeoutMs);
      timer.unref?.();
      child = execFile(cmd, args, { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 }, (error, stdout, stderr) => {
        clearTimeout(timer);
        // docker exec 的 CLI 被 SIGKILL 后可能以 0 退出——超时必须以自身计时为准
        if (forcedTimedOut) {
          resolve({ code: null, stdout: stdout ?? "", stderr: stderr ?? "", timedOut: true });
          return;
        }
        if (error) {
          const errno = error as NodeJS.ErrnoException & { killed?: boolean };
          if (typeof errno.code === "number") {
            resolve({ code: errno.code, stdout: stdout ?? "", stderr: stderr ?? "" });
            return;
          }
          reject(error);
          return;
        }
        resolve({ code: 0, stdout: stdout ?? "", stderr: stderr ?? "" });
      });
    });
}

function truncate(
  r: { stdout: string; stderr: string },
  maxStdoutBytes: number,
  maxStderrBytes: number,
): { stdout: string; stderr: string; truncated?: ExecutionResult["truncated"] } {
  if (r.stdout.length <= maxStdoutBytes && r.stderr.length <= maxStderrBytes) {
    return r;
  }
  if (r.stdout.length > maxStdoutBytes) {
    return {
      stdout: r.stdout.slice(0, maxStdoutBytes),
      stderr: r.stderr,
      truncated: { field: "stdout", originalLen: r.stdout.length, keptLen: maxStdoutBytes },
    };
  }
  return {
    stdout: r.stdout,
    stderr: r.stderr.slice(0, maxStderrBytes),
    truncated: { field: "stderr", originalLen: r.stderr.length, keptLen: maxStderrBytes },
  };
}
