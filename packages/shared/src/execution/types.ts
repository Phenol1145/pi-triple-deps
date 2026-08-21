/**
 * execution/types.ts —— 执行面统一协议类型（execution/v1）。
 *
 * 三仓同源契约（pi-triple-deps / pth / ptl）：
 *  - SandboxBackend（pth-sandbox，sandbox-untrusted）
 *  - LocalBackend（ptl，host）
 *  - DockerExecBackend（ptl，dev-container）
 */

export type ExecutionProfile = "host" | "dev-container" | "sandbox-untrusted";

export interface ExecutionPathMapping {
  /** 宿主侧根路径 */
  hostRoot: string;
  /** 执行端（容器内）根路径 */
  execRoot: string;
}

export interface ExecutionRequest {
  /** shell 命令字符串 或 argv 数组（数组不经 shell） */
  cmd: string | string[];
  /** 执行目录；profile 决定校验策略 */
  cwd?: string;
  /** env 增量；sandbox-untrusted 拒绝敏感键（LLM 密钥等） */
  env?: Record<string, string>;
  /** 超时 ms（>0）；到达后终止整个进程组 */
  timeoutMs?: number;
  /** stdout 字节上限（1..4MB）；超限终止进程组并回 truncated */
  maxStdoutBytes?: number;
  /** stderr 字节上限（同上） */
  maxStderrBytes?: number;
  /** true → 返回 execId 异步执行，经流式接口消费 */
  stream?: boolean;
  /** 宿主路径 ↔ 执行端路径映射（一等字段；backend 能力声明 pathMapping 时可用） */
  pathMapping?: ExecutionPathMapping;
  /** 信任档；客户端不得自我提升——backend 按自身实现校验/降级 */
  profile?: ExecutionProfile;
}

export interface ExecutionTruncation {
  field: "stdout" | "stderr";
  originalLen: number;
  keptLen: number;
}

export interface ExecutionResult {
  stdout: string;
  stderr: string;
  exitCode: number | null;
  /** 被信号终止时的信号名（如 SIGKILL） */
  signal?: string | null;
  /** 超时强杀标记 */
  timedOut: boolean;
  /** 字节上限截断标记 */
  truncated?: ExecutionTruncation;
  /** stream 完成态附带 */
  execId?: string;
}

export interface ExecutionCapabilities {
  /** wire 协议版本（breaking 升级走 v2） */
  version: "execution/v1";
  streaming: boolean;
  cancel: boolean;
  cwdWhitelist: boolean;
  uidIsolation: boolean;
  egressLocked: boolean;
  pathMapping: boolean;
}

export interface ExecutionJobState {
  execId: string;
  status: "running" | "done";
  result?: ExecutionResult;
}

export interface ExecutionOutputEvent {
  stream: "stdout" | "stderr";
  data: string;
}

export interface ExecutionDoneEvent {
  exitCode: number | null;
  timedOut: boolean;
}

export interface ExecutionError {
  error: { code: string; message: string };
}
