/**
 * execution/types.ts —— 执行面统一协议类型。
 *
 * execution/v1（P0）：sync + SSE stream；
 * execution/v1.1（P0.1）：模式框架 + interactive WS + persistent wire 规范（实现后置）。
 *
 * 三仓同源契约（pi-triple-deps / pth / ptl）：
 *  - SandboxBackend（pth-sandbox，sandbox-untrusted，保持 v1）
 *  - LocalBackend（ptl，host）
 *  - DockerExecBackend（ptl，dev-container → 工具容器）
 *  - ExecutionHttpServer（v1.1 服务端唯一实现）
 */

export type ExecutionProfile = "host" | "dev-container" | "sandbox-untrusted";

export type ExecutionProtocolVersion = "execution/v1" | "execution/v1.1";

/** v1.1 调用模式。v1 请求不声明 mode：stream:true 隐式映射 stream，否则 sync。 */
export type InvocationMode = "sync" | "stream" | "interactive" | "persistent";

export interface ExecutionModes {
  sync: boolean;
  stream: boolean;
  interactive: boolean;
  persistent: boolean;
}

export interface ExecutionPathMapping {
  /** 宿主侧根路径 */
  hostRoot: string;
  /** 执行端（容器内）根路径 */
  execRoot: string;
}

/** interactive 的 pty 请求；后端不支持 pty 时按自身能力降级或拒绝 */
export interface ExecutionPtyRequest {
  /** 终端列数（1..1000；默认 80） */
  cols?: number;
  /** 终端行数（1..1000；默认 24） */
  rows?: number;
  /** TERM（如 xterm-256color；默认由后端决定） */
  term?: string;
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
  /**
   * v1 遗留字段：true → 异步执行，经流式接口消费。
   * v1.1 起优先使用 mode；mode 缺省时 stream:true 映射为 mode:"stream"。
   */
  stream?: boolean;
  /**
   * v1.1 调用模式：sync | stream | interactive | persistent。
   * 未声明模式时按 v1 语义解析（stream:true → stream，否则 sync）。
   */
  mode?: InvocationMode;
  /** interactive 专用：pty 请求 */
  pty?: ExecutionPtyRequest;
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
  /** stream/interactive 完成态附带 */
  execId?: string;
}

export interface ExecutionCapabilities {
  /**
   * wire 协议版本。v1.0 客户端必须对 v1.1 fail-closed；
   * v1.1 客户端可同时消费 v1 与 v1.1。
   */
  version: ExecutionProtocolVersion;
  streaming: boolean;
  cancel: boolean;
  cwdWhitelist: boolean;
  uidIsolation: boolean;
  egressLocked: boolean;
  pathMapping: boolean;
  /**
   * v1.1 模式位图（v1 capabilities 省略；v1 的有效位图按 streaming 推导）。
   * 请求声明的 mode 不在位图内 → MODE_NOT_SUPPORTED。
   */
  modes?: ExecutionModes;
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
  signal?: string | null;
}

export interface ExecutionErrorEvent {
  code: string;
  message: string;
}

export interface ExecutionError {
  error: { code: string; message: string };
}

export interface ExecutionBackend {
  /** backend 标识（如 "sandbox" / "local" / "docker-exec"） */
  readonly id: string;
  /** 能力声明（HTTP client 每次探测；本地 backend 静态返回） */
  getCapabilities(): Promise<ExecutionCapabilities>;
  /** 同步执行；请求含 stream:true 时 backend 可返回未完成 job 状态或按能力拒绝 */
  execute(request: ExecutionRequest, signal?: AbortSignal): Promise<ExecutionResult>;
}

/* ── v1.1 任务级接口（ExecutionHttpServer 的驱动面） ───────────────── */

export interface ExecutionJobOutput {
  stream: "stdout" | "stderr";
  data: string;
}

export interface ExecutionJobHandlers {
  onOutput?: (event: ExecutionOutputEvent) => void;
  onDone?: (result: ExecutionResult) => void;
  onError?: (event: ExecutionErrorEvent) => void;
}

/**
 * 一个已启动的 stream/interactive 任务句柄。
 * 实现约定：
 *  - startJob resolve 前进程必须已 spawn（早于订阅产生的输出由实现缓冲，
 *    经 outputSnapshot() 补发）；
 *  - done 后 cancel 为 no-op；subscribe 可多次订阅。
 */
export interface ExecutionJob {
  readonly execId: string;
  readonly status: "running" | "done";
  /** interactive：向 stdin 写数据；无此能力 → interactive 不可用 */
  writeStdin?(data: string): void;
  /** interactive：pty resize；后端不支持时 WS 层回 error 帧（连接保持） */
  resize?(cols: number, rows: number): void;
  /** 订阅输出/完成；返回取消订阅函数 */
  subscribe(handlers: ExecutionJobHandlers): () => void;
  /** 自启动以来全部输出快照（SSE/WS 新订阅补发用） */
  outputSnapshot(): ExecutionJobOutput[];
  /** done 后的 ExecutionResult；未完成 → undefined */
  getResult(): ExecutionResult | undefined;
  /** 尽力取消（进程组强杀）；幂等 */
  cancel(): Promise<boolean> | boolean;
}

export interface ExecutionJobBackend extends ExecutionBackend {
  /** 启动 stream/interactive 任务 */
  startJob(request: ExecutionRequest, signal?: AbortSignal): Promise<ExecutionJob>;
}

/* ── interactive WS 消息帧（JSON text frame） ─────────────────────── */

export interface ExecutionWsStdinFrame {
  type: "stdin";
  data: string;
}

export interface ExecutionWsResizeFrame {
  type: "resize";
  cols: number;
  rows: number;
}

export type ExecutionWsClientFrame = ExecutionWsStdinFrame | ExecutionWsResizeFrame;

export interface ExecutionWsOutputFrame {
  type: "stdout" | "stderr";
  data: string;
}

export interface ExecutionWsDoneFrame {
  type: "done";
  exitCode: number | null;
  signal?: string | null;
  timedOut: boolean;
}

export interface ExecutionWsErrorFrame {
  type: "error";
  code: string;
  message: string;
}

export type ExecutionWsServerFrame = ExecutionWsOutputFrame | ExecutionWsDoneFrame | ExecutionWsErrorFrame;

export interface ExecutionInteractiveHandlers {
  onOutput?: (event: ExecutionOutputEvent) => void;
  onDone?: (event: ExecutionDoneEvent) => void;
}

export interface ExecutionInteractiveSession {
  readonly execId: string;
  /** 向 stdin 写数据（WS 已打开；关闭后写抛 ExecutionClientError） */
  writeStdin(data: string): void;
  /** pty resize（后端支持时生效） */
  resize(cols: number, rows: number): void;
  /** 关闭 WS（不取消任务；取消走 cancel(execId)） */
  close(): void;
  /** done/error/连接关闭时 settle */
  readonly done: Promise<ExecutionDoneEvent>;
}

/* ── persistent 模式 wire 规范（v1.1 定稿；实现后置） ─────────────── */

export type ExecutionSessionState = "active" | "released" | "expired";

export interface ExecutionSessionCreateRequest {
  /** 租约 ms（5_000..86_400_000；缺省 600_000）；每次 execute 自动续租 */
  leaseMs?: number;
}

export interface ExecutionSessionCreateResponse {
  sessionId: string;
  status: "active";
  createdAt: number;
  expiresAt: number;
  leaseMs: number;
}

export interface ExecutionSession {
  sessionId: string;
  status: ExecutionSessionState;
  createdAt: number;
  expiresAt: number;
  leaseMs: number;
  /** 最近一次 execute 的 exitCode 与完成时间 */
  lastResult?: { exitCode: number | null; completedAt: number };
  snapshotCount: number;
}

export interface ExecutionSessionExecuteRequest {
  cmd: string | string[];
  cwd?: string;
  env?: Record<string, string>;
  timeoutMs?: number;
  maxStdoutBytes?: number;
  maxStderrBytes?: number;
}

export interface ExecutionSessionExecuteResult extends ExecutionResult {
  sessionId: string;
}

export interface ExecutionSessionSnapshotRequest {
  /** 可选标签（≤128 字符，非空） */
  tag?: string;
}

export interface ExecutionSessionSnapshot {
  sessionId: string;
  snapshotId: string;
  tag?: string;
  createdAt: number;
}

export interface ExecutionSessionResetRequest {
  /** 缺省回滚到会话初始状态；提供时回滚到指定快照 */
  snapshotId?: string;
}

/* ── engine 侧 backend 注册描述（P0 协议面冻结） ───────────────────── */

/**
 * 一个 descriptor = 一个外部执行面的接入身份；engine 是唯一客户端。
 */
export interface ExecutionBackendDescriptor {
  /** engine 内唯一后端名，如 "sandbox" / "local-lean"（^[a-z][a-z0-9._-]{0,63}$） */
  id: string;
  /** 执行面 baseUrl（http/https，不带尾斜杠） */
  url: string;
  /** 期望信任档；engine 只能按此档发请求，执行面必须接受/再校验 */
  profile: ExecutionProfile;
  /** 认证 token 所在的环境变量名（值不落配置） */
  tokenEnv?: string;
  /** 可选：engine 路径 → 执行面路径的默认映射；请求自带 mapping 优先 */
  pathMapping?: ExecutionPathMapping;
  /** true → 该 backend 不可用时 engine 拒绝启动（strict 模式） */
  required?: boolean;
}

export interface ExecutionStreamHandlers {
  onOutput?: (event: ExecutionOutputEvent) => void;
  onError?: (event: ExecutionErrorEvent) => void;
  onDone: (event: ExecutionDoneEvent) => void;
}
