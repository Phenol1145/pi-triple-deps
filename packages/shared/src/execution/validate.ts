/**
 * execution/validate.ts —— execution/v1 + v1.1 请求/能力校验（backend 无关的纯函数）。
 *
 * sandbox 侧的 cwd 白名单/私有工作区校验仍由 pth-sandbox 按 profile 做；
 * 这里只做跨 backend 共有的结构与数值校验。
 */

import type {
  ExecutionBackendDescriptor,
  ExecutionCapabilities,
  ExecutionModes,
  ExecutionPathMapping,
  ExecutionProfile,
  ExecutionRequest,
  ExecutionSessionCreateRequest,
  ExecutionSessionExecuteRequest,
  ExecutionSessionResetRequest,
  ExecutionSessionSnapshotRequest,
  InvocationMode,
} from "./types.js";
import { EXECUTION_INVOCATION_MODES, EXECUTION_WIRE } from "./wire.js";

export const EXECUTION_LIMITS = {
  maxTimeoutMs: 24 * 60 * 60 * 1000,
  maxOutputBytes: 4 * 1024 * 1024,
  maxPtyCols: 1000,
  maxPtyRows: 1000,
  maxTermLength: 64,
} as const;

/** persistent 模式 wire 规范（v1.1 定稿；实现后置） */
export const EXECUTION_SESSION_LIMITS = {
  minLeaseMs: 5_000,
  maxLeaseMs: 24 * 60 * 60 * 1000,
  defaultLeaseMs: 10 * 60 * 1000,
  maxTagLength: 128,
  maxSessionIdLength: 128,
} as const;

export class ExecutionRequestError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "ExecutionRequestError";
    this.code = code;
  }
}

/** backend descriptor 配置错误（engine 侧 fail-closed；非 wire 错误） */
export class ExecutionBackendDescriptorError extends Error {
  readonly code = EXECUTION_WIRE.errorCodes.invalidRequest;
  constructor(message: string) {
    super(message);
    this.name = "ExecutionBackendDescriptorError";
  }
}

function bad(message: string): never {
  throw new ExecutionRequestError(EXECUTION_WIRE.errorCodes.invalidRequest, message);
}

export function isExecutionProfile(value: unknown): value is ExecutionProfile {
  return value === "host" || value === "dev-container" || value === "sandbox-untrusted";
}

const MODE_SET = new Set<string>(EXECUTION_INVOCATION_MODES);

export function isInvocationMode(value: unknown): value is InvocationMode {
  return typeof value === "string" && MODE_SET.has(value);
}

export function isExecutionProtocolVersion(value: unknown): value is "execution/v1" | "execution/v1.1" {
  return value === EXECUTION_WIRE.versions.v1 || value === EXECUTION_WIRE.versions.v1_1;
}

/**
 * 解析请求的有效调用模式（v1 → v1.1 兼容映射）：
 *  - mode 显式声明时以 mode 为准；
 *  - 未声明时，legacy stream:true → "stream"，否则 → "sync"。
 * 注意：本函数不向请求对象注入 mode，以保持 v1 wire 字节形状兼容。
 */
export function resolveExecutionMode(request: Pick<ExecutionRequest, "mode" | "stream">): InvocationMode {
  if (request.mode !== undefined) return request.mode;
  return request.stream === true ? "stream" : "sync";
}

/**
 * 解析 backend 的有效模式位图：
 *  - v1 capabilities：sync 恒可用，stream 由 streaming 推导，interactive/persistent false；
 *  - v1.1 capabilities：直接采用 modes；缺失时全部 false（fail-closed）。
 */
export function resolveExecutionModes(capabilities: ExecutionCapabilities): ExecutionModes {
  if (capabilities.version === EXECUTION_WIRE.versions.v1) {
    return {
      sync: true,
      stream: capabilities.streaming === true,
      interactive: false,
      persistent: false,
    };
  }
  return capabilities.modes ?? { sync: false, stream: false, interactive: false, persistent: false };
}

function validateModeAndLegacyStream(req: Record<string, unknown>, normalized: ExecutionRequest): void {
  if (req.mode !== undefined) {
    if (!isInvocationMode(req.mode)) {
      bad(`mode must be one of ${EXECUTION_INVOCATION_MODES.join("|")}`);
    }
    normalized.mode = req.mode;
  }
  if (req.mode !== undefined && req.stream !== undefined) {
    if (req.mode === "stream" && req.stream !== true) {
      bad("mode=stream conflicts with stream:false; omit stream field");
    }
    if (req.mode !== "stream" && req.stream === true) {
      bad(`legacy stream:true conflicts with mode=${req.mode}; omit stream field`);
    }
  }
}

function validatePty(req: Record<string, unknown>, normalized: ExecutionRequest): void {
  if (req.pty === undefined) return;
  if (req.mode !== "interactive") bad("pty is only allowed with mode=interactive");
  const raw = req.pty;
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    bad("pty must be an object");
  }
  const pty = raw as Record<string, unknown>;
  for (const key of Object.keys(pty)) {
    if (key !== "cols" && key !== "rows" && key !== "term") bad(`unknown pty field: ${key}`);
  }
  const size = (value: unknown, field: "cols" | "rows") => {
    if (value === undefined) return undefined;
    if (!Number.isInteger(value) || (value as number) < 1 || (value as number) > EXECUTION_LIMITS.maxPtyCols) {
      bad(`${field} must be an integer in 1..${field === "cols" ? EXECUTION_LIMITS.maxPtyCols : EXECUTION_LIMITS.maxPtyRows}`);
    }
    return value as number;
  };
  const cols = size(pty.cols, "cols");
  const rows = size(pty.rows, "rows");
  let term: string | undefined;
  if (pty.term !== undefined) {
    if (
      typeof pty.term !== "string" ||
      pty.term.length === 0 ||
      pty.term.length > EXECUTION_LIMITS.maxTermLength ||
      /[\u0000-\u001f\u007f]/.test(pty.term)
    ) {
      bad(`term must be a 1..${EXECUTION_LIMITS.maxTermLength} character string without control characters`);
    }
    term = pty.term;
  }
  normalized.pty = { ...(cols !== undefined ? { cols } : {}), ...(rows !== undefined ? { rows } : {}), ...(term !== undefined ? { term } : {}) };
}

/**
 * 结构化校验；返回规范化后的请求（默认值展开）。
 * profile 只做结构校验——信任档的强制由 backend 决定，客户端传入仅供声明。
 * v1.1 新增 mode/pty；旧 payload（无 mode）保持原样兼容 v1。
 */
export function validateExecutionRequest(input: unknown, defaults: {
  timeoutMs?: number;
  maxStdoutBytes?: number;
  maxStderrBytes?: number;
} = {}): ExecutionRequest {
  if (!input || typeof input !== "object") bad("request body required");
  const req = input as Record<string, unknown>;

  const cmd = req.cmd;
  const cmdOk =
    (typeof cmd === "string" && cmd.length > 0) ||
    (Array.isArray(cmd) && cmd.length > 0 && cmd.every((c) => typeof c === "string"));
  if (!cmdOk) bad("cmd must be a non-empty string or an array of strings");

  let timeoutMs = defaults.timeoutMs ?? 30_000;
  if (req.timeoutMs !== undefined) {
    if (typeof req.timeoutMs !== "number" || !Number.isFinite(req.timeoutMs) || req.timeoutMs <= 0) {
      bad("timeoutMs must be a positive number (ms)");
    }
    timeoutMs = Math.min(req.timeoutMs, EXECUTION_LIMITS.maxTimeoutMs);
  }

  const outputMax = (value: unknown, field: "maxStdoutBytes" | "maxStderrBytes", fallback: number): number => {
    if (value === undefined) return fallback;
    if (
      typeof value !== "number" ||
      !Number.isFinite(value) ||
      value <= 0 ||
      value > EXECUTION_LIMITS.maxOutputBytes
    ) {
      bad(`${field} must be 1..${EXECUTION_LIMITS.maxOutputBytes}`);
    }
    return Math.floor(value);
  };

  const normalized: ExecutionRequest = {
    cmd: cmd as string | string[],
    timeoutMs,
    maxStdoutBytes: outputMax(req.maxStdoutBytes, "maxStdoutBytes", defaults.maxStdoutBytes ?? 1024 * 1024),
    maxStderrBytes: outputMax(req.maxStderrBytes, "maxStderrBytes", defaults.maxStderrBytes ?? 1024 * 1024),
  };

  if (req.cwd !== undefined) {
    if (typeof req.cwd !== "string" || req.cwd.length === 0) bad("cwd must be a non-empty string");
    normalized.cwd = req.cwd;
  }

  if (req.env !== undefined) {
    if (
      typeof req.env !== "object" ||
      req.env === null ||
      Array.isArray(req.env) ||
      Object.values(req.env).some((v) => typeof v !== "string")
    ) {
      bad("env must be an object of string values");
    }
    normalized.env = { ...(req.env as Record<string, string>) };
  }

  if (req.stream !== undefined) {
    if (typeof req.stream !== "boolean") bad("stream must be a boolean");
    normalized.stream = req.stream;
  }

  validateModeAndLegacyStream(req, normalized);
  validatePty(req, normalized);

  if (req.pathMapping !== undefined) {
    const pm = req.pathMapping as Record<string, unknown>;
    if (
      typeof pm !== "object" ||
      pm === null ||
      typeof pm.hostRoot !== "string" ||
      pm.hostRoot.length === 0 ||
      typeof pm.execRoot !== "string" ||
      pm.execRoot.length === 0
    ) {
      bad("pathMapping must be { hostRoot, execRoot } non-empty strings");
    }
    normalized.pathMapping = { hostRoot: pm.hostRoot, execRoot: pm.execRoot };
  }

  if (req.profile !== undefined) {
    if (!isExecutionProfile(req.profile)) bad(`profile must be one of host|dev-container|sandbox-untrusted`);
    normalized.profile = req.profile;
  }

  return normalized;
}

/**
 * capabilities 结构化校验（v1 / v1.1）。
 * v1.1 必须带完整 modes 位图；v1 省略 modes（提供时同样校验）。
 * 未知顶层字段允许（能力声明可加性演进）；mode 位图未知键忽略。
 */
export function validateExecutionCapabilities(input: unknown): ExecutionCapabilities {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    bad("capabilities must be an object");
  }
  const caps = input as Record<string, unknown>;
  const booleanFields = ["streaming", "cancel", "cwdWhitelist", "uidIsolation", "egressLocked", "pathMapping"] as const;
  for (const field of booleanFields) {
    if (typeof caps[field] !== "boolean") bad(`capabilities.${field} must be a boolean`);
  }
  if (!isExecutionProtocolVersion(caps.version)) {
    bad("capabilities.version must be execution/v1 or execution/v1.1");
  }

  const normalized: ExecutionCapabilities = {
    version: caps.version,
    streaming: caps.streaming as boolean,
    cancel: caps.cancel as boolean,
    cwdWhitelist: caps.cwdWhitelist as boolean,
    uidIsolation: caps.uidIsolation as boolean,
    egressLocked: caps.egressLocked as boolean,
    pathMapping: caps.pathMapping as boolean,
  };

  if (caps.modes !== undefined || caps.version === EXECUTION_WIRE.versions.v1_1) {
    const modes = caps.modes as Record<string, unknown> | undefined;
    if (typeof modes !== "object" || modes === null || Array.isArray(modes)) {
      bad("capabilities.modes must be { sync, stream, interactive, persistent } booleans");
    }
    const normalizedModes = {} as Record<InvocationMode, boolean>;
    for (const mode of EXECUTION_INVOCATION_MODES) {
      if (typeof modes[mode] !== "boolean") bad(`capabilities.modes.${mode} must be a boolean`);
      normalizedModes[mode] = modes[mode] as boolean;
    }
    normalized.modes = normalizedModes;
  }

  return normalized;
}

/* ── persistent 模式请求校验（wire 规范定稿；服务端实现后置） ─────── */

function allowedKeys(value: Record<string, unknown>, allowed: readonly string[], label: string): void {
  for (const key of Object.keys(value)) {
    if (!allowed.includes(key)) bad(`unknown ${label} field: ${key}`);
  }
}

function positiveFinite(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    bad(`${field} must be a positive number`);
  }
  return value;
}

export function validateExecutionSessionCreateRequest(input: unknown): ExecutionSessionCreateRequest {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    bad("session create request must be an object");
  }
  const req = input as Record<string, unknown>;
  allowedKeys(req, ["leaseMs"], "session create request");
  if (req.leaseMs === undefined) return {};
  const leaseMs = positiveFinite(req.leaseMs, "leaseMs");
  if (leaseMs < EXECUTION_SESSION_LIMITS.minLeaseMs || leaseMs > EXECUTION_SESSION_LIMITS.maxLeaseMs) {
    bad(`leaseMs must be in ${EXECUTION_SESSION_LIMITS.minLeaseMs}..${EXECUTION_SESSION_LIMITS.maxLeaseMs}`);
  }
  return { leaseMs };
}

export function validateExecutionSessionExecuteRequest(input: unknown, defaults: {
  timeoutMs?: number;
  maxStdoutBytes?: number;
  maxStderrBytes?: number;
} = {}): ExecutionSessionExecuteRequest {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    bad("session execute request must be an object");
  }
  const req = input as Record<string, unknown>;
  allowedKeys(req, ["cmd", "cwd", "env", "timeoutMs", "maxStdoutBytes", "maxStderrBytes"], "session execute request");
  const normalized = validateExecutionRequest(
    {
      cmd: req.cmd,
      ...(req.cwd !== undefined ? { cwd: req.cwd } : {}),
      ...(req.env !== undefined ? { env: req.env } : {}),
      ...(req.timeoutMs !== undefined ? { timeoutMs: req.timeoutMs } : {}),
      ...(req.maxStdoutBytes !== undefined ? { maxStdoutBytes: req.maxStdoutBytes } : {}),
      ...(req.maxStderrBytes !== undefined ? { maxStderrBytes: req.maxStderrBytes } : {}),
    },
    defaults,
  );
  const out: ExecutionSessionExecuteRequest = {
    cmd: normalized.cmd,
    timeoutMs: normalized.timeoutMs,
    maxStdoutBytes: normalized.maxStdoutBytes,
    maxStderrBytes: normalized.maxStderrBytes,
  };
  if (normalized.cwd !== undefined) out.cwd = normalized.cwd;
  if (normalized.env !== undefined) out.env = normalized.env;
  return out;
}

export function validateExecutionSessionSnapshotRequest(input: unknown): ExecutionSessionSnapshotRequest {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    bad("session snapshot request must be an object");
  }
  const req = input as Record<string, unknown>;
  allowedKeys(req, ["tag"], "session snapshot request");
  if (req.tag === undefined) return {};
  if (
    typeof req.tag !== "string" ||
    req.tag.length === 0 ||
    req.tag.length > EXECUTION_SESSION_LIMITS.maxTagLength
  ) {
    bad(`tag must be a 1..${EXECUTION_SESSION_LIMITS.maxTagLength} character string`);
  }
  return { tag: req.tag };
}

export function validateExecutionSessionResetRequest(input: unknown): ExecutionSessionResetRequest {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    bad("session reset request must be an object");
  }
  const req = input as Record<string, unknown>;
  allowedKeys(req, ["snapshotId"], "session reset request");
  if (req.snapshotId === undefined) return {};
  if (
    typeof req.snapshotId !== "string" ||
    req.snapshotId.length === 0 ||
    req.snapshotId.length > EXECUTION_SESSION_LIMITS.maxSessionIdLength
  ) {
    bad(`snapshotId must be a 1..${EXECUTION_SESSION_LIMITS.maxSessionIdLength} character string`);
  }
  return { snapshotId: req.snapshotId };
}

/* ── backend descriptor（P0 协议面冻结） ───────────────────────────── */

function badDescriptor(message: string): never {
  throw new ExecutionBackendDescriptorError(message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validatePathMappingShape(value: unknown): ExecutionPathMapping | undefined {
  if (value === undefined) return undefined;
  if (
    !isRecord(value) ||
    typeof value.hostRoot !== "string" ||
    value.hostRoot.length === 0 ||
    typeof value.execRoot !== "string" ||
    value.execRoot.length === 0
  ) {
    badDescriptor("pathMapping must be { hostRoot, execRoot } non-empty strings");
  }
  return { hostRoot: value.hostRoot, execRoot: value.execRoot };
}

const BACKEND_ID_PATTERN = /^[a-z][a-z0-9._-]{0,63}$/;
const ENV_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]{0,127}$/;
const DESCRIPTOR_KEYS = new Set(["id", "url", "profile", "tokenEnv", "pathMapping", "required"]);

/**
 * engine 侧 backend 注册描述的结构校验（P0 协议面冻结）。
 * 返回规范化 descriptor（url 去尾斜杠）；未知字段 fail-closed。
 */
export function validateExecutionBackendDescriptor(input: unknown): ExecutionBackendDescriptor {
  if (!isRecord(input)) badDescriptor("backend descriptor must be an object");
  for (const key of Object.keys(input)) {
    if (!DESCRIPTOR_KEYS.has(key)) badDescriptor(`unknown backend descriptor field: ${key}`);
  }

  const id = input.id;
  if (typeof id !== "string" || !BACKEND_ID_PATTERN.test(id)) {
    badDescriptor("id must match ^[a-z][a-z0-9._-]{0,63}$");
  }

  const rawUrl = input.url;
  if (typeof rawUrl !== "string" || rawUrl.length === 0) badDescriptor("url must be a non-empty string");
  const url = rawUrl.replace(/\/+$/, "");
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    badDescriptor("url must be a valid absolute URL");
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    badDescriptor("url protocol must be http or https");
  }
  if (parsed.search !== "" || parsed.hash !== "") badDescriptor("url must not contain query or fragment");

  const profile = input.profile;
  if (!isExecutionProfile(profile)) {
    badDescriptor("profile must be one of host|dev-container|sandbox-untrusted");
  }

  const tokenEnv = input.tokenEnv;
  if (tokenEnv !== undefined) {
    if (typeof tokenEnv !== "string" || !ENV_NAME_PATTERN.test(tokenEnv)) {
      badDescriptor("tokenEnv must be an environment variable name");
    }
  }

  const required = input.required;
  if (required !== undefined && typeof required !== "boolean") {
    badDescriptor("required must be a boolean");
  }

  const descriptor: ExecutionBackendDescriptor = {
    id,
    url,
    profile,
  };
  if (tokenEnv !== undefined) descriptor.tokenEnv = tokenEnv;
  if (required !== undefined) descriptor.required = required;
  const pathMapping = validatePathMappingShape(input.pathMapping);
  if (pathMapping !== undefined) descriptor.pathMapping = pathMapping;
  return descriptor;
}
