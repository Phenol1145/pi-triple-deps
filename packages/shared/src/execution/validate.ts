/**
 * execution/validate.ts —— execution/v1 请求校验（backend 无关的纯函数）。
 *
 * sandbox 侧的 cwd 白名单/私有工作区校验仍由 pth-sandbox 按 profile 做；
 * 这里只做跨 backend 共有的结构与数值校验。
 */

import type {
  ExecutionBackendDescriptor,
  ExecutionPathMapping,
  ExecutionProfile,
  ExecutionRequest,
} from "./types.js";
import { EXECUTION_WIRE } from "./wire.js";

export const EXECUTION_LIMITS = {
  maxTimeoutMs: 24 * 60 * 60 * 1000,
  maxOutputBytes: 4 * 1024 * 1024,
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

/**
 * 结构化校验；返回规范化后的请求（默认值展开）。
 * profile 只做结构校验——信任档的强制由 backend 决定，客户端传入仅供声明。
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
