/**
 * execution/index.ts —— 执行面统一协议 barrel（execution/v1 + v1.1 模式框架）。
 */
export * from "./types.js";
export * from "./wire.js";
export {
  validateExecutionRequest,
  validateExecutionBackendDescriptor,
  validateExecutionCapabilities,
  validateExecutionSessionCreateRequest,
  validateExecutionSessionExecuteRequest,
  validateExecutionSessionSnapshotRequest,
  validateExecutionSessionResetRequest,
  isExecutionProfile,
  isInvocationMode,
  isExecutionProtocolVersion,
  resolveExecutionMode,
  resolveExecutionModes,
  ExecutionRequestError,
  ExecutionBackendDescriptorError,
  EXECUTION_LIMITS,
  EXECUTION_SESSION_LIMITS,
} from "./validate.js";
export { HttpExecutionClient, ExecutionClientError } from "./client.js";
export { ExecutionSessionManager, type ExecutionSessionManagerOptions } from "./sessions.js";
export { ExecutionHttpServer, type ExecutionHttpServerOptions } from "./server.js";
export {
  LocalBackend,
  DockerExecBackend,
  HttpExecutionBackend,
  type LocalBackendOptions,
  type DockerExecBackendOptions,
  type HttpExecutionBackendOptions,
} from "./backends/index.js";
