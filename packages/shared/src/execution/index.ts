/**
 * execution/index.ts —— 执行面统一协议 barrel（execution/v1）。
 */
export * from "./types.js";
export * from "./wire.js";
export {
  validateExecutionRequest,
  validateExecutionBackendDescriptor,
  isExecutionProfile,
  ExecutionRequestError,
  ExecutionBackendDescriptorError,
  EXECUTION_LIMITS,
} from "./validate.js";
export { HttpExecutionClient, ExecutionClientError } from "./client.js";
export {
  LocalBackend,
  DockerExecBackend,
  HttpExecutionBackend,
  type LocalBackendOptions,
  type DockerExecBackendOptions,
  type HttpExecutionBackendOptions,
} from "./backends/index.js";
