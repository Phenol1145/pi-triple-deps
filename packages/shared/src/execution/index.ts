/**
 * execution/index.ts —— 执行面统一协议 barrel（execution/v1）。
 */
export * from "./types.js";
export * from "./wire.js";
export { validateExecutionRequest, isExecutionProfile, ExecutionRequestError, EXECUTION_LIMITS } from "./validate.js";
export { HttpExecutionClient, ExecutionClientError } from "./client.js";
export { LocalBackend, DockerExecBackend } from "./backends/index.js";
export type { LocalBackendOptions, DockerExecBackendOptions } from "./backends/index.js";
