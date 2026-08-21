/**
 * execution/index.ts —— 执行面统一协议 barrel（execution/v1）。
 */
export * from "./types.js";
export * from "./wire.js";
export { validateExecutionRequest, isExecutionProfile, ExecutionRequestError, EXECUTION_LIMITS } from "./validate.js";
