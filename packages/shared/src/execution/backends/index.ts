/**
 * execution/backends —— 通用 execution/v1 backend 实现（三仓共享）。
 */
export { LocalBackend, type LocalBackendOptions } from "./local.js";
export { DockerExecBackend, type DockerExecBackendOptions } from "./docker-exec.js";
