/**
 * execution/wire.ts —— execution/v1 wire 常量（三仓共享事实源）。
 */

export const EXECUTION_PROTOCOL_VERSION = "execution/v1" as const;

export const EXECUTION_WIRE = {
  version: EXECUTION_PROTOCOL_VERSION,
  paths: {
    /** POST：同步执行或 stream:true 返回 execId */
    exec: "/exec",
    /** GET：任务状态 */
    job: "/exec/:id",
    /** GET：SSE 流 */
    stream: "/exec/:id/stream",
    /** POST：尽力取消 */
    cancel: "/exec/:id/cancel",
    /** GET：liveness（sandbox 无需认证；网络内网可达） */
    health: "/health",
    /** GET：能力声明 */
    capabilities: "/capabilities",
  },
  events: {
    output: "output",
    done: "done",
  },
  errorCodes: {
    invalidRequest: "INVALID_REQUEST",
    cwdNotAllowed: "CWD_NOT_ALLOWED",
    envRejected: "ENV_REJECTED",
    unauthorized: "UNAUTHORIZED",
    notFound: "NOT_FOUND",
    cancelled: "CANCELLED",
    backendUnavailable: "BACKEND_UNAVAILABLE",
  },
} as const;
