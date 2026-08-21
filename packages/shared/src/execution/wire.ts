/**
 * execution/wire.ts —— execution wire 常量（三仓共享事实源）。
 *
 * execution/v1 = 同步 + SSE 流式（P0，sandbox 当前实现）；
 * execution/v1.1 = 模式框架（sync/stream/interactive/persistent，P0.1）。
 * 版本规则：v1.0 客户端遇 v1.1 capabilities 必须 fail-closed；
 * breaking 升级走 v2。
 */

export const EXECUTION_PROTOCOL_VERSION = "execution/v1" as const;
export const EXECUTION_PROTOCOL_VERSION_V11 = "execution/v1.1" as const;

export const EXECUTION_INVOCATION_MODES = ["sync", "stream", "interactive", "persistent"] as const;

export const EXECUTION_WIRE = {
  version: EXECUTION_PROTOCOL_VERSION,
  versions: {
    v1: EXECUTION_PROTOCOL_VERSION,
    v1_1: EXECUTION_PROTOCOL_VERSION_V11,
  },
  paths: {
    /** POST：按 mode 路由执行（sync 同步返回 / stream·interactive 返回 execId） */
    exec: "/exec",
    /** GET：任务状态 */
    job: "/exec/:id",
    /** GET：SSE 流（mode=stream） */
    stream: "/exec/:id/stream",
    /** GET：WebSocket 升级（mode=interactive；stdin/stdout/stderr/resize/pty） */
    ws: "/exec/:id/ws",
    /** POST：尽力取消 */
    cancel: "/exec/:id/cancel",
    /** GET：liveness（无需认证；网络内网可达） */
    health: "/health",
    /** GET：能力声明（v1 / v1.1 协商入口） */
    capabilities: "/capabilities",
    /** persistent 模式 wire 规范（v1.1 定稿，实现与 sandbox kernel-host 迁移捆绑） */
    sessions: "/sessions",
    session: "/sessions/:id",
    sessionExecute: "/sessions/:id/execute",
    sessionSnapshot: "/sessions/:id/snapshot",
    sessionReset: "/sessions/:id/reset",
    sessionRelease: "/sessions/:id/release",
  },
  events: {
    /** SSE event: output {stream,data} */
    output: "output",
    /** SSE event: done {exitCode,timedOut,signal?} */
    done: "done",
    /** SSE event: error {code,message}（v1.1；v1 服务端不发送） */
    error: "error",
  },
  /** interactive WS 消息帧类型（JSON text frame） */
  wsFrames: {
    stdin: "stdin",
    stdout: "stdout",
    stderr: "stderr",
    resize: "resize",
    done: "done",
    error: "error",
  },
  errorCodes: {
    invalidRequest: "INVALID_REQUEST",
    cwdNotAllowed: "CWD_NOT_ALLOWED",
    envRejected: "ENV_REJECTED",
    unauthorized: "UNAUTHORIZED",
    notFound: "NOT_FOUND",
    cancelled: "CANCELLED",
    backendUnavailable: "BACKEND_UNAVAILABLE",
    /** v1.1：请求声明的 mode 不在 backend capabilities.modes 内 */
    modeNotSupported: "MODE_NOT_SUPPORTED",
    /** 服务端 token 未配置等 fail-closed 状态 */
    serverMisconfigured: "SERVER_MISCONFIGURED",
    /** persistent：session 租约到期/已释放仍被使用 */
    sessionExpired: "SESSION_EXPIRED",
    /** persistent：reset/回滚引用的快照不存在 */
    snapshotNotFound: "SNAPSHOT_NOT_FOUND",
    internalError: "INTERNAL_ERROR",
  },
} as const;
