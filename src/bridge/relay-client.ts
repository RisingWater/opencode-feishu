/**
 * bridge 中继客户端。
 *
 * bridge 模式下插件不再直连飞书，而是连接本地 Python bridge 服务：
 * - 事件（飞书消息 / 卡片回调 / bot 入群）由 bridge 通过 WS 推送进来
 * - 插件侧的飞书 API 调用经 RPC 请求代理到 bridge
 *
 * 本模块只做连接生命周期（重连 / hello / 心跳 / RPC 关联），
 * 飞书协议语义在 lark-shim 和 gateway 中。
 */
import WebSocket from "ws"
import type { LogFn } from "../types.js"

/** bridge 协议版本；bridge 端不兼容时会拒绝 hello。 */
export const BRIDGE_PROTO_VERSION = 1

/** RPC 请求超时（毫秒）。飞书 API 通常在几百毫秒内返回。 */
const RPC_TIMEOUT_MS = 30_000

/** 心跳间隔（毫秒）。bridge 30s 未收到 ping 会主动断开。 */
const PING_INTERVAL_MS = 15_000

/** 重连退避上限（毫秒）。 */
const MAX_RECONNECT_DELAY_MS = 15_000

/** bridge → 插件的飞书事件载荷。 */
export interface BridgeEventMessage {
  type: "event"
  /** 事件类型：im.message.receive_v1 / card.action.trigger / im.chat.member.bot.added_v1 */
  eventType: string
  /** 飞书原始事件 payload（SDK 分发器收到的 data 对象） */
  payload: Record<string, unknown>
}

/** bridge → 插件的会话绑定控制载荷。 */
export interface BridgeBindMessage {
  type: "bind"
  /** 被绑定的飞书聊天。 */
  chatId: string
  /** 绑定的 OpenCode session ID；null 表示解绑（回到插件默认会话映射）。 */
  sessionId: string | null
}

/** bridge → 插件的 session 列表请求（选择卡片二级列表用）。 */
export interface BridgeListSessionsMessage {
  type: "list_sessions"
  /** 请求 ID，插件通过 respond_sessions 应答。 */
  reqId: string
}

export type BridgeServerMessage = BridgeEventMessage | BridgeBindMessage | BridgeListSessionsMessage

/** bridge RPC 方法名。与 Python 侧 api.py 的分发表一一对应。 */
export type BridgeRpcMethod =
  | "im.message.create"
  | "im.message.update"
  | "im.message.patch"
  | "im.message.delete"
  | "im.message.get"
  | "im.message.list"
  | "im.messageResource.get"
  | "im.chat.get"
  | "contact.user.get"
  | "bot.info"
  | "cardkit.card.create"
  | "cardkit.cardElement.content"
  | "cardkit.cardElement.create"
  | "cardkit.cardElement.update"
  | "cardkit.cardElement.patch"
  | "cardkit.cardElement.delete"
  | "cardkit.card.settings"

/** bridge RPC 调用失败时抛出的异常。message 携带 bridge 返回的错误文本。 */
export class BridgeRpcError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "BridgeRpcError"
  }
}

interface PendingRpc {
  resolve: (value: Record<string, unknown>) => void
  reject: (err: Error) => void
  timer: ReturnType<typeof setTimeout>
}

/** relay-client 的外部依赖与回调。 */
export interface RelayClientOptions {
  /** bridge WebSocket 地址。 */
  url: string
  /** 可选共享 token，bridge 校验失败会拒绝 hello。 */
  token?: string
  /** 本实例的工作目录，注册时上报给 bridge 做路由展示。 */
  workspace: string
  /** 连接成功（hello 完成）回调。 */
  onReady: () => void
  /** 收到飞书事件。 */
  onEvent: (msg: BridgeEventMessage) => void
  /** 收到会话绑定控制消息。 */
  onBind: (msg: BridgeBindMessage) => void
  /** 收到 session 列表请求，应答由调用方通过 respondSessions() 完成。 */
  onListSessions: (msg: BridgeListSessionsMessage) => void
  /** 连接丢失回调（每次断开都会触发，包括重连失败）。 */
  onDisconnect: () => void
  log: LogFn
}

export interface RelayClient {
  /** 发起 RPC 调用并等待 bridge 应答。 */
  rpc: (method: BridgeRpcMethod, params: Record<string, unknown>) => Promise<Record<string, unknown>>
  /** 应答 bridge 的 session 列表请求。 */
  respondSessions: (reqId: string, sessions: Array<{ id: string; title?: string; updateTime?: number }>) => void
  /** 当前连接是否就绪（hello 完成）。 */
  isReady: () => boolean
  /** 主动关闭并停止重连。 */
  close: () => void
}

/**
 * 启动 bridge 中继客户端。
 *
 * 连接断开后按指数退避自动重连（1s → 15s 封顶）；重连成功后重新 hello。
 * RPC 在连接未就绪时直接失败，由调用方决定降级语义。
 */
export function startRelayClient(options: RelayClientOptions): RelayClient {
  const { url, token, workspace, onReady, onEvent, onBind, onListSessions, onDisconnect, log } = options

  let ws: WebSocket | null = null
  let ready = false
  let closed = false
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null
  let pingTimer: ReturnType<typeof setInterval> | null = null
  let reconnectDelay = 1_000
  let rpcSeq = 0

  /** 等待中的 RPC 请求；连接断开时全部立即失败。 */
  const pendingRpcs = new Map<string, PendingRpc>()
  /** 等待应答的 list_sessions 请求。 */
  const pendingSessionRequests = new Map<string, (sessions: Array<{ id: string; title?: string; updateTime?: number }>) => void>()

  function failAllPending(err: Error): void {
    for (const pending of pendingRpcs.values()) {
      clearTimeout(pending.timer)
      pending.reject(err)
    }
    pendingRpcs.clear()
    for (const resolve of pendingSessionRequests.values()) resolve([])
    pendingSessionRequests.clear()
  }

  function send(obj: Record<string, unknown>): boolean {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false
    ws.send(JSON.stringify(obj))
    return true
  }

  function startPing(): void {
    stopPing()
    pingTimer = setInterval(() => {
      send({ type: "ping" })
    }, PING_INTERVAL_MS)
  }

  function stopPing(): void {
    if (pingTimer) {
      clearInterval(pingTimer)
      pingTimer = null
    }
  }

  function scheduleReconnect(): void {
    if (closed || reconnectTimer) return
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null
      reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY_MS)
      connect()
    }, reconnectDelay)
  }

  function handleServerMessage(raw: WebSocket.RawData): void {
    let msg: Record<string, unknown>
    try {
      msg = JSON.parse(raw.toString())
    } catch {
      log("warn", "bridge 消息 JSON 解析失败", { preview: raw.toString().slice(0, 200) })
      return
    }

    switch (msg.type) {
      case "hello_ok": {
        ready = true
        reconnectDelay = 1_000
        startPing()
        log("info", "bridge 连接就绪", { url })
        onReady()
        break
      }
      case "hello_err": {
        const detail = String(msg.error ?? "unknown")
        log("error", "bridge 拒绝注册", { detail })
        // 注册被拒（token 错 / workspace 冲突）通常不会自愈，停止重连避免刷日志。
        closed = true
        try { ws?.close() } catch { /* ignore */ }
        break
      }
      case "event":
        onEvent(msg as unknown as BridgeEventMessage)
        break
      case "bind":
        onBind(msg as unknown as BridgeBindMessage)
        break
      case "list_sessions":
        onListSessions(msg as unknown as BridgeListSessionsMessage)
        break
      case "rpc_result": {
        const id = String(msg.id ?? "")
        const pending = pendingRpcs.get(id)
        if (!pending) break
        pendingRpcs.delete(id)
        clearTimeout(pending.timer)
        if (msg.ok === true) {
          const result = (msg.result && typeof msg.result === "object") ? msg.result as Record<string, unknown> : {}
          pending.resolve(result)
        } else {
          pending.reject(new BridgeRpcError(String(msg.error ?? "bridge rpc failed")))
        }
        break
      }
      case "pong":
        break
      default:
        log("warn", "bridge 未知消息类型", { msgType: String(msg.type) })
    }
  }

  function connect(): void {
    if (closed) return
    log("info", "连接 bridge 服务", { url })
    const socket = new WebSocket(url)
    ws = socket

    socket.on("open", () => {
      const hello: Record<string, unknown> = {
        type: "hello",
        protoVersion: BRIDGE_PROTO_VERSION,
        workspace,
        pid: process.pid,
      }
      if (token) hello.token = token
      send(hello)
    })

    socket.on("message", handleServerMessage)

    socket.on("close", () => {
      const wasReady = ready
      ready = false
      stopPing()
      failAllPending(new BridgeRpcError("bridge 连接已断开"))
      if (ws === socket) ws = null
      if (closed) return
      log(wasReady ? "warn" : "info", "bridge 连接断开", { reconnectInMs: reconnectDelay })
      onDisconnect()
      scheduleReconnect()
    })

    socket.on("error", (err) => {
      log("warn", "bridge 连接错误", { error: err instanceof Error ? err.message : String(err) })
      // close 事件随后触发，重连逻辑集中在那里。
    })
  }

  connect()

  return {
    async rpc(method, params) {
      if (!ready || !ws || ws.readyState !== WebSocket.OPEN) {
        throw new BridgeRpcError("bridge 未就绪")
      }
      const id = `rpc-${++rpcSeq}`
      return new Promise<Record<string, unknown>>((resolve, reject) => {
        const timer = setTimeout(() => {
          pendingRpcs.delete(id)
          reject(new BridgeRpcError(`bridge RPC 超时: ${method}`))
        }, RPC_TIMEOUT_MS)
        pendingRpcs.set(id, { resolve, reject, timer })
        if (!send({ type: "rpc", id, method, params })) {
          pendingRpcs.delete(id)
          clearTimeout(timer)
          reject(new BridgeRpcError("bridge 连接不可用"))
        }
      })
    },

    respondSessions(reqId, sessions) {
      send({ type: "respond_sessions", reqId, sessions })
    },

    isReady() {
      return ready
    },

    close() {
      closed = true
      ready = false
      if (reconnectTimer) {
        clearTimeout(reconnectTimer)
        reconnectTimer = null
      }
      stopPing()
      failAllPending(new BridgeRpcError("relay client 已关闭"))
      try { ws?.close() } catch { /* ignore */ }
      ws = null
    },
  }
}
