/**
 * Lark Client 形状的 bridge 代理（shim）。
 *
 * bridge 模式下插件不再持有真实 Lark SDK Client。`feishu/` 下所有模块
 * （sender / cardkit / resource / quote / user-name / history）只消费
 * `client.im.* / client.cardkit.* / client.contact.* / client.request()`，
 * 本类以相同的方法签名把它们转发为 bridge RPC。
 *
 * 只实现仓库实际调用的方法子集；未覆盖的方法抛错提醒走直连模式。
 * 通过 `as unknown as Lark.Client` 注入到现有依赖位，调用方零改动。
 */
import { Readable } from "node:stream"
import type { BridgeRpcMethod, RelayClient } from "./relay-client.js"
import { BridgeRpcError } from "./relay-client.js"
import type { LogFn } from "../types.js"

/** 模拟 Lark SDK 响应的通用形状：业务数据在 data，错误码在 code/msg。 */
type ShimResponse<T> = T & { code?: number; msg?: string }

/** 从 Lark 风格参数对象中提取 path / params / data 三段。 */
function splitLarkParams(input: Record<string, unknown>): {
  path: Record<string, unknown>
  params: Record<string, unknown>
  data: Record<string, unknown>
} {
  return {
    path: (input.path && typeof input.path === "object") ? input.path as Record<string, unknown> : {},
    params: (input.params && typeof input.params === "object") ? input.params as Record<string, unknown> : {},
    data: (input.data && typeof input.data === "object") ? input.data as Record<string, unknown> : {},
  }
}

/**
 * 资源下载 shim：把 bridge 返回的 base64 转成 Lark SDK 下载响应形状。
 *
 * resource.ts 消费 `res.getReadableStream()` 和 `res.headers["content-type"]`。
 */
function makeResourceResponse(base64: string, mime: string, headers: Record<string, string>) {
  const buffer = Buffer.from(base64, "base64")
  let cachedStream: Readable | null = null
  return {
    headers,
    code: 0,
    msg: "success",
    getReadableStream(): Readable {
      if (!cachedStream) {
        cachedStream = Readable.from(buffer)
      }
      return cachedStream
    },
  }
}

/** 把底层错误折叠成 Lark SDK 风格的异常（sender/cardkit 会提取 code/msg/logId）。 */
function wrapRpcError(err: unknown, method: string): Error {
  if (err instanceof BridgeRpcError) {
    const wrapped = new Error(`[${method}] ${err.message}`) as Error & { code?: number; msg?: string }
    wrapped.code = -1
    wrapped.msg = err.message
    return wrapped
  }
  return err instanceof Error ? err : new Error(String(err))
}

export class BridgeLarkShim {
  constructor(
    private readonly relay: RelayClient,
    private readonly log: LogFn,
  ) {}

  private async call<T>(method: BridgeRpcMethod, params: Record<string, unknown>): Promise<ShimResponse<T>> {
    try {
      const result = await this.relay.rpc(method, params)
      // bridge 返回 { code, msg, data }；code 缺省视为成功。
      const code = typeof result.code === "number" ? result.code : 0
      const data = (result.data && typeof result.data === "object") ? result.data as T : {} as T
      return { code, msg: typeof result.msg === "string" ? result.msg : "success", data } as unknown as ShimResponse<T>
    } catch (err) {
      throw wrapRpcError(err, method)
    }
  }

  /** 资源下载走专用 RPC，返回值带 base64 数据。 */
  private async callResource(params: Record<string, unknown>): Promise<ReturnType<typeof makeResourceResponse>> {
    try {
      const result = await this.relay.rpc("im.messageResource.get", params)
      const base64 = typeof result.dataBase64 === "string" ? result.dataBase64 : ""
      const mime = typeof result.mime === "string" ? result.mime : "application/octet-stream"
      const headers = (result.headers && typeof result.headers === "object")
        ? result.headers as Record<string, string>
        : { "content-type": mime }
      return makeResourceResponse(base64, mime, headers)
    } catch (err) {
      throw wrapRpcError(err, "im.messageResource.get")
    }
  }

  /** 供 index.ts 获取 bot open_id 用（与真实 client.request() 同形）。
   * 真实 SDK 的 request() 直接返回响应体 JSON（bot 字段在顶层），
   * 因此这里把 bridge 的 {code,msg,data} 展平成 data 内容。 */
  async request<T>(options: { url: string; method?: string }): Promise<ShimResponse<T>> {
    if (!options.url.includes("/bot/v3/info")) {
      throw new Error(`bridge 模式暂不支持通用 request(): ${options.url}`)
    }
    const res = await this.call<{ bot?: { open_id?: string } }>("bot.info", {})
    const data = res as unknown as { data?: { bot?: { open_id?: string } } }
    return { ...(data.data ?? {}) } as ShimResponse<T>
  }

  /** SDK 命名空间形状的只读代理。 */
  readonly im = {
    message: {
      create: (input: Record<string, unknown>) =>
        this.call<{ data?: { message_id?: string } }>("im.message.create", splitLarkParams(input).data),
      update: (input: Record<string, unknown>) =>
        this.call("im.message.update", splitLarkParams(input)),
      patch: (input: Record<string, unknown>) =>
        this.call("im.message.patch", splitLarkParams(input)),
      delete: (input: Record<string, unknown>) =>
        this.call("im.message.delete", splitLarkParams(input)),
      get: (input: Record<string, unknown>) =>
        this.call<{ data?: { items?: Array<Record<string, unknown>> } }>("im.message.get", splitLarkParams(input).path),
      list: (input: Record<string, unknown>) =>
        this.call<{ data?: { items?: Array<Record<string, unknown>>; has_more?: boolean; page_token?: string } }>(
          "im.message.list", { ...splitLarkParams(input).params, ...splitLarkParams(input).path },
        ),
    },
    messageResource: {
      get: (input: Record<string, unknown>) => {
        const { path, params } = splitLarkParams(input)
        return this.callResource({ ...path, ...params })
      },
    },
    chat: {
      get: (input: Record<string, unknown>) =>
        this.call<{ data?: { chat_mode?: string; chat_type?: string } }>("im.chat.get", splitLarkParams(input).path),
    },
  }

  readonly cardkit = {
    v1: {
      card: {
        create: (input: Record<string, unknown>) =>
          this.call<{ data?: { card_id?: string } }>("cardkit.card.create", splitLarkParams(input).data),
        settings: (input: Record<string, unknown>) =>
          this.call("cardkit.card.settings", splitLarkParams(input)),
      },
      cardElement: {
        content: (input: Record<string, unknown>) =>
          this.call("cardkit.cardElement.content", splitLarkParams(input)),
        create: (input: Record<string, unknown>) =>
          this.call("cardkit.cardElement.create", splitLarkParams(input)),
        update: (input: Record<string, unknown>) =>
          this.call("cardkit.cardElement.update", splitLarkParams(input)),
        patch: (input: Record<string, unknown>) =>
          this.call("cardkit.cardElement.patch", splitLarkParams(input)),
        delete: (input: Record<string, unknown>) =>
          this.call("cardkit.cardElement.delete", splitLarkParams(input)),
      },
    },
  }

  readonly contact = {
    user: {
      get: (input: Record<string, unknown>) =>
        this.call<{ data?: { user?: { name?: string } } }>("contact.user.get", splitLarkParams(input)),
    },
  }
}

/**
 * 创建 bridge 模式下的伪 Lark Client。
 *
 * 返回值通过 `as unknown as Lark.Client` 注入现有依赖位；
 * 仓库未使用的 SDK 能力不可用，调用会抛出明确错误。
 */
export function createBridgeLarkShim(relay: RelayClient, log: LogFn): unknown {
  const shim = new BridgeLarkShim(relay, log)
  log("info", "bridge Lark shim 已创建（飞书 API 调用将代理到 bridge）")
  return shim
}
