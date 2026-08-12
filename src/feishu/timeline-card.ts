/**
 * TimelineCard / TimelineManager：时间线多卡展示模式（replyMode: "timeline"）。
 *
 * 与 StreamingCard（single 模式单卡汇总）不同，时间线模式下一次 run 会按事件顺序
 * 生成多张独立卡片：
 * - thinking 卡：每轮 reasoning（按 partID 判定边界）一张，实时刷新思考内容
 * - 工具卡：每个工具调用（按 callID 区分）一张，展示运行状态
 * - 最终答复卡：收到首个 text 时才创建（延迟创建，避免时间线顺序倒置）
 *
 * 每张卡都带中断（abort）按钮，点击任意卡均可中断当前 run。
 */
import type { CardKitClient, CardKitSchema } from "./cardkit.js"
import * as sender from "./sender.js"
import type * as Lark from "@larksuiteoapi/node-sdk"
import type { LogFn } from "../types.js"
import {
  ACTIONS_ELEMENT_ID,
  REPLY_ELEMENT_ID,
  STATUS_ELEMENT_ID,
  buildAbortAction,
  buildActionsElement,
  buildCompactStatus,
  buildStatusMarkdown,
} from "./result-card-view.js"
import { cleanMarkdown, truncateMarkdown } from "./markdown.js"
import type { ReplyTerminalState, ReplyRunState } from "../handler/reply-run-registry.js"

/** 卡片正文为空时的 plugin UI 占位描述。 */
const EMPTY_CARD_PLACEHOLDER = "_⏳ 等待中…_"

/** 工具卡运行状态对应的状态文案。 */
function toolStatusText(state: "running" | "completed" | "error"): string {
  switch (state) {
    case "running":
      return "🔄 运行中"
    case "completed":
      return "✅ 已完成"
    case "error":
      return "❌ 失败"
    default:
      return state
  }
}

/** 单张时间线卡 schema 构建参数。 */
function buildCardSchema(params: {
  title: string
  status?: string
  content?: string
  abort: ReturnType<typeof buildAbortAction>
}): CardKitSchema {
  const elements: Array<Record<string, unknown>> = []
  if (params.status) {
    elements.push({
      tag: "markdown",
      element_id: STATUS_ELEMENT_ID,
      content: params.status,
    })
  }
  if (params.content) {
    elements.push({
      tag: "markdown",
      element_id: REPLY_ELEMENT_ID,
      content: params.content,
    })
  }
  const actionsElement = buildActionsElement([params.abort])
  if (actionsElement) elements.push(actionsElement)

  return {
    data: {
      schema: "2.0",
      config: {
        streaming_mode: true,
        wide_screen_mode: true,
      },
      header: {
        title: { tag: "plain_text", content: params.title },
        template: "blue",
      },
      body: { elements },
    },
  }
}

/**
 * 单张时间线卡片。
 *
 * 复用与 StreamingCard 相同的串行更新队列 + debounce 机制，
 * 但结构更简单（状态区 + 内容区 + 中断按钮），不承载详细步骤折叠面板。
 */
export class TimelineCard {
  private cardId?: string
  private messageId?: string
  private seq = 0
  /** 串行更新队列。 */
  private queue: Promise<void> = Promise.resolve()
  private closed = false
  private contentTimer: ReturnType<typeof setTimeout> | null = null
  /** 当前累积的内容文本。 */
  private text = ""
  /** 避免重复写相同内容。 */
  private readonly rendered = { status: "", content: "" }
  /** CardKit 中途更新失败后进入 degraded：停止刷新但保留本地快照。 */
  private degraded = false

  constructor(
    private readonly cardkit: CardKitClient,
    private readonly feishuClient: InstanceType<typeof Lark.Client>,
    private readonly chatId: string,
    private readonly log: LogFn,
  ) {}

  /** 创建卡片实体并发送到飞书聊天，返回消息 ID。 */
  async start(params: { title: string; status?: string; content?: string; abort: ReturnType<typeof buildAbortAction> }): Promise<string> {
    const schema = buildCardSchema(params)
    this.cardId = await this.cardkit.createCard(schema)
    const res = await sender.sendCardMessage(this.feishuClient, this.chatId, this.cardId, this.log)
    if (!res.ok || !res.messageId) {
      throw new Error(`发送时间线卡片消息失败: ${res.error ?? "unknown"}`)
    }
    this.rendered.status = params.status ?? ""
    this.rendered.content = normalizeTimelineContent(params.content ?? "")
    this.messageId = res.messageId
    return this.messageId
  }

  /** 更新状态区文本。 */
  async setStatus(status: string): Promise<void> {
    if (this.closed || !this.cardId) return
    if (this.rendered.status === status) return
    this.rendered.status = status
    this.enqueue(async () => {
      if (this.degraded || !this.cardId) return
      await this.cardkit.updateElement(this.cardId, STATUS_ELEMENT_ID, status, ++this.seq)
    })
  }

  /** 用完整文本替换内容区（快照语义）。 */
  async replaceText(fullText: string): Promise<void> {
    if (this.closed || !this.cardId) return
    this.text = fullText
    this.scheduleContentRender()
  }

  /** 关闭流式模式，定格卡片。 */
  async close(): Promise<void> {
    if (this.closed) return
    if (!this.cardId) {
      this.closed = true
      return
    }
    this.flushContentTimer()
    await this.drain()
    if (this.degraded) {
      this.closed = true
      return
    }
    try {
      await this.renderContent()
    } catch (err) {
      this.markDegraded(err)
    }
    try {
      await this.cardkit.closeStreaming(this.cardId, ++this.seq)
    } catch (err) {
      this.log("error", "TimelineCard closeStreaming 失败（内容已定格）", {
        cardId: this.cardId,
        error: err instanceof Error ? err.message : String(err),
      })
    }
    this.closed = true
  }

  /** 删除整条飞书消息（早期失败场景清理）。 */
  async destroy(): Promise<void> {
    this.closed = true
    if (this.contentTimer) {
      clearTimeout(this.contentTimer)
      this.contentTimer = null
    }
    if (this.messageId) {
      await sender.deleteMessage(this.feishuClient, this.messageId, this.log)
    }
  }

  private scheduleContentRender(): void {
    if (this.contentTimer) clearTimeout(this.contentTimer)
    this.contentTimer = setTimeout(() => {
      this.contentTimer = null
      this.enqueue(async () => {
        await this.renderContent()
      })
    }, 200)
  }

  private flushContentTimer(): void {
    if (this.contentTimer) {
      clearTimeout(this.contentTimer)
      this.contentTimer = null
      this.enqueue(async () => {
        await this.renderContent()
      })
    }
  }

  private async renderContent(): Promise<void> {
    if (this.degraded || !this.cardId) return
    const content = normalizeTimelineContent(this.text)
    if (this.rendered.content === content) return
    this.rendered.content = content
    await this.cardkit.updateElement(this.cardId, REPLY_ELEMENT_ID, content, ++this.seq)
  }

  private enqueue(fn: () => Promise<void>): void {
    this.queue = this.queue.then(fn).catch((err) => {
      this.markDegraded(err)
      this.log("error", "TimelineCard queue 操作失败", {
        error: err instanceof Error ? err.message : String(err),
      })
    })
  }

  private markDegraded(err: unknown): void {
    if (this.degraded) return
    this.degraded = true
    this.log("warn", "TimelineCard 进入 degraded（UI 停止刷新，保留本地快照）", {
      error: err instanceof Error ? err.message : String(err),
    })
  }

  private async drain(): Promise<void> {
    await this.queue
  }
}

/** 时间线多卡编排所需的运行元信息。 */
export interface TimelineManagerMeta {
  runId: string
  sessionId: string
  /** 最终答复卡的标题（用户消息首行）。 */
  title: string
}

/**
 * 时间线多卡编排器。
 *
 * 对外 API 全部串行执行（内部 queue），避免并发 SSE 事件导致建卡/更新竞态。
 * 卡片创建或更新失败仅降级日志，不阻断主对话流程。
 */
export class TimelineManager {
  private thinkingCard?: TimelineCard
  private thinkingPartID = ""
  private readonly toolCards = new Map<string, TimelineCard>()
  private finalCard?: TimelineCard
  private terminalState?: ReplyTerminalState
  private queue: Promise<void> = Promise.resolve()

  constructor(
    private readonly cardkit: CardKitClient,
    private readonly feishuClient: InstanceType<typeof Lark.Client>,
    private readonly chatId: string,
    private readonly log: LogFn,
    private readonly meta: TimelineManagerMeta,
  ) {}

  /** 新增/更新 thinking 卡。partID 变化视为新一轮思考。 */
  ensureThinkingCard(partID: string, text: string): Promise<void> {
    return this.enqueue(async () => {
      if (this.terminalState) return
      if (partID !== this.thinkingPartID) {
        await this.closeActiveThinking()
        const created = await this.createCard({
          title: "💭 思考过程",
          status: "💭 思考中",
        })
        if (!created) return
        this.thinkingPartID = partID
        this.thinkingCard = created
      }
      await this.thinkingCard?.replaceText(text)
    })
  }

  /** 新增/更新工具卡。按 callID 区分；终态时定格。 */
  ensureToolCard(callID: string, tool: string, state: "running" | "completed" | "error"): Promise<void> {
    return this.enqueue(async () => {
      if (this.terminalState) return
      let card = this.toolCards.get(callID)
      if (!card) {
        // 新工具开始：先定格当前思考卡，让时间线顺序正确。
        await this.closeActiveThinking()
        const created = await this.createCard({
          title: normalizeToolTitle(tool),
          status: toolStatusText(state),
        })
        if (!created) return
        card = created
        this.toolCards.set(callID, card)
      }
      await card?.setStatus(toolStatusText(state))
      if (state === "completed" || state === "error") {
        await card?.close()
      }
    })
  }

  /** 新增/更新最终答复卡。幂等：收到首个 text 时才创建。 */
  ensureFinalCard(text: string): Promise<void> {
    return this.enqueue(async () => {
      if (this.terminalState) return
      await this.closeActiveThinking()
      if (!this.finalCard) {
        const created = await this.createCard({
          title: this.meta.title,
          status: "⏳ 正在生成回复",
        })
        if (!created) return
        this.finalCard = created
      }
      await this.finalCard?.replaceText(text)
    })
  }

  /** 终态收尾：定格所有 active 卡，最终卡写入结论。 */
  async finalize(state: ReplyRunState, conclusion?: string): Promise<void> {
    return this.enqueue(async () => {
      this.terminalState = toTerminalState(state)
      await this.closeActiveThinking()
      for (const card of this.toolCards.values()) {
        await card.close()
      }
      this.toolCards.clear()
      if (this.finalCard) {
        await this.finalCard.setStatus(buildStatusMarkdown(buildCompactStatus(state)))
        if (conclusion) {
          await this.finalCard.replaceText(conclusion)
        }
        await this.finalCard.close()
      }
    })
  }

  private async createCard(params: { title: string; status?: string }): Promise<TimelineCard | undefined> {
    const card = new TimelineCard(this.cardkit, this.feishuClient, this.chatId, this.log)
    try {
      await card.start({
        title: params.title,
        status: params.status,
        content: EMPTY_CARD_PLACEHOLDER,
        abort: buildAbortAction(this.meta.runId, this.meta.sessionId),
      })
      return card
    } catch (err) {
      this.log("error", "创建时间线卡片失败（跳过该卡片，不阻断主流程）", {
        runId: this.meta.runId,
        title: params.title,
        error: err instanceof Error ? err.message : String(err),
      })
      await card.destroy().catch(() => {})
      return undefined
    }
  }

  private async closeActiveThinking(): Promise<void> {
    if (this.thinkingCard) {
      await this.thinkingCard.close()
      this.thinkingCard = undefined
    }
    this.thinkingPartID = ""
  }

  private enqueue(fn: () => Promise<void>): Promise<void> {
    const run = this.queue.then(fn)
    this.queue = run.catch(() => undefined)
    return run
  }
}

function normalizeTimelineContent(content: string): string {
  const cleaned = cleanMarkdown(content)
    .replace(/\r/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim()
  if (!cleaned) return EMPTY_CARD_PLACEHOLDER
  return truncateMarkdown(cleaned)
}

function normalizeToolTitle(tool: string): string {
  const cleaned = tool.replace(/\s+/g, " ").trim()
  const limited = cleaned.length <= 50 ? cleaned : `${cleaned.slice(0, 49)}…`
  return limited || "工具调用"
}

function toTerminalState(state: ReplyRunState): ReplyTerminalState | undefined {
  switch (state) {
    case "completed":
      return "completed"
    case "failed":
      return "failed"
    case "timed_out":
      return "timed_out"
    case "aborted":
      return "aborted"
    default:
      return undefined
  }
}
