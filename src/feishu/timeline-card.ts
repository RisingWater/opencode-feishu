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
  normalizeReplyTitle,
} from "./result-card-view.js"
import { cleanMarkdown, truncateMarkdown } from "./markdown.js"
import type { ReplyTerminalState, ReplyRunState } from "../handler/reply-run-registry.js"

/** 卡片正文为空时的 plugin UI 占位描述。 */
const EMPTY_CARD_PLACEHOLDER = "_⏳ 等待中…_"

/** 工具卡正文：命令（input）+ 输出（output）。 */
function buildToolContent(input: Record<string, unknown> | undefined, output: string | undefined): string {
  const sections: string[] = []
  const commandText = extractToolCommand(input)
  if (commandText) {
    sections.push(`**命令**\n\`\`\`\n${commandText}\n\`\`\``)
  } else if (input && Object.keys(input).length > 0) {
    sections.push(`**入参**\n\`\`\`\n${JSON.stringify(input, null, 2)}\n\`\`\``)
  }
  if (output && output.trim()) {
    sections.push(`**输出**\n\`\`\`\n${output.trim()}\n\`\`\``)
  }
  return sections.length > 0 ? sections.join("\n\n") : EMPTY_CARD_PLACEHOLDER
}

/** 从工具入参提取可展示的命令文本（bash 等命令行工具）。 */
function extractToolCommand(input: Record<string, unknown> | undefined): string | undefined {
  if (!input) return undefined
  const candidate = input.command ?? input.cmd ?? input.command_line ?? input.description
  if (typeof candidate !== "string" || !candidate.trim()) return undefined
  return candidate.trim()
}

/** 单张时间线卡 schema 构建参数。 */
function buildCardSchema(params: {
  title: string
  status?: string
  content?: string
  abort?: ReturnType<typeof buildAbortAction>
}): CardKitSchema {
  const elements: Array<Record<string, unknown>> = []
  if (params.status) {
    elements.push({
      tag: "markdown",
      element_id: STATUS_ELEMENT_ID,
      content: params.status,
    })
  }
  // reply_text 元素始终存在（用占位内容），否则后续 replaceText 更新不存在的元素会失败。
  elements.push({
    tag: "markdown",
    element_id: REPLY_ELEMENT_ID,
    content: params.content ?? EMPTY_CARD_PLACEHOLDER,
  })
  if (params.abort) {
    const actionsElement = buildActionsElement([params.abort])
    if (actionsElement) elements.push(actionsElement)
  }

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
  /** 当前累积的内容文本（只读，供 TimelineManager 去重判断）。 */
  get currentText(): string {
    return this.text
  }
  /** 避免重复写相同内容。 */
  private readonly rendered = { status: "", content: "" }
  /** CardKit 中途更新失败后进入 degraded：停止刷新但保留本地快照。 */
  private degraded = false
  /** 卡片是否带中断按钮（决定 close 时是否删除 actions 元素）。 */
  private hasActions = false

  constructor(
    private readonly cardkit: CardKitClient,
    private readonly feishuClient: InstanceType<typeof Lark.Client>,
    private readonly chatId: string,
    private readonly log: LogFn,
  ) {}

  /** 创建卡片实体并发送到飞书聊天，返回消息 ID。 */
  async start(params: { title: string; status?: string; content?: string; abort?: ReturnType<typeof buildAbortAction> }): Promise<string> {
    const schema = buildCardSchema(params)
    this.hasActions = !!params.abort
    this.cardId = await this.cardkit.createCard(schema)
    const res = await sender.sendCardMessage(this.feishuClient, this.chatId, this.cardId, this.log)
    if (!res.ok || !res.messageId) {
      throw new Error(`发送时间线卡片消息失败: ${res.error ?? "unknown"}`)
    }
    this.rendered.status = params.status ?? ""
    this.text = params.content ?? ""
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
    if (this.hasActions) {
      this.enqueue(async () => {
        if (this.degraded || !this.cardId) return
        try {
          await this.cardkit.deleteElement(this.cardId, ACTIONS_ELEMENT_ID, ++this.seq)
        } catch (err) {
          this.log("error", "TimelineCard close 移除中断按钮失败", {
            error: err instanceof Error ? err.message : String(err),
          })
        }
      })
    }
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
  /** 用户消息完整原文，用于过滤 OpenCode 回显的 text part。 */
  userText: string
}

/**
 * 时间线多卡编排器。
 *
 * 对外 API 全部串行执行（内部 queue），避免并发 SSE 事件导致建卡/更新竞态。
 * 卡片创建或更新失败仅降级日志，不阻断主对话流程。
 */

/** 待建卡队列 key：think=partID，tool=callID，trans=partID。 */
type PendingCardKey = `think:${string}` | `tool:${string}` | `trans:${string}`

/** 待建卡条目：携带 part.time.start，flush 时按 time 升序建卡保证顺序。 */
interface PendingCard {
  /** 真实输出顺序时间戳。 */
  time: number
  /** 卡片标题。 */
  title: string
  /** 卡片正文。 */
  content?: string
  /** 是否需要中断按钮（用户卡不需要）。 */
  withAbort?: boolean
  /** 工具/思考/过渡的类型标记。 */
  kind: "think" | "tool" | "trans"
}
export class TimelineManager {
  private thinkingCard?: TimelineCard
  private thinkingPartID = ""
  private readonly toolCards = new Map<string, TimelineCard>()
  private readonly transitionCards = new Map<string, TimelineCard>()
  private finalCard?: TimelineCard
  private userCard?: TimelineCard
  private terminalState?: ReplyTerminalState
  /** 已被定格移除的过渡 partID：后续同 partID 文本不再重复建卡。 */
  private readonly closedTransitionPartIDs = new Set<string>()
  /** 轮询/事件期间累积的最终文本，finalize 时才创建最终答复卡。 */
  private pendingFinalText = ""
  /**
   * 待建卡队列：SSE 事件先入队（带 time.start），200ms 窗口后按 time 排序批量建卡，
   * 保证时间线顺序与 TUI 一致（不依赖事件到达顺序）。
   */
  private readonly pendingCards = new Map<string, PendingCard>()
  private flushTimer: ReturnType<typeof setTimeout> | null = null
  private queue: Promise<void> = Promise.resolve()

  constructor(
    private readonly cardkit: CardKitClient,
    private readonly feishuClient: InstanceType<typeof Lark.Client>,
    private readonly chatId: string,
    private readonly log: LogFn,
    private readonly meta: TimelineManagerMeta,
  ) {}

  /** 用户消息确认卡：友好提示已收到，开始处理。无 abort 按钮。 */
  ensureUserMessageCard(text: string): Promise<void> {
    return this.enqueue(async () => {
      if (this.terminalState) return
      if (this.userCard) return
      const created = await this.createCard({
        title: normalizeReplyTitle(this.meta.userText),
        content: `${text}\n\n_收到，开始处理…_`,
        withAbort: false,
      })
      this.log("info", "timeline.userCard.created", { created: !!created })
      if (!created) return
      this.userCard = created
    })
  }

  /** 新增/更新 thinking 卡。partID 变化视为新一轮思考。 */
  ensureThinkingCard(partID: string, text: string, time?: number): Promise<void> {
    return this.enqueue(async () => {
      if (this.terminalState) return
      // 已建卡：直接更新内容。
      if (this.thinkingCard && this.thinkingPartID === partID) {
        await this.thinkingCard.replaceText(text)
        return
      }
      // 未建卡：入队等 flush 按时间排序建卡。
      const key = `think:${partID}`
      const existing = this.pendingCards.get(key)
      this.pendingCards.set(key, {
        time: mergePendingTime(existing?.time, time),
        title: "💭 思考过程",
        content: text,
        kind: "think",
      })
      this.scheduleFlush()
    })
  }

  /** 新增/更新工具卡。按 callID 区分；终态时定格。 */
  ensureToolCard(
    callID: string,
    tool: string,
    state: "running" | "completed" | "error",
    input?: Record<string, unknown>,
    output?: string,
    time?: number,
  ): Promise<void> {
    return this.enqueue(async () => {
      if (this.terminalState) return
      // 已建卡：直接更新内容/状态。
      const existing = this.toolCards.get(callID)
      if (existing) {
        if (input) await existing.replaceText(buildToolContent(input, output))
        if (state === "completed" || state === "error") {
          await existing.close()
          this.toolCards.delete(callID)
        }
        return
      }
      // 未建卡：入队等 flush 按时间排序建卡。
      const key = `tool:${callID}`
      const existingPending = this.pendingCards.get(key)
      const mergedTime = mergePendingTime(existingPending?.time, time)
      this.log("info", "timeline.tool.pending", {
        callID, tool, state, time, existingTime: existingPending?.time, mergedTime,
      })
      this.pendingCards.set(key, {
        time: mergedTime,
        title: normalizeToolTitle(tool),
        content: buildToolContent(input, output),
        kind: "tool",
      })
      this.scheduleFlush()
    })
  }

  /**
   * 过渡叙述文本（工具调用前后的过程性文字），独立卡片展示，不带状态字样。
   * 过滤 OpenCode 回显的用户消息前缀。
   */
  ensureTextCard(partID: string, text: string, time?: number): Promise<void> {
    return this.enqueue(async () => {
      if (this.terminalState) return
      // 已定格移除的 partID 不重复建卡。
      if (this.closedTransitionPartIDs.has(partID)) return
      let effective = text.trim()
      if (this.meta.userText.trim() && effective.startsWith(this.meta.userText.trim())) {
        effective = effective.slice(this.meta.userText.trim().length).trim()
      }
      if (!effective) return
      // 去重：OpenCode 的 text part 快照是累积的——新 partID 的内容包含之前 part 的完整文本。
      // 若本段文本与任一已显示过渡卡相同，或以其为前缀只新增了后缀，则跳过重复 / 只显示新增部分。
      for (const [, existing] of this.transitionCards) {
        const prev = existing.currentText.trim()
        if (!prev) continue
        if (effective === prev) return // 内容完全重复：不建卡
        if (effective.startsWith(prev)) {
          effective = effective.slice(prev.length).trim()
          if (!effective) return
          break
        }
      }
      // 已建卡：更新内容。
      const built = this.transitionCards.get(partID)
      if (built) {
        await built.replaceText(effective)
        return
      }
      // 未建卡：入队等 flush 按时间排序建卡。
      const key = `trans:${partID}`
      const existingPending = this.pendingCards.get(key)
      this.pendingCards.set(key, {
        time: mergePendingTime(existingPending?.time, time),
        title: "📝 处理中",
        content: effective,
        withAbort: false,
        kind: "trans",
      })
      this.scheduleFlush()
    })
  }

  /** 累积最终答复文本，finalize 时创建最终答复卡。 */
  ensureFinalCard(text: string): Promise<void> {
    return this.enqueue(async () => {
      if (this.terminalState) return
      this.pendingFinalText = text
    })
  }

  /** 终态收尾：定格所有 active 卡，最终卡写入结论。 */
  async finalize(state: ReplyRunState, conclusion?: string): Promise<void> {
    return this.enqueue(async () => {
      this.terminalState = toTerminalState(state)
      // 清掉未触发的 flush timer，强制建完剩余 pending 卡。
      if (this.flushTimer) {
        clearTimeout(this.flushTimer)
        this.flushTimer = null
      }
      await this.flushPendingCards()
      await this.closeActiveThinking()
      await this.closeTransitionCards()
      if (this.userCard) {
        await this.userCard.close()
        this.userCard = undefined
      }
      for (const card of this.toolCards.values()) {
        await card.close()
      }
      this.toolCards.clear()
      // 最终答复卡在收尾阶段才创建，保证时间线顺序（思考 → 工具 → 最终答复）。
      const finalText = conclusion ?? this.pendingFinalText
      const created = await this.createCard({
        title: "🤖 最终答复",
        status: buildStatusMarkdown(buildCompactStatus(state)),
        content: finalText,
      })
      if (!created) return
      this.finalCard = created
      await created.close()
    })
  }

  /**
   * 安排一次 flush：缓冲窗口内到达的 SSE 事件一起按 time.start 排序建卡。
   * 200ms 足够覆盖相邻事件的到达间隔，同时不显著延迟卡片显示。
   */
  private scheduleFlush(): void {
    if (this.flushTimer) return
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null
      this.enqueue(async () => {
        await this.flushPendingCards()
      })
    }, 200)
  }

  /** 按 time 升序建卡；建完后已建卡走实时更新路径。 */
  private async flushPendingCards(): Promise<void> {
    if (this.terminalState || this.pendingCards.size === 0) return
    const entries = Array.from(this.pendingCards.entries())
    this.log("info", "timeline.flush.start", {
      count: entries.length,
      pending: entries.map(([k, v]) => ({ key: k, title: v.title, time: v.time })),
    })
    const sorted = entries.sort((a, b) => a[1].time - b[1].time)
    this.pendingCards.clear()
    for (const [key, item] of sorted) {
      const created = await this.createCard({
        title: item.title,
        content: item.content,
        withAbort: item.withAbort,
      })
      if (!created) continue
      this.log("info", "timeline.card.flushed", { key, title: item.title, time: item.time })
      if (key.startsWith("tool:")) {
        this.toolCards.set(key.slice("tool:".length), created)
      } else if (key.startsWith("think:")) {
        this.thinkingPartID = key.slice("think:".length)
        this.thinkingCard = created
      } else if (key.startsWith("trans:")) {
        this.transitionCards.set(key.slice("trans:".length), created)
      }
    }
  }

  private async createCard(
    params: { title: string; status?: string; content?: string; withAbort?: boolean },
  ): Promise<TimelineCard | undefined> {
    const card = new TimelineCard(this.cardkit, this.feishuClient, this.chatId, this.log)
    try {
      await card.start({
        title: params.title,
        status: params.status,
        content: params.content ?? EMPTY_CARD_PLACEHOLDER,
        abort: params.withAbort !== false
          ? buildAbortAction(this.meta.runId, this.meta.sessionId)
          : undefined,
      })
      this.log("info", "timeline.card.created", { title: params.title, runId: this.meta.runId })
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

  /** 定格并移除所有过渡卡，记录已关闭 partID 防重复。 */
  private async closeTransitionCards(): Promise<void> {
    for (const [partID, card] of this.transitionCards) {
      await card.close()
      this.closedTransitionPartIDs.add(partID)
    }
    this.transitionCards.clear()
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

/**
 * 合并待建卡时间戳：首次事件可能不带 time（回退 MAX），后续事件带来真实 time 时覆盖。
 * 已有时戳优先保留（更早的起始时间），MAX 占位则被真实 time 替换。
 */
function mergePendingTime(existing: number | undefined, incoming: number | undefined): number {
  if (incoming === undefined) return existing ?? Number.MAX_SAFE_INTEGER
  if (existing === undefined || existing === Number.MAX_SAFE_INTEGER) return incoming
  return existing
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
