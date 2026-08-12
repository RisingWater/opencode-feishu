/**
 * 时间线多卡对话处理链路（replyMode: "timeline"）。
 *
 * 与 chat.ts（single 模式单卡汇总）共享消息 → OpenCode 的主链路骨架
 * （session 绑定、baseline、promptAsync、轮询、错误分类），区别只在卡片渲染：
 * - 不预创建占位卡；thinking / 工具 / 最终答复按 SSE 事件顺序各自成卡
 * - 最终答复卡延迟到首个 text 快照时才创建
 * - 错误收尾用 TimelineManager.finalize 定格所有 active 卡
 *
 * 输入透传原则与 single 模式一致：不塑形 agent 内容，只做展示投影。
 */
import type { FeishuMessageContext } from "../types.js"
import type { OpencodeClient } from "@opencode-ai/sdk"
import type * as Lark from "@larksuiteoapi/node-sdk"
import * as sender from "../feishu/sender.js"
import {
  clearNudge,
  clearRetryAttempts,
  clearSessionError,
  registerPending,
  unregisterPending,
} from "./event.js"
import {
  extractSessionError,
  tryModelRecovery,
} from "./error-recovery.js"
import { classify, matchPluginError, toLog } from "./errors.js"
import { buildSessionKey, getOrCreateSession, invalidateSession } from "../session.js"
import { registerSessionChat } from "../feishu/session-chat-map.js"
import { getSessionIdleVersion, subscribe } from "./action-bus.js"
import { TimelineManager } from "../feishu/timeline-card.js"
import {
  completeReplyRun,
  createReplyRun,
  getRunAbortSignal,
  getRunByRunId,
} from "./reply-run-registry.js"
import {
  handlePermissionRequested,
  handleQuestionRequested,
} from "./interactive.js"
import {
  buildPromptParts,
  extractLastAssistantSnapshot,
  findAndCleanPoisonedMessage,
  mergeAbortSignals,
  pollForResponse,
  traceLangfuseUser,
  type AssistantSnapshot,
  type ChatDeps,
} from "./chat.js"

/** 终态错误文案（与 single 模式 finalizeReply 对齐）。 */
function terminalConclusionFor(
  state: "completed" | "failed" | "timed_out" | "aborted",
  conclusion?: string,
): string | undefined {
  if (conclusion && conclusion.trim()) return conclusion
  switch (state) {
    case "aborted":
      return "已中断，保留当前可见结果。"
    case "failed":
      return "❌ 当前回答失败。"
    case "timed_out":
      return "⚠️ 响应超时。"
    default:
      return conclusion
  }
}

/**
 * 处理一条飞书消息（时间线模式）。
 *
 * `signal` 透传给轮询等待逻辑，支持外部取消。
 */
export async function handleTimelineChat(
  ctx: FeishuMessageContext,
  deps: ChatDeps,
  signal?: AbortSignal,
): Promise<void> {
  const { content, chatId, chatType, senderId, shouldReply, messageType, rawContent, messageId, parentId } = ctx
  if (!content.trim() && messageType === "text") return undefined

  const { config, client, feishuClient, log, directory } = deps
  const query = directory ? { directory } : undefined
  const sessionKey = buildSessionKey(chatType, chatType === "p2p" ? senderId : chatId)

  // /new：显式会话重置，与 single 模式一致。
  if (shouldReply && messageType === "text" && content.trim() === "/new") {
    invalidateSession(sessionKey)
    const freshSession = await getOrCreateSession(client, sessionKey, directory)
    registerSessionChat(freshSession.id, chatId, chatType)
    clearNudge(freshSession.id)
    clearRetryAttempts(sessionKey)
    const ack = await sender.sendTextMessage(
      feishuClient,
      chatId,
      `✅ 已创建新会话（${freshSession.id}）。请继续发送你的问题。`,
      log,
    )
    if (!ack.ok) {
      log("error", "发送 /new 确认消息失败", {
        chatId,
        sessionKey,
        sessionId: freshSession.id,
        error: ack.error ?? "unknown",
      })
    }
    return undefined
  }

  const session = await getOrCreateSession(client, sessionKey, directory)
  registerSessionChat(session.id, chatId, chatType)
  traceLangfuseUser(session.id, senderId, log)
  clearNudge(session.id)

  const parts = await buildPromptParts(
    feishuClient, messageId, messageType, rawContent, content, chatType, senderId, log,
    config.maxResourceSize, parentId,
  )
  if (!parts.length) return undefined

  log("info", "收到用户消息", {
    sessionKey,
    sessionId: session.id,
    chatType,
    senderId,
    messageType,
    shouldReply,
    content,
    parts,
  })

  const baseBody = { parts }

  // 静默监听模式：只作为上下文送入 OpenCode，不给用户任何可见回复。
  if (!shouldReply) {
    try {
      await client.session.promptAsync({
        path: { id: session.id },
        query,
        body: { ...baseBody, noReply: true },
      })
    } catch (err) {
      log("error", "静默转发失败", {
        error: err instanceof Error ? err.message : String(err),
      })
    }
    return undefined
  }

  const run = createReplyRun({ sessionId: session.id, sessionKey, chatId, chatType })
  let latestSnapshot: AssistantSnapshot = { text: "", reasoning: "" }
  let timedOut = false
  const activeSessionId = session.id

  // 时间线管理器：依赖 CardKit，创建失败时降级为日志（无卡但仍完成主链路）。
  let manager: TimelineManager | undefined
  if (deps.cardkit) {
    manager = new TimelineManager(deps.cardkit, feishuClient, chatId, log, {
      runId: run.runId,
      sessionId: session.id,
      userText: content,
    })
    // 用户消息确认卡：立即展示"已收到，开始处理"，让用户感知任务已进入处理。
    await manager.ensureUserMessageCard(content)
  }

  // 注册 pending 上下文。
  // timeline 模式一个 run 可能跨多个 assistant message（多轮 thinking/tool 各属不同
  // messageID），因此放宽首条 messageID 锁；run 隔离由 session-queue FIFO（L4）保证。
  registerPending(activeSessionId, {
    placeholderId: "",
    feishuClient,
    mirrorTextToMessage: false,
    allowAnyMessageId: true,
  })

  // 订阅 action-bus：SSE 事件驱动时间线卡片。
  let cardUnsub: (() => void) | undefined
  cardUnsub = subscribe(activeSessionId, async (action) => {
    if (!manager) return
    switch (action.type) {
      case "reasoning-updated":
        log("info", "timeline.reasoning-updated", { partID: action.partID, len: action.text.length, time: action.time, manager: !!manager })
        if (manager) await manager.ensureThinkingCard(action.partID, action.text, action.time)
        break
      case "tool-state-changed":
        log("info", "timeline.tool-state-changed", { callID: action.callID, tool: action.tool, state: action.state, time: action.time, manager: !!manager })
        if (manager) await manager.ensureToolCard(action.callID, action.tool, action.state, action.input, action.output, action.time)
        break
      case "text-updated":
        // 过渡叙述文本：独立卡片展示（工具调用前后的过程性文字），不带状态字样。
        log("info", "timeline.text-updated", { partID: action.partID, len: action.fullText?.length ?? 0, time: action.time, preview: action.fullText?.slice(0, 80), manager: !!manager })
        if (action.fullText && action.partID && manager) await manager.ensureTextCard(action.partID, action.fullText, action.time)
        break
      case "permission-requested":
        if (deps.interactiveDeps) {
          handlePermissionRequested(action.request, chatId, deps.interactiveDeps, chatType, action.sessionId)
        }
        break
      case "question-requested":
        if (deps.interactiveDeps) {
          handleQuestionRequested(action.request, chatId, deps.interactiveDeps, chatType, action.sessionId)
        }
        break
      case "details-updated":
      case "session-idle":
      case "assistant-meta-updated":
        break
    }
  })

  // 轮询快照：文本驱动最终答复卡；reasoning 由 SSE 实时驱动，此处不重复建卡。
  const handleSnapshot = async (snapshot: AssistantSnapshot): Promise<void> => {
    latestSnapshot = snapshot
    if (snapshot.text && manager) {
      await manager.ensureFinalCard(snapshot.text)
    }
  }

  const poll = (
    currentClient: OpencodeClient,
    currentSessionId: string,
    pollOptions: {
      timeout?: number
      pollInterval: number
      stablePolls: number
      query?: { directory: string }
      signal?: AbortSignal
      baseline?: AssistantSnapshot
      idleAfterVersion?: number
    },
  ) => pollForResponse(currentClient, currentSessionId, {
    ...pollOptions,
    onSnapshot: handleSnapshot,
    onTimedOut: () => {
      timedOut = true
    },
  })

  try {
    clearSessionError(session.id)

    const { data: baselineMessages } = await client.session.messages({ path: { id: session.id }, query }).catch(() => ({ data: undefined }))
    const baseline = extractLastAssistantSnapshot(baselineMessages ?? [])
    log("info", "baseline captured", {
      sessionKey,
      sessionId: session.id,
      fetchSuccess: baselineMessages !== undefined,
      hasBaseline: baseline.text.length > 0 || baseline.reasoning.length > 0,
      textLen: baseline.text.length,
      reasoningLen: baseline.reasoning.length,
    })

    const idleAfterVersion = getSessionIdleVersion(session.id)
    await client.session.promptAsync({
      path: { id: session.id },
      query,
      body: baseBody,
    })

    const finalText = await poll(client, session.id, {
      timeout: config.timeout,
      pollInterval: config.pollInterval,
      stablePolls: config.stablePolls,
      query,
      signal: mergeAbortSignals([signal, getRunAbortSignal(run.runId)]),
      baseline,
      idleAfterVersion,
    })

    log("info", "模型响应完成", {
      sessionKey,
      sessionId: session.id,
      output: finalText || "(empty)",
    })

    clearRetryAttempts(sessionKey)
    const terminalState: "completed" | "timed_out" = timedOut ? "timed_out" : "completed"
    completeReplyRun(run.runId, terminalState)
    await manager?.finalize(
      terminalState,
      terminalConclusionFor(
        terminalState,
        finalText || latestSnapshot.text || (timedOut ? "⚠️ 响应超时" : undefined),
      ),
    )
  } catch (err) {
    const currentRunState = getRunByRunId(run.runId)?.state
    if (err instanceof Error && err.name === "AbortError" && currentRunState === "aborting") {
      completeReplyRun(run.runId, "aborted")
      await manager?.finalize("aborted", terminalConclusionFor("aborted", latestSnapshot.text || undefined))
      return
    }

    const sessionError = extractSessionError(err, session.id)
    const rawForClassify = sessionError?.raw ?? err
    const pluginError = classify(rawForClassify)
    log("info", "error.classified", toLog(pluginError))

    await matchPluginError(pluginError, {
      SessionPoisoned: async (e) => {
        log("error", "检测到 session 历史数据中毒", {
          sessionKey, oldSessionId: session.id, rule: e.rule,
        })

        let recovered = false
        if (deps.v2Client) {
          const cleaned = await findAndCleanPoisonedMessage({
            v2Client: deps.v2Client,
            sessionId: session.id,
            rule: e.rule,
            directory,
            log,
          })
          if (cleaned) {
            timedOut = false
            clearSessionError(session.id)
            try {
              const idleAfterVersion = getSessionIdleVersion(session.id)
              await client.session.promptAsync({
                path: { id: session.id },
                query,
                body: baseBody,
              })
              const finalText = await poll(client, session.id, {
                timeout: config.timeout, pollInterval: config.pollInterval, stablePolls: config.stablePolls,
                query,
                signal: mergeAbortSignals([signal, getRunAbortSignal(run.runId)]),
                idleAfterVersion,
              })
              const terminalState: "completed" | "timed_out" = timedOut ? "timed_out" : "completed"
              completeReplyRun(run.runId, terminalState)
              await manager?.finalize(
                terminalState,
                terminalConclusionFor(
                  terminalState,
                  finalText || latestSnapshot.text || (timedOut ? "⚠️ 响应超时" : undefined),
                ),
              )
              recovered = true
              log("info", "session 中毒恢复成功（已删除不兼容消息）", {
                sessionKey, sessionId: session.id, rule: e.rule,
              })
            } catch (recoveryErr) {
              log("error", "删除中毒消息后重发 prompt 失败，降级到新建 session", {
                sessionKey, rule: e.rule,
                error: recoveryErr instanceof Error ? recoveryErr.message : String(recoveryErr),
              })
            }
          }
        }

        if (!recovered) {
          invalidateSession(sessionKey)
          completeReplyRun(run.runId, "failed")
          await manager?.finalize(
            "failed",
            terminalConclusionFor("failed", "⚠️ 会话历史包含不兼容数据，已自动重置。请重新发送消息。"),
          )
        }
      },

      ModelUnavailable: async (e) => {
        timedOut = false
        const recovery = await tryModelRecovery({
          pluginError: e, sessionId: session.id, sessionKey, client, directory,
          parts,
          timeout: config.timeout,
          pollInterval: config.pollInterval,
          stablePolls: config.stablePolls,
          query,
          signal: mergeAbortSignals([signal, getRunAbortSignal(run.runId)]),
          log,
          poll,
        })

        if (recovery.recovered) {
          const terminalState: "completed" | "timed_out" = timedOut ? "timed_out" : "completed"
          completeReplyRun(run.runId, terminalState)
          await manager?.finalize(
            terminalState,
            terminalConclusionFor(
              terminalState,
              recovery.text || latestSnapshot.text || (timedOut ? "⚠️ 响应超时" : undefined),
            ),
          )
          return
        }
        completeReplyRun(run.runId, "failed")
        await manager?.finalize(
          "failed",
          terminalConclusionFor(
            "failed",
            latestSnapshot.text || ("❌ " + (sessionError?.message ?? pluginError.original)),
          ),
        )
      },

      ContextOverflow: async (e) => {
        log("warn", "上下文溢出", { sessionKey, providerID: e.providerID })
        completeReplyRun(run.runId, "failed")
        await manager?.finalize(
          "failed",
          terminalConclusionFor("failed", "⚠️ 对话历史过长。请开始新对话（/new 或直接在新会话里发消息）。"),
        )
      },

      Unauthorized: async (e) => {
        log("error", "provider 认证失败", { sessionKey, providerID: e.providerID })
        completeReplyRun(run.runId, "failed")
        await manager?.finalize(
          "failed",
          terminalConclusionFor("failed", "⚠️ 模型 provider 认证失败，请联系管理员检查 API key。"),
        )
      },

      UnknownUpstream: async (e) => {
        const thrownError = err instanceof Error ? err.message : String(err)
        const errorMessage = sessionError?.message || thrownError
        log("error", "对话处理失败", {
          sessionId: session.id, sessionKey, chatType,
          hint: e.hint,
          error: thrownError,
        })
        completeReplyRun(run.runId, "failed")
        await manager?.finalize(
          "failed",
          terminalConclusionFor(
            "failed",
            latestSnapshot.text || ("❌ " + errorMessage),
          ),
        )
      },
    })
  } finally {
    if (cardUnsub) cardUnsub()
    unregisterPending(activeSessionId)
  }
}
