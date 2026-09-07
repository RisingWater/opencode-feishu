/**
 * bridge 会话绑定（chatId → sessionId override）与 session 列表查询。
 *
 * bridge 模式下，用户通过 bridge 的选择卡片把飞书聊天窗口绑定到
 * 某个具体 OpenCode session。插件收到 bridge 推送的 `bind` 控制消息后
 * 调用 `setSessionOverride()`；此后该 chat 的一切消息固定路由到该 session，
 * 直到 bridge 解绑（sessionId 为 null）。
 */
import type { OpencodeClient } from "@opencode-ai/sdk"

/** chatId → sessionId。值为 null 表示显式解绑。 */
const sessionOverrides = new Map<string, string | null>()

/**
 * 设置/解除 chatId 的 session 绑定。
 *
 * `sessionId === null` 表示解绑：回到插件默认的 sessionKey 映射逻辑。
 */
export function setSessionOverride(chatId: string, sessionId: string | null): void {
  if (sessionId === null) {
    sessionOverrides.delete(chatId)
  } else {
    sessionOverrides.set(chatId, sessionId)
  }
}

/**
 * 查询 chatId 当前绑定的 session；未绑定时返回 undefined。
 */
export function getSessionOverride(chatId: string): string | undefined {
  return sessionOverrides.get(chatId) ?? undefined
}

/**
 * 列出当前 OpenCode 实例中的用户会话。
 *
 * 供 bridge 的选择卡片使用：过滤掉子任务等非对话 session，
 * 按更新时间倒序返回（最新会话排最前）。
 */
export async function listOpenCodeSessions(
  client: OpencodeClient,
  directory?: string,
): Promise<Array<{ id: string; title?: string; updateTime?: number }>> {
  const query = directory ? { directory } : undefined
  const { data: sessions } = await client.session.list({ query })
  if (!Array.isArray(sessions)) return []

  return sessions
    // parentID 存在说明是子任务/子代理 session，不作为可选目标。
    .filter((s) => !s.parentID && s.id)
    .map((s) => ({
      id: s.id,
      title: s.title,
      updateTime: s.time?.updated ?? s.time?.created ?? 0,
    }))
    .sort((a, b) => (b.updateTime ?? 0) - (a.updateTime ?? 0))
}
