"""/ws 指令处理 + 工作区/会话选择卡片。

指令均带聊天窗口上下文（chat_id）：
    use / unbind 直接作用于当前窗口的绑定。
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from .config import BridgeConfig
from .procman import ProcManager
from .state import BridgeState


class CommandHandler:
    def __init__(self, config: BridgeConfig, state: BridgeState, procman: ProcManager, log: logging.Logger):
        self.config = config
        self.state = state
        self.procman = procman
        self.log = log
        self._flush_impl = None  # larkgw.flush_pending 注入

    def set_flush_impl(self, impl) -> None:  # noqa: ANN001
        self._flush_impl = impl

    # ────────────── 指令解析 ──────────────

    def handle(self, text: str, operator_id: str, chat_id: str) -> str:
        """处理 /ws 指令，返回回复文本。"""
        prefix = self.config.command_prefix.lower()
        body = text.strip()[len(prefix):].strip()
        parts = body.split()
        if not parts or parts[0].lower() in ("help", "?", "？"):
            return self._help()

        cmd = parts[0].lower()
        args = parts[1:]

        if not self._is_admin(operator_id):
            return "⛔ 你没有权限使用该指令"

        if cmd == "list":
            return self._list()
        if cmd == "use" and args:
            return self._use(args[0], chat_id)
        if cmd == "open" and args:
            return self._open(args[0])
        if cmd == "close" and args:
            return self._close(args[0])
        if cmd == "new" and args:
            return self._new(" ".join(args))
        if cmd == "unbind":
            return self._unbind(chat_id)
        return f"❓ 未知指令。输入 {self.config.command_prefix} help 查看用法"

    def _help(self) -> str:
        return "\n".join([
            "🗂 工作区指令：",
            f"{self.config.command_prefix} list — 列出所有工作区",
            f"{self.config.command_prefix} use <n> — 切换当前窗口到工作区 n",
            f"{self.config.command_prefix} open <n> — 启动工作区 n 的 opencode",
            f"{self.config.command_prefix} close <n> — 停止工作区 n 的 opencode",
            f"{self.config.command_prefix} new <路径> — 注册并启动新工作区",
            f"{self.config.command_prefix} unbind — 解绑当前窗口",
        ])

    def _is_admin(self, operator_id: str) -> bool:
        return not self.config.admins or operator_id in self.config.admins

    def _resolve_workspace(self, ref: str) -> Tuple[Optional[str], Optional[str]]:
        """编号（1 起，list 输出顺序）或路径子串 → 工作区路径。

        可选范围 = 配置的工作区 ∪ 当前已连接的实例。
        """
        all_ws = [w.path for w in self.config.workspaces]
        for ws_path in self.state.by_workspace:
            if ws_path not in all_ws:
                all_ws.append(ws_path)
        if ref.isdigit():
            idx = int(ref) - 1
            if 0 <= idx < len(all_ws):
                return all_ws[idx], None
            return None, f"❌ 编号超范围: {ref}（1-{len(all_ws)}）"
        matches = [w for w in all_ws if ref in w]
        if len(matches) == 1:
            return matches[0], None
        if not matches:
            return None, f"❌ 找不到工作区: {ref}"
        return None, "❌ 多个匹配，请用编号:\n" + self._list()

    def _list(self) -> str:
        configured = [w.path for w in self.config.workspaces]
        all_ws = list(configured)
        for ws_path in self.state.by_workspace:
            if ws_path not in all_ws:
                all_ws.append(ws_path)
        alive = {w for w in all_ws if self.state.workspace_alive(w)}
        ordered = [w for w in all_ws if w in alive] + [w for w in all_ws if w not in alive]
        lines = []
        for i, w in enumerate(ordered, 1):
            if w in alive:
                mark = "🟢 运行中" if w in configured else "🟢 运行中（未配置）"
            else:
                mark = "⚪ 未连接"
            lines.append(f"{i}. {w} — {mark}")
        return "\n".join(lines)

    def _use(self, ref: str, chat_id: str) -> str:
        workspace, err = self._resolve_workspace(ref)
        if err or workspace is None:
            return err or "❌ 未知错误"
        if not self.state.workspace_alive(workspace):
            return f"⚠️ 工作区未运行，先 {self.config.command_prefix} open"
        self.state.set_binding(chat_id, workspace, None)
        self.log.info("窗口绑定切换: chat=%s -> %s", chat_id, workspace)
        return f"✅ 当前窗口已切换到 {workspace}"

    def _open(self, ref: str) -> str:
        workspace, err = self._resolve_workspace(ref)
        if err or workspace is None:
            return err or "❌ 未知错误"
        wcfg = next((w for w in self.config.workspaces if w.path == workspace), None)
        import asyncio
        task = asyncio.get_event_loop().create_task(
            self._open_task(workspace, wcfg.command if wcfg else None))
        task.add_done_callback(self._notify_task_result)
        self._task_chats[id(task)] = None  # 无聊天上下文，结果仅落日志
        return f"⏳ 正在启动 {workspace} …"

    async def _open_task(self, workspace: str, command: Optional[str]) -> str:
        return await self.procman.start(workspace, command)

    def _close(self, ref: str) -> str:
        workspace, err = self._resolve_workspace(ref)
        if err or workspace is None:
            return err or "❌ 未知错误"
        import asyncio
        task = asyncio.get_event_loop().create_task(self.procman.stop(workspace))
        task.add_done_callback(self._notify_task_result)
        self._task_chats[id(task)] = None
        return f"⏳ 正在停止 {workspace} …"

    def _notify_task_result(self, task: "asyncio.Task[str]") -> None:
        self._task_chats.pop(id(task), None)
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            self.log.error("异步操作失败: %s", exc)
        else:
            self.log.info("异步操作完成: %s", task.result())

    _task_chats: Dict[int, Optional[str]] = {}

    def _new(self, path: str) -> str:
        import os
        path = os.path.abspath(os.path.expanduser(path))
        if not os.path.isdir(path):
            return f"❌ 目录不存在: {path}"
        existing = [w.path for w in self.config.workspaces]
        if path in existing:
            return self._open(str(existing.index(path) + 1))
        from .config import WorkspaceConfig
        self.config.workspaces.append(WorkspaceConfig(path=path))
        # 持久化新工作区到 bridge.json 不做（保持配置文件权威），仅运行时生效
        idx = [w.path for w in self.config.workspaces].index(path) + 1
        return self._open(str(idx)) + f"\nℹ️ 新工作区编号 {idx}（重启后需写入 bridge.json 保留）"

    def _unbind(self, chat_id: str) -> str:
        if self.state.remove_binding(chat_id):
            return "✅ 已解绑当前窗口；下次发消息将重新弹出选择卡片"
        return "ℹ️ 当前窗口没有绑定"

    # ────────────── 选择卡片 ──────────────

    def build_pick_card(self, chat_id: str, sessions: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """构建「选工作区 + 选会话」的 form 卡片。

        结构与插件 `buildCardFromDSL`（send-card.ts）产出对齐：
        select_static 组件 + form 容器 + submit 按钮命名约定 btn_submit_<formName>。

        sessions: 插件回传的 OpenCode session 列表（最新在前），
        每项 {id, title?, updateTime?}。为空时只给「新会话」选项。
        """
        # 工作区下拉 = 配置的工作区 ∪ 当前已连接的实例（未配置的在线实例也可选）
        configured = [w.path for w in self.config.workspaces]
        all_ws = list(configured)
        for ws_path in self.state.by_workspace:
            if ws_path not in all_ws:
                all_ws.append(ws_path)
        alive = [w for w in all_ws if self.state.workspace_alive(w)]
        dead = [w for w in all_ws if w not in alive]
        options: List[Dict[str, Any]] = []
        for w in alive:
            mark = "" if w in configured else "（未配置·在线）"
            options.append({"text": {"tag": "plain_text", "content": f"🟢 {w}{mark}"}, "value": w})
        for w in dead:
            options.append({"text": {"tag": "plain_text", "content": f"⚪ {w}（未运行）"}, "value": w})

        # 会话下拉：默认复用最近一个会话；始终提供新会话选项。
        session_options: List[Dict[str, Any]] = []
        for s in (sessions or [])[:20]:
            title = (s.get("title") or s["id"]).strip()
            label = f"{title[:40]}（{s['id'][:14]}…）" if title != s["id"] else s["id"][:48]
            session_options.append({"text": {"tag": "plain_text", "content": label}, "value": s["id"]})
        default_session_value = session_options[0]["value"] if session_options else "__new__"
        session_options.append({"text": {"tag": "plain_text", "content": "🆕 新会话"}, "value": "__new__"})

        return {
            "schema": "2.0",
            "config": {"update_multi": True},
            "header": {"title": {"tag": "plain_text", "content": "选择目标工作区"}, "template": "blue"},
            "body": {
                "elements": [
                    {
                        "tag": "form",
                        "name": "ws_pick",
                        "direction": "vertical",
                        "elements": [
                            {"tag": "markdown", "content": "**工作区**"},
                            {
                                "tag": "select_static",
                                "name": "workspace",
                                "placeholder": {"tag": "plain_text", "content": "请选择工作区"},
                                "options": options or [{"text": {"tag": "plain_text", "content": "（无可用工作区）"}, "value": "_none_"}],
                            },
                            {"tag": "markdown", "content": "**会话**（默认复用最近会话）"},
                            {
                                "tag": "select_static",
                                "name": "session_choice",
                                "placeholder": {"tag": "plain_text", "content": "选择会话"},
                                "options": session_options,
                            },
                            {"tag": "markdown", "content": "**新工作区路径**（选填，填了则忽略上面的工作区下拉）"},
                            {
                                "tag": "input",
                                "name": "new_workspace",
                                "placeholder": {"tag": "plain_text", "content": "如 /home/me/new-project，留空不新建"},
                                "default_value": "",
                            },
                            {
                                "tag": "button",
                                "name": "btn_submit_ws_pick",
                                "text": {"tag": "plain_text", "content": "确认选择"},
                                "type": "primary",
                                "form_action_type": "submit",
                            },
                            {
                                "tag": "markdown",
                                "content": "💡 未运行的工作区会自动启动；排队消息会在绑定后自动补投。",
                            },
                        ],
                    },
                ],
            },
        }

    async def handle_pick_submit(self, chat_id: str, form_value: Dict[str, Any], operator_id: str) -> str:
        """ws_pick 表单提交：新建/启动工作区 → 绑定 → 补投排队消息。

        返回结果消息文本（由调用方发到窗口；toast 3 秒窗口等不了启动耗时）。
        """
        self.log.info("ws_pick 表单提交: chat=%s operator=%s formValue=%s", chat_id, operator_id, form_value)

        if not self._is_admin(operator_id):
            return "⛔ 你没有权限使用该操作"

        workspace = str(form_value.get("workspace", "")).strip()
        session_choice = str(form_value.get("session_choice", "")).strip()
        new_workspace = str(form_value.get("new_workspace", "")).strip()

        # ── 新工作区：注册 + 启动 + 等插件连接（最多 15s，toast 窗口 3s 内先返回）──
        if new_workspace:
            import os
            path = os.path.abspath(os.path.expanduser(new_workspace))
            if not os.path.isdir(path):
                return f"❌ 目录不存在: {path}"
            existing = [w.path for w in self.config.workspaces]
            if path not in existing:
                from .config import WorkspaceConfig
                self.config.workspaces.append(WorkspaceConfig(path=path))
                self.log.info("新工作区已注册: %s（重启后需写入 bridge.json 持久化）", path)
            workspace = path

        if not workspace or workspace == "_none_":
            return "❌ 没有可选的工作区"

        if not self.state.workspace_alive(workspace):
            wcfg = next((w for w in self.config.workspaces if w.path == workspace), None)
            await self.procman.start(workspace, wcfg.command if wcfg else None)
            # 等插件连接（opencode 启动 + 插件 hello 需要几秒）
            for _ in range(30):
                if self.state.workspace_alive(workspace):
                    break
                await asyncio.sleep(0.5)
            if not self.state.workspace_alive(workspace):
                return "⚠️ 工作区已启动但插件尚未连接，请稍后再发一条消息重试"

        # ── 会话选择：__new__ = 不绑定具体 session（插件默认新建/复用逻辑）；否则绑定指定 session ──
        session_id = None if (not session_choice or session_choice == "__new__") else session_choice
        if self._flush_impl:
            await self._flush_impl(chat_id, workspace, session_id)
        return "ok"  # 成功消息由 flush_pending 发送（含绑定详情）
