"""运行时状态：实例注册表、聊天窗口绑定、待选择排队消息。

持久化只覆盖「窗口绑定」；实例注册表和排队消息是易失的，
bridge 重启后由插件重新 hello、消息自然重投。
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class PluginConnection:
    """一个已注册的插件连接（对应一个 OpenCode 实例）。"""

    workspace: str
    """工作目录；同一 bridge 不允许重复注册。"""
    conn_id: str
    """连接 ID（分配的短 ID，用于 /ws 列表展示和操作）。"""
    send_queue: "asyncio.Queue[Dict[str, Any]]"
    """向该插件推送消息的发送队列。"""
    pid: Optional[int] = None
    response_waiters: Dict[str, asyncio.Future] = field(default_factory=dict)
    """respond_sessions 等请求应答的等待器。"""

    def is_alive(self) -> bool:
        return not self.send_queue._closed if hasattr(self.send_queue, "_closed") else True


@dataclass
class WindowBinding:
    """一个飞书聊天窗口的当前指向。"""

    workspace: str
    """绑定的工作区路径。"""
    session_id: Optional[str] = None
    """可选：绑定到该工作区的具体 OpenCode session；None 表示用插件默认映射。"""
    bound_at: float = field(default_factory=time.time)


@dataclass
class PendingPick:
    """未绑定窗口的待选择状态：排队消息 + 选择卡片。"""

    chat_id: str
    chat_type: str
    queued: List[Dict[str, Any]] = field(default_factory=list)
    """排队等待的飞书消息（已折叠成插件可直接消费的 dict）。"""
    card_message_id: Optional[str] = None
    expire_at: float = 0.0
    """选择超时时间戳；0 表示未开始计时。"""
    task: Optional[asyncio.Task] = None
    """超时看护任务。"""


class BridgeState:
    """全部易变状态 + 绑定表持久化。所有方法非线程安全（均在事件循环内调用）。"""

    def __init__(self, state_file: str, log: Optional[logging.Logger] = None):
        self.state_file = state_file
        self.log = log or logging.getLogger("feishu_bridge")
        self.connections: Dict[str, PluginConnection] = {}
        """conn_id → 连接。"""
        self.by_workspace: Dict[str, PluginConnection] = {}
        """workspace → 连接（注册时保证唯一）。"""
        self.bindings: Dict[str, WindowBinding] = {}
        """chatId → 绑定。"""
        self.pending_picks: Dict[str, PendingPick] = {}
        """chatId → 待选择状态。"""
        self._next_seq = 1
        self._load()

    # ────────────── 实例注册 ──────────────

    def next_conn_id(self) -> str:
        conn_id = f"ws{self._next_seq}"
        self._next_seq += 1
        return conn_id

    def register(self, conn: PluginConnection) -> None:
        self.connections[conn.conn_id] = conn
        self.by_workspace[conn.workspace] = conn
        self.log.info("插件已注册: %s -> %s (pid=%s)", conn.conn_id, conn.workspace, conn.pid)

    def unregister(self, conn_id: str) -> Optional[PluginConnection]:
        conn = self.connections.pop(conn_id, None)
        if conn and self.by_workspace.get(conn.workspace) is conn:
            del self.by_workspace[conn.workspace]
            self.log.info("插件已断开: %s (%s)", conn.conn_id, conn.workspace)
        return conn

    # ────────────── 绑定 ──────────────

    def get_binding(self, chat_id: str) -> Optional[WindowBinding]:
        return self.bindings.get(chat_id)

    def set_binding(self, chat_id: str, workspace: str, session_id: Optional[str]) -> None:
        self.bindings[chat_id] = WindowBinding(workspace=workspace, session_id=session_id)
        self.save()

    def remove_binding(self, chat_id: str) -> bool:
        if chat_id in self.bindings:
            del self.bindings[chat_id]
            self.save()
            return True
        return False

    def workspace_alive(self, workspace: str) -> bool:
        return workspace in self.by_workspace

    # ────────────── 持久化 ──────────────

    def _load(self) -> None:
        if not os.path.exists(self.state_file):
            return
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            for chat_id, b in raw.get("bindings", {}).items():
                self.bindings[chat_id] = WindowBinding(
                    workspace=b["workspace"],
                    session_id=b.get("sessionId"),
                    bound_at=b.get("boundAt", 0.0),
                )
            self._next_seq = int(raw.get("nextSeq", 1))
            self.log.info("已恢复绑定表: %d 条 (来自 %s)", len(self.bindings), self.state_file)
        except Exception as e:  # noqa: BLE001
            self.log.error("绑定表加载失败（忽略，使用空状态）: %s", e)

    def save(self) -> None:
        data = {
            "bindings": {
                chat_id: {
                    "workspace": b.workspace,
                    "sessionId": b.session_id,
                    "boundAt": b.bound_at,
                }
                for chat_id, b in self.bindings.items()
            },
            "nextSeq": self._next_seq,
        }
        try:
            tmp = self.state_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.state_file)
        except Exception as e:  # noqa: BLE001
            self.log.error("绑定表保存失败: %s", e)
