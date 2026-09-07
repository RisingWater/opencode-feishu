"""bridge 配置模型。"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class WorkspaceConfig:
    """一个受管工作区：bridge 可按需拉起对应目录的 opencode 实例。"""

    path: str
    """工作目录（绝对路径）。"""
    command: Optional[str] = None
    """自定义启动命令；默认 `opencode serve --port <port>`。"""


@dataclass
class BridgeConfig:
    app_id: str
    app_secret: str
    host: str = "127.0.0.1"
    port: int = 8787
    """本地 WS 服务监听地址/端口（插件连接用）。"""
    token: Optional[str] = None
    """插件连接鉴权 token；不配则不做校验。"""
    admins: List[str] = field(default_factory=list)
    """允许使用 /ws 指令的 open_id 白名单；为空则所有用户可用。"""
    command_prefix: str = "/ws"
    """bridge 指令前缀（大小写不敏感）。"""
    workspaces: List[WorkspaceConfig] = field(default_factory=list)
    """受管工作区列表（/ws open <n> 的目标）。"""
    base_port: int = 14000
    """opencode serve 自动分配端口的起始值。"""
    state_file: str = "bridge-state.json"
    """绑定表持久化文件路径（相对 cwd）。"""
    pick_timeout_seconds: int = 300
    """选择卡片等待用户点击的超时；超时后丢弃排队的消息。"""
    queue_max_messages: int = 20
    """每个聊天窗口排队等待选择的最大消息数。"""
    default_workspace: Optional[str] = None
    """可选：默认工作区路径（配置过的窗口自动绑定它）。"""

    @staticmethod
    def from_dict(raw: Dict[str, Any]) -> "BridgeConfig":
        if "appId" not in raw or "appSecret" not in raw:
            raise ValueError("bridge.json 缺少 appId / appSecret")
        workspaces = [
            WorkspaceConfig(
                path=w["path"],
                command=w.get("command"),
            )
            for w in raw.get("workspaces", [])
        ]
        cfg = BridgeConfig(
            app_id=raw["appId"],
            app_secret=raw["appSecret"],
            host=raw.get("host", "127.0.0.1"),
            port=int(raw.get("port", 8787)),
            token=raw.get("token"),
            admins=list(raw.get("admins", [])),
            command_prefix=raw.get("commandPrefix", "/ws"),
            workspaces=workspaces,
            base_port=int(raw.get("basePort", 14000)),
            state_file=raw.get("stateFile", "bridge-state.json"),
            pick_timeout_seconds=int(raw.get("pickTimeoutSeconds", 300)),
            queue_max_messages=int(raw.get("queueMaxMessages", 20)),
            default_workspace=raw.get("defaultWorkspace"),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.app_id.startswith("cli_"):
            raise ValueError("appId 应以 cli_ 开头")
        seen: set = set()
        for w in self.workspaces:
            if w.path in seen:
                raise ValueError(f"工作区路径重复: {w.path}")
            seen.add(w.path)
