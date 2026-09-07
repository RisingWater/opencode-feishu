"""feishu_bridge — 飞书 ↔ 多 OpenCode 工作区桥接服务。"""

from .api import RpcHandler
from .commands import CommandHandler
from .config import BridgeConfig, WorkspaceConfig
from .larkgw import FeishuGateway
from .procman import ProcManager
from .server import BridgeServer
from .state import BridgeState, PendingPick, PluginConnection, WindowBinding

__all__ = [
    "BridgeConfig",
    "WorkspaceConfig",
    "BridgeState",
    "PendingPick",
    "PluginConnection",
    "WindowBinding",
    "ProcManager",
    "CommandHandler",
    "RpcHandler",
    "FeishuGateway",
    "BridgeServer",
]
