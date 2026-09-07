"""opencode 进程管理：按工作区拉起 / 停止 / 查询实例。"""

import asyncio
import logging
import os
import signal
import socket
from typing import Dict, Optional

from .config import BridgeConfig


class ManagedProcess:
    def __init__(self, workspace: str, port: Optional[int], pid: int, process: asyncio.subprocess.Process):
        self.workspace = workspace
        self.port = port
        self.pid = pid
        self.process = process


class ProcManager:
    """spawn `opencode serve` 子进程。

    只负责进程生命周期；连接由插件主动建立（opencode 启动后插件随其拉起）。
    """

    def __init__(self, config: BridgeConfig, log: logging.Logger):
        self.config = config
        self.log = log
        self.processes: Dict[str, ManagedProcess] = {}
        self._port_lock = asyncio.Lock()
        self._next_port = config.base_port

    def _alloc_port(self) -> int:
        port = self._next_port
        self._next_port += 1
        return port

    async def _port_free(self, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.3)
            try:
                s.bind(("127.0.0.1", port))
                return True
            except OSError:
                return False

    async def start(self, workspace: str, command: Optional[str] = None) -> str:
        """启动一个工作区的 opencode。返回用户可读状态。"""
        if not os.path.isdir(workspace):
            return f"❌ 工作区不存在: {workspace}"
        if workspace in self.processes:
            existing = self.processes[workspace]
            if existing.process.returncode is None:
                return f"ℹ️ 工作区已在运行: {workspace} (pid={existing.pid})"

        async with self._port_lock:
            port = None
            for _ in range(20):
                candidate = self._alloc_port()
                if await self._port_free(candidate):
                    port = candidate
                    break
        if port is None:
            return "❌ 无可用端口（basePort 起连续 20 个均被占用）"

        if command:
            cmd = command.replace("{port}", str(port)).replace("{path}", workspace)
        else:
            cmd = f"opencode serve --port {port} --hostname 127.0.0.1"

        try:
            process = await asyncio.create_subprocess_shell(
                cmd,
                cwd=workspace,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception as e:  # noqa: BLE001
            self.log.error("启动 opencode 失败: %s (%s)", workspace, e)
            return f"❌ 启动失败: {e}"

        self.processes[workspace] = ManagedProcess(workspace, port, process.pid, process)
        self.log.info("opencode 已启动: %s (pid=%s, port=%s, cmd=%s)", workspace, process.pid, port, cmd)
        return f"✅ 已启动 {workspace}\n(pid={process.pid}, port={port}) 等待插件连接…"

    async def stop(self, workspace: str) -> str:
        mp = self.processes.get(workspace)
        if not mp or mp.process.returncode is not None:
            self.processes.pop(workspace, None)
            return f"ℹ️ 工作区未在运行（bridge 未托管）: {workspace}"
        try:
            os.killpg(os.getpgid(mp.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            try:
                mp.process.terminate()
            except ProcessLookupError:
                pass
        try:
            await asyncio.wait_for(mp.process.wait(), timeout=8)
        except asyncio.TimeoutError:
            self.log.warning("SIGTERM 超时，强制 kill: %s", workspace)
            try:
                os.killpg(os.getpgid(mp.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        self.processes.pop(workspace, None)
        self.log.info("opencode 已停止: %s", workspace)
        return f"🛑 已停止 {workspace}"

    def status_line(self, workspace: str) -> str:
        mp = self.processes.get(workspace)
        if mp and mp.process.returncode is None:
            return f"运行中 (pid={mp.pid})"
        # 进程退出但未清理
        if mp:
            self.processes.pop(workspace, None)
        return "未运行"

    def stop_all(self) -> None:
        for workspace in list(self.processes.keys()):
            mp = self.processes[workspace]
            try:
                os.killpg(os.getpgid(mp.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError, OSError):
                pass
