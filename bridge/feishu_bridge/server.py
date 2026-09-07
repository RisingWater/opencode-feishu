"""本地 WS 服务：插件连接入口。

协议（JSON 行，方向标注）：
    插件 → bridge:
        {type: "hello", protoVersion: 1, workspace, pid, token?}
        {type: "rpc", id, method, params}
        {type: "ping"}
        {type: "respond_sessions", reqId, sessions}
    bridge → 插件:
        {type: "hello_ok"} / {type: "hello_err", error}
        {type: "event", eventType, payload}        # 飞书事件转发
        {type: "bind", chatId, sessionId|null}     # 会话绑定控制
        {type: "list_sessions", reqId}             # 请求 session 列表
        {type: "rpc_result", id, ok, result|error}
        {type: "pong"}
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional

import websockets

from .api import RpcError
from .config import BridgeConfig
from .larkgw import FeishuGateway
from .procman import ProcManager
from .state import BridgeState, PluginConnection

PROTO_VERSION = 1
PING_TIMEOUT = 30.0
SESSION_LIST_TIMEOUT = 5.0


class BridgeServer:
    def __init__(
        self,
        config: BridgeConfig,
        state: BridgeState,
        procman: ProcManager,
        gateway: FeishuGateway,
        log: logging.Logger,
    ):
        self.config = config
        self.state = state
        self.procman = procman
        self.gateway = gateway
        self.log = log
        self._last_ping: Dict[str, float] = {}

    async def serve(self) -> None:
        try:
            async with websockets.serve(
                self._handle,
                self.config.host,
                self.config.port,
                ping_interval=20,
                ping_timeout=20,
            ):
                self.log.info("bridge WS 服务已监听 ws://%s:%d", self.config.host, self.config.port)
                if self.config.host not in ("127.0.0.1", "localhost", "::1") and not self.config.token:
                    self.log.warning(
                        "监听地址 %s 非回环地址且未配置 token——任何能访问该端口的人都可以"
                        "注册插件实例、读取消息内容并调用飞书 API！强烈建议在 bridge.json 配置 token。",
                        self.config.host,
                    )
                await asyncio.Future()  # run forever
        except asyncio.CancelledError:
            raise
        except Exception:
            self.log.exception("bridge WS 服务启动失败（端口被占用？host/port 配置错误？）")
            raise

    # ────────────── 连接处理 ──────────────

    async def _handle(self, ws: Any) -> None:  # websockets.WebSocketServerProtocol
        peer = getattr(ws, "remote_address", ("?", 0))
        conn: Optional[PluginConnection] = None
        conn_id = ""
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                mtype = msg.get("type")

                if mtype == "hello" and conn is None:
                    conn_id, err = await self._register(ws, msg)
                    if err:
                        await ws.send(json.dumps({"type": "hello_err", "error": err}))
                        return
                    conn = self.state.connections[conn_id]
                    await ws.send(json.dumps({"type": "hello_ok"}))
                    # 注册成功后启动该连接的推送循环
                    asyncio.get_event_loop().create_task(self._push_loop(ws, conn))
                    # 重推该工作区的存量绑定：bridge 重启后绑定从 state 恢复，
                    # 但插件内存 override 是空的，必须补发 bind，否则消息会走默认 session 映射。
                    rebound = 0
                    for chat_id, binding in self.state.bindings.items():
                        if binding.workspace == conn.workspace:
                            await conn.send_queue.put({
                                "type": "bind",
                                "chatId": chat_id,
                                "sessionId": binding.session_id,
                            })
                            rebound += 1
                    if rebound:
                        self.log.info("已重推存量绑定: %s -> %d 条", conn.conn_id, rebound)
                    continue

                if conn is None:
                    await ws.send(json.dumps({"type": "hello_err", "error": "必须先发送 hello"}))
                    return

                if mtype == "ping":
                    self._last_ping[conn_id] = time.monotonic()
                    await ws.send(json.dumps({"type": "pong"}))
                elif mtype == "rpc":
                    asyncio.get_event_loop().create_task(self._handle_rpc(ws, conn, msg))
                elif mtype == "respond_sessions":
                    waiter = conn.response_waiters.pop(str(msg.get("reqId")), None)
                    if waiter and not waiter.done():
                        waiter.set_result(msg.get("sessions") or [])
        except (websockets.ConnectionClosed, asyncio.CancelledError):
            pass
        finally:
            if conn:
                for waiter in conn.response_waiters.values():
                    if not waiter.done():
                        waiter.set_result([])
                self.state.unregister(conn_id)

    async def _register(self, ws: Any, msg: Dict[str, Any]) -> Any:
        if msg.get("protoVersion") != PROTO_VERSION:
            return None, f"协议版本不兼容: 插件={msg.get('protoVersion')} bridge={PROTO_VERSION}"
        if self.config.token and msg.get("token") != self.config.token:
            return None, "token 校验失败"
        workspace = str(msg.get("workspace") or "").strip()
        if not workspace:
            return None, "缺少 workspace"
        if workspace in self.state.by_workspace:
            existing = self.state.by_workspace[workspace]
            return None, f"工作区已被连接 {existing.conn_id} 占用（重复注册？）"
        conn_id = self.state.next_conn_id()
        conn = PluginConnection(
            workspace=workspace,
            conn_id=conn_id,
            send_queue=asyncio.Queue(),
            pid=msg.get("pid"),
        )
        self.state.register(conn)
        self._last_ping[conn_id] = time.monotonic()
        return conn_id, None

    # ────────────── 推送 / RPC ──────────────

    async def _push_loop(self, ws: Any, conn: PluginConnection) -> None:
        """send_queue → ws。单条消息失败只丢该条并记日志，不让循环死掉。"""
        while True:
            msg = await conn.send_queue.get()
            try:
                payload = json.dumps(msg, ensure_ascii=False, default=str)
                await ws.send(payload)
                self.log.info("推送 -> %s: type=%s", conn.conn_id, msg.get("type"))
            except websockets.ConnectionClosed:
                self.log.info("推送中止（连接关闭）: %s", conn.conn_id)
                break
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                self.log.exception("推送失败（丢弃该条）: %s type=%s", conn.conn_id, msg.get("type"))

    async def _handle_rpc(self, ws: Any, conn: PluginConnection, msg: Dict[str, Any]) -> None:
        rpc_id = str(msg.get("id"))
        method = str(msg.get("method"))
        params = msg.get("params") or {}
        self.log.info("RPC <- %s (%s): %s", conn.conn_id, method, json.dumps(params, ensure_ascii=False)[:200])
        try:
            if method == "__list_sessions__":
                result = await self._request_sessions(conn)
            else:
                result = await self.gateway.rpc.dispatch(method, params)
            self.log.info("RPC -> %s (%s): ok=%s", conn.conn_id, method,
                          json.dumps(result, ensure_ascii=False)[:150])
            await ws.send(json.dumps({"type": "rpc_result", "id": rpc_id, "ok": True, "result": result}, ensure_ascii=False))
        except RpcError as e:
            await ws.send(json.dumps({"type": "rpc_result", "id": rpc_id, "ok": False, "error": str(e)}))
        except Exception as e:  # noqa: BLE001
            self.log.exception("RPC 执行失败: %s", method)
            await ws.send(json.dumps({"type": "rpc_result", "id": rpc_id, "ok": False, "error": str(e)}))

    async def _request_sessions(self, conn: PluginConnection) -> list:
        """向插件请求 session 列表（带超时）。"""
        req_id = f"ls-{time.monotonic_ns()}"
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        conn.response_waiters[req_id] = fut
        await conn.send_queue.put({"type": "list_sessions", "reqId": req_id})
        try:
            return await asyncio.wait_for(fut, timeout=SESSION_LIST_TIMEOUT)
        except asyncio.TimeoutError:
            conn.response_waiters.pop(req_id, None)
            return []

    async def list_sessions_for_workspace(self, workspace: str) -> list:
        """按工作区查询 session 列表；实例不在线返回空。"""
        conn = self.state.by_workspace.get(workspace)
        if not conn:
            return []
        try:
            return await self._request_sessions(conn)
        except Exception:  # noqa: BLE001
            self.log.exception("查询 session 列表失败: %s", workspace)
            return []
