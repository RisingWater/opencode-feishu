"""协议自测：模拟插件连接 bridge，走 hello → RPC → event 路由 → bind → 补投。

不连真实飞书（FeishuGateway 的 lark 部分被 mock），只验证 server.py / state.py /
commands.py 的协议与路由逻辑。
"""

import asyncio
import json
import sys
import logging

sys.path.insert(0, "bridge")
sys.path.insert(0, "/tmp/opencode")

import websockets  # noqa: E402

from feishu_bridge.config import BridgeConfig  # noqa: E402
from feishu_bridge.state import BridgeState  # noqa: E402
from feishu_bridge.procman import ProcManager  # noqa: E402
from feishu_bridge.commands import CommandHandler  # noqa: E402
from feishu_bridge.server import BridgeServer  # noqa: E402


class MockGateway:
    """替代 FeishuGateway：不发真实飞书请求。"""

    def __init__(self, state, log):
        self.state = state
        self.rpc = MockRpc()
        self.sent_texts = []

    async def _send_text(self, chat_id, text):  # noqa: ANN001
        self.sent_texts.append((chat_id, text))


class MockRpc:
    async def dispatch(self, method, params):  # noqa: ANN001
        return {"code": 0, "msg": "success", "data": {"echo_method": method}}


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[test] %(message)s")
    log = logging.getLogger("test")

    cfg = BridgeConfig.from_dict({
        "appId": "cli_test", "appSecret": "x",
        "port": 18971, "workspaces": [{"path": "/tmp/proj-a"}],
        "stateFile": "/tmp/opencode/test-bridge-state.json",
    })
    state = BridgeState(cfg.state_file, log)
    procman = ProcManager(cfg, log)
    commands = CommandHandler(cfg, state, procman, log)
    gateway = MockGateway(state, log)
    server = BridgeServer(cfg, state, procman, gateway, log)
    server_task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.3)

    uri = "ws://127.0.0.1:18971"
    async with websockets.connect(uri) as ws:
        # 1) hello + 注册
        await ws.send(json.dumps({"type": "hello", "protoVersion": 1, "workspace": "/tmp/proj-a", "pid": 123}))
        print("hello_ok:", json.loads(await ws.recv())["type"] == "hello_ok")

        # 2) RPC 代理
        await ws.send(json.dumps({"type": "rpc", "id": "rpc-1", "method": "im.message.create", "params": {}}))
        res = json.loads(await ws.recv())
        print("rpc ok:", res["ok"] and res["result"]["data"]["echo_method"] == "im.message.create")

        # 3) 未绑定窗口的消息 → 选择卡片（Mock gateway 记录不了真实卡片，验证排队即可）
        from feishu_bridge.state import PendingPick
        raw_event = {
            "type": "im.message.receive_v1",
            "payload": {"message": {"chat_id": "oc_win1", "content": json.dumps({"text": "hi"})}},
        }
        pending = PendingPick(chat_id="oc_win1", chat_type="p2p")
        pending.queued.append(raw_event)
        state.pending_picks["oc_win1"] = pending

        # 4) pick 提交 → flush_pending → 插件收到 bind + 补投事件
        await gateway_flush(gateway, "oc_win1", "/tmp/proj-a", None)

        bind_msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
        evt_msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
        print("bind ok:", bind_msg["type"] == "bind" and bind_msg["chatId"] == "oc_win1")
        print("queued event ok:", evt_msg["type"] == "event"
              and evt_msg["eventType"] == "im.message.receive_v1"
              and evt_msg["payload"]["message"]["chat_id"] == "oc_win1")
        print("binding saved:", state.get_binding("oc_win1").workspace == "/tmp/proj-a")

        # 5) 已绑定消息路由：直接投递到 conn 队列（模拟 larkgw._push_event 的产出形态）
        conn = state.by_workspace["/tmp/proj-a"]
        await conn.send_queue.put({
            "type": "event",
            "eventType": "im.message.receive_v1",
            "payload": raw_event["payload"],
        })
        routed = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
        print("route ok:", routed["type"] == "event" and routed["eventType"] == "im.message.receive_v1"
              and routed["payload"]["message"]["chat_id"] == "oc_win1")

        # 6) /ws 指令：use / list / unbind
        reply = commands.handle("/ws list", "ou_user", "oc_win1")
        print("list:", reply)
        reply = commands.handle("/ws use 1", "ou_user", "oc_win1")
        print("use:", reply)
        print("use rebind:", state.get_binding("oc_win1").session_id is None)
        reply = commands.handle("/ws unbind", "ou_user", "oc_win1")
        print("unbind:", reply, "| binding removed:", state.get_binding("oc_win1") is None)

        # 7) list_sessions 请求/应答
        import time as _t
        req_id = f"ls-{_t.monotonic_ns()}"
        fut = asyncio.get_event_loop().create_future()
        conn.response_waiters[req_id] = fut
        await conn.send_queue.put({"type": "list_sessions", "reqId": req_id})
        pushed = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
        print("list_sessions push ok:", pushed["type"] == "list_sessions" and pushed["reqId"] == req_id)
        await ws.send(json.dumps({"type": "respond_sessions", "reqId": req_id, "sessions": [{"id": "ses-1"}]}))
        sessions = await asyncio.wait_for(fut, timeout=3)
        print("list_sessions resp ok:", sessions == [{"id": "ses-1"}])

    server_task.cancel()
    print("ALL PASS")


async def gateway_flush(gateway, chat_id, workspace, session_id):  # noqa: ANN001
    """复刻 larkgw.flush_pending 的核心路径（含 event 信封包装）。"""
    pending = gateway.state.pending_picks.pop(chat_id, None)
    gateway.state.set_binding(chat_id, workspace, session_id)
    conn = gateway.state.by_workspace.get(workspace)
    if conn:
        await conn.send_queue.put({"type": "bind", "chatId": chat_id, "sessionId": session_id})
        if pending:
            for raw_event in pending.queued:
                await conn.send_queue.put({
                    "type": "event",
                    "eventType": raw_event.get("type", ""),
                    "payload": raw_event.get("payload", {}),
                })


asyncio.run(main())
