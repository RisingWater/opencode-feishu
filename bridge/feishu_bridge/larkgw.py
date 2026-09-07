"""飞书网关：lark-oapi WS 长连接 + 事件路由 + 指令拦截 + 选择卡片。

关键点：lark-oapi 1.7.3 的 `ws.Client._handle_data_frame` 会丢弃 CARD 帧，
本模块用子类补上：CARD 帧同样走 `_do_without_validation`，
从而拿到 `card.action.trigger` 回调（含 toast 返回能力）。
"""

import asyncio
import json
import logging
from http import HTTPStatus as _HTTPStatus
from typing import Any, Dict, List, Optional

import lark_oapi as lark
import lark_oapi.ws.client as lark_ws
from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageRequestBody,
    P2ImChatMemberBotAddedV1,
    P2ImMessageReceiveV1,
)
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    CallBackToast,
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)

from .api import RpcHandler
from .commands import CommandHandler
from .config import BridgeConfig
from .procman import ProcManager
from .state import BridgeState, PendingPick, PluginConnection


class CardAwareWsClient(lark_ws.Client):
    """补上 CARD 帧处理：card.action.trigger 回调返回 toast。"""

    async def _handle_data_frame(self, frame: Any) -> None:  # noqa: ANN401
        from lark_oapi.ws.client import (
            JSON,
            MessageType,
            Response,
            UTF_8,
            _get_by_key,
        )
        from lark_oapi.ws.const import (
            HEADER_MESSAGE_ID,
            HEADER_SEQ,
            HEADER_SUM,
            HEADER_TYPE,
        )
        import base64
        import time

        hs = frame.headers
        msg_id = _get_by_key(hs, HEADER_MESSAGE_ID)
        sum_ = int(_get_by_key(hs, HEADER_SUM) or 1)
        seq = int(_get_by_key(hs, HEADER_SEQ) or 0)
        type_ = _get_by_key(hs, HEADER_TYPE)

        pl = frame.payload
        if sum_ > 1:
            pl = self._combine(msg_id, sum_, seq, pl)
            if pl is None:
                return

        message_type = MessageType(type_)
        resp = Response(code=_HTTPStatus.OK)
        try:
            start = int(round(time.time() * 1000))
            if message_type in (MessageType.EVENT, MessageType.CARD):
                logging.getLogger("feishu_bridge").info(
                    "WS 帧: type=%s msgId=%s preview=%s",
                    message_type.value, msg_id, pl[:120].decode(UTF_8, errors="replace"))
                result = self._event_handler._do_without_validation(pl)
            else:
                return
            end = int(round(time.time() * 1000))
            if result is not None:
                resp.data = base64.b64encode(JSON.marshal(result).encode(UTF_8))
        except Exception as e:  # noqa: BLE001
            self.logger.error(self._fmt_log("handle message failed, message_type: {}, err: {}",
                                            message_type.value, e))
            resp = Response(code=_HTTPStatus.INTERNAL_SERVER_ERROR)

        frame.payload = JSON.marshal(resp).encode(UTF_8)
        await self._write_message(frame.SerializeToString())


class FeishuGateway:
    def __init__(
        self,
        config: BridgeConfig,
        state: BridgeState,
        procman: ProcManager,
        commands: CommandHandler,
        log: logging.Logger,
    ):
        self.config = config
        self.state = state
        self.procman = procman
        self.commands = commands
        self.log = log
        self.rpc = RpcHandler(self._make_client())
        self._ws_client: Optional[CardAwareWsClient] = None
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        self.server = None  # BridgeServer 引用，由 __main__ 注入（session 列表查询用）
        # 指令卡片提交 → 补投消息的实现注入（避免循环 import）
        commands.set_flush_impl(self.flush_pending)

    def _make_client(self) -> lark.Client:
        return lark.Client.builder() \
            .app_id(self.config.app_id) \
            .app_secret(self.config.app_secret) \
            .build()

    # ────────────── 生命周期 ──────────────

    def _sync_handler(self, coro_factory):
        """把 async 处理函数包装成同步函数。

        lark-oapi 1.7.3 全链路同步调用 handler；而 ws.Client 跑在 executor
        线程自己的 event loop 里。这里把协程调度回主 loop（bridge 的
        websockets 服务所在）后立刻返回，不阻塞 SDK 收包线程。
        """
        def run(*args):  # noqa: ANN002
            main_loop = self._main_loop
            if main_loop is None or main_loop.is_closed():
                self.log.error("主事件循环不可用，事件被丢弃")
                return None
            asyncio.run_coroutine_threadsafe(coro_factory(*args), main_loop)
            return None
        return run

    def _sync_card_handler(self, coro_factory):
        """卡片回调版同步包装：需同步等到结果（3 秒窗口内返回 toast）。"""
        def run(*args):  # noqa: ANN002
            main_loop = self._main_loop
            if main_loop is None or main_loop.is_closed():
                self.log.error("主事件循环不可用，卡片回调被丢弃")
                return P2CardActionTriggerResponse()
            fut = asyncio.run_coroutine_threadsafe(coro_factory(*args), main_loop)
            try:
                return fut.result(timeout=2.5)
            except Exception as e:  # noqa: BLE001
                self.log.error("卡片回调处理超时/失败: %s", e)
                return P2CardActionTriggerResponse()
        return run

    async def start(self) -> None:
        self._main_loop = asyncio.get_running_loop()
        handler = lark.EventDispatcherHandler.builder("", "") \
            .register_p2_im_message_receive_v1(self._sync_handler(lambda d: self._on_message_event(d))) \
            .register_p2_im_chat_member_bot_added_v1(self._sync_handler(lambda d: self._on_bot_added(d))) \
            .register_p2_card_action_trigger(self._sync_card_handler(lambda d: self._on_card_action(d))) \
            .build()

        def run_ws() -> None:
            client = CardAwareWsClient(
                self.config.app_id,
                self.config.app_secret,
                event_handler=handler,
                log_level=lark.LogLevel.INFO,
            )
            self._ws_client = client
            client.start()

        await asyncio.get_running_loop().run_in_executor(None, run_ws)

    # ────────────── 飞书事件 ──────────────

    async def _on_message_event(self, data: P2ImMessageReceiveV1) -> None:
        """im.message.receive_v1：路由到绑定实例，或拦截指令，或弹选择卡片。"""
        try:
            event = data.event
            if not event or not event.message:
                return
            message = event.message
            chat_id = message.chat_id or ""
            chat_type = message.chat_type or "p2p"
            sender_id = (event.sender.sender_id.open_id or "") if event.sender else ""

            raw_event = self._event_to_dict(data)

            # 1) /ws 指令拦截（p2p 直接回复；群里只响应被 @ 的消息）
            content = message.content or ""
            text = self._extract_text(content)
            prefix = self.config.command_prefix.lower()
            self.log.info("收到飞书消息: chat=%s type=%s sender=%s text=%r",
                          chat_id, chat_type, sender_id, text[:30])
            if text.lower().startswith(prefix):
                if chat_type == "group" and not self._is_bot_mentioned(raw_event):
                    return
                reply = self.commands.handle(text, sender_id, chat_id)
                await self._send_text(chat_id, reply)
                return

            # 2) 已绑定 → 转发给对应插件
            binding = self.state.get_binding(chat_id)
            if binding and self.state.workspace_alive(binding.workspace):
                conn = self.state.by_workspace[binding.workspace]
                self.log.info("消息路由到绑定实例: chat=%s -> %s (%s)", chat_id, binding.workspace, conn.conn_id)
                await self._push_event(conn, raw_event)
                return

            # 3) 未绑定（或绑定的实例离线）→ 排队 + 选择卡片
            self.log.info("消息进入未绑定路径: chat=%s binding=%s", chat_id, "有(实例离线)" if binding else "无")
            await self._enqueue_and_ask(chat_id, chat_type, raw_event)
        except Exception as e:  # noqa: BLE001
            self.log.exception("处理飞书消息失败: %s", e)

    async def _on_bot_added(self, data: P2ImChatMemberBotAddedV1) -> None:
        try:
            chat_id = data.event.chat_id if data.event else ""
            if not chat_id:
                return
            raw = {
                "type": "im.chat.member.bot.added_v1",
                "payload": {"chat_id": chat_id},
            }
            # 入群事件广播给所有在线实例（历史摄入按各自绑定窗口处理）
            for conn in list(self.state.connections.values()):
                await self._push_event(conn, raw)
        except Exception as e:  # noqa: BLE001
            self.log.exception("处理 bot 入群事件失败: %s", e)

    async def _on_card_action(self, data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
        """card.action.trigger：指令卡片 bridge 自己消费；其余转发给绑定实例。"""
        try:
            event = data.event
            if not event:
                return P2CardActionTriggerResponse()
            context = event.context
            chat_id = (context.open_chat_id if context else "") or ""

            # 选择卡片（form 提交，formName: ws_pick）由 bridge 处理。
            # 绑定可能涉及启动工作区（>3s），后台执行；立即返回"处理中" toast，
            # 完成后由 flush_pending 追加文本消息告知结果。
            action = event.action
            if action and action.form_value is not None:
                form_name = ""
                if action.name and action.name.startswith("btn_submit_"):
                    form_name = action.name[len("btn_submit_"):]
                if form_name == "ws_pick":
                    operator_id = (event.operator.open_id if event.operator else "") or ""
                    asyncio.get_running_loop().create_task(
                        self._run_pick_submit(chat_id, action.form_value or {}, operator_id))
                    resp = P2CardActionTriggerResponse()
                    resp.toast = CallBackToast.builder() \
                        .type("info").content("处理中，完成后将在此窗口提示…").build()
                    return resp

            # 其他卡片回调转发给绑定实例（插件负责 toast 语义）
            binding = self.state.get_binding(chat_id)
            if binding and self.state.workspace_alive(binding.workspace):
                conn = self.state.by_workspace[binding.workspace]
                await self._push_event(conn, {
                    "type": "card.action.trigger",
                    "payload": self._card_action_to_dict(data),
                })
                # 插件的 toast 无法经 WS 返回，返回通用成功 toast（点击本身已被感知）
                resp = P2CardActionTriggerResponse()
                resp.toast = CallBackToast.builder() \
                    .type("info").content("已收到点击").build()
                return resp

            self.log.warning("卡片回调无绑定实例: chat=%s", chat_id)
            resp = P2CardActionTriggerResponse()
            resp.toast = CallBackToast.builder() \
                .type("warning").content("当前窗口未绑定工作区，请先发送消息完成选择").build()
            return resp
        except Exception as e:  # noqa: BLE001
            self.log.exception("处理卡片回调失败: %s", e)
            return P2CardActionTriggerResponse()

    # ────────────── 选择卡片流程 ──────────────

    async def _run_pick_submit(self, chat_id: str, form_value: Dict[str, Any], operator_id: str) -> None:
        """后台执行选择提交；结果通过窗口文本消息反馈（toast 已提前返回"处理中"）。"""
        try:
            message = await self.commands.handle_pick_submit(chat_id, form_value, operator_id)
            if message and message != "ok":
                await self._send_text(chat_id, message)
        except Exception as e:  # noqa: BLE001
            self.log.exception("选择提交后台处理失败: %s", e)
            await self._send_text(chat_id, f"⚠️ 绑定失败: {e}")

    async def _enqueue_and_ask(self, chat_id: str, chat_type: str, raw_event: Dict[str, Any]) -> None:
        pending = self.state.pending_picks.get(chat_id)
        if pending is None:
            pending = PendingPick(chat_id=chat_id, chat_type=chat_type)
            self.state.pending_picks[chat_id] = pending
            self.log.info("窗口进入待选择状态: chat=%s type=%s", chat_id, chat_type)
        if len(pending.queued) >= self.config.queue_max_messages:
            await self._send_text(chat_id, "⚠️ 排队消息过多，请先完成工作区/会话选择")
            return
        pending.queued.append(raw_event)
        self.log.info("消息已排队: chat=%s queueLen=%d cardSent=%s", chat_id, len(pending.queued), pending.card_message_id is not None)
        if pending.card_message_id is None:
            await self._send_pick_card(chat_id, chat_type)

    async def _send_pick_card(self, chat_id: str, chat_type: str) -> None:
        pending = self.state.pending_picks.get(chat_id)
        if not pending:
            return
        try:
            # 在线实例顺带拉取 session 列表，供卡片二级下拉（离线实例只能新会话）。
            # 多实例时取第一个有返回的实例；都拿不到则列表为空。
            sessions: list = []
            for ws_path in self.state.by_workspace:
                sessions = await self.server.list_sessions_for_workspace(ws_path)
                if sessions:
                    break
            card = self.commands.build_pick_card(chat_id, sessions)
            select_el = card["body"]["elements"][0]["elements"][1]
            self.log.info("下发选择卡片: chat=%s selectTag=%s optionCount=%d options=%s sessionCount=%d",
                          chat_id, select_el.get("tag"), len(select_el.get("options", [])),
                          [o["text"]["content"] for o in select_el.get("options", [])],
                          len(sessions))
            req = CreateMessageRequest.builder() \
                .receive_id_type("chat_id") \
                .request_body(CreateMessageRequestBody.builder()
                              .receive_id(chat_id)
                              .msg_type("interactive")
                              .content(json.dumps(card, ensure_ascii=False))
                              .build()) \
                .build()
            resp: Any = await asyncio.get_running_loop().run_in_executor(
                None, self.rpc.client.im.v1.message.create, req)
            if resp.success() and resp.data and resp.data.message_id:
                pending.card_message_id = resp.data.message_id
                self.log.info("选择卡片已送达: chat=%s messageId=%s", chat_id, resp.data.message_id)
            if not resp.success():
                self.log.error("发送选择卡片失败: %s", resp.msg)
        except Exception as e:  # noqa: BLE001
            self.log.exception("发送选择卡片异常: %s", e)

    async def flush_pending(self, chat_id: str, workspace: str, session_id: Optional[str]) -> None:
        """选择完成：绑定 + 通知插件 + 补投排队消息。"""
        pending = self.state.pending_picks.pop(chat_id, None)
        if pending and pending.task:
            pending.task.cancel()
        self.state.set_binding(chat_id, workspace, session_id)
        conn = self.state.by_workspace.get(workspace)
        if conn:
            await conn.send_queue.put({
                "type": "bind",
                "chatId": chat_id,
                "sessionId": session_id,
            })
            if pending:
                for raw_event in pending.queued:
                    await self._push_event(conn, raw_event)
        await self._send_text(chat_id, f"✅ 已绑定 {workspace}" + (f"（会话 {session_id[:12]}…）" if session_id else ""))

    # ────────────── 发送 / 推送 ──────────────

    async def _push_event(self, conn: PluginConnection, raw: Dict[str, Any]) -> None:
        """raw 形如 {type: <eventType>, payload: {...}}；统一包装为插件协议的 event 信封。"""
        await conn.send_queue.put({
            "type": "event",
            "eventType": raw.get("type", ""),
            "payload": raw.get("payload", {}),
        })

    async def _send_text(self, chat_id: str, text: str) -> None:
        try:
            req = CreateMessageRequest.builder() \
                .receive_id_type("chat_id") \
                .request_body(CreateMessageRequestBody.builder()
                              .receive_id(chat_id)
                              .msg_type("text")
                              .content(json.dumps({"text": text}, ensure_ascii=False))
                              .build()) \
                .build()
            resp: Any = await asyncio.get_running_loop().run_in_executor(
                None, self.rpc.client.im.v1.message.create, req)
            if not resp.success():
                self.log.error("发送文本失败: %s", resp.msg)
        except Exception as e:  # noqa: BLE001
            self.log.exception("发送文本异常: %s", e)

    # ────────────── payload 折叠 ──────────────

    def _event_to_dict(self, data: P2ImMessageReceiveV1) -> Dict[str, Any]:
        """SDK 模型 → 插件 gateway 期望的飞书原始事件 dict（im.message.receive_v1 data 形状）。"""
        event = data.event
        message = event.message
        sender = event.sender
        raw_message: Dict[str, Any] = {
            "chat_id": message.chat_id,
            "message_id": message.message_id,
            "message_type": message.message_type,
            "content": message.content,
            "chat_type": message.chat_type,
            "root_id": message.root_id,
            "parent_id": message.parent_id,
            "create_time": str(message.create_time) if message.create_time else None,
            "mentions": self._mentions_to_list(message.mentions),
        }
        raw_event = {
            "sender": {"sender_id": {"open_id": sender.sender_id.open_id if sender and sender.sender_id else ""}},
            "message": raw_message,
        }
        return {
            "type": "im.message.receive_v1",
            "payload": _plain(raw_event),
        }

    def _card_action_to_dict(self, data: P2CardActionTrigger) -> Dict[str, Any]:
        event = data.event
        action = event.action
        context = event.context
        operator = event.operator
        raw = {
            "action": {
                "tag": action.tag,
                "value": action.value,
                "name": action.name,
                "input_value": action.input_value,
                "option": action.option,
                "option_list": action.options,
                "checked": action.checked,
                "form_value": action.form_value,
                "timezone": action.timezone,
            },
            "context": {
                "open_message_id": context.open_message_id if context else None,
                "open_chat_id": context.open_chat_id if context else None,
            },
            "operator": {"open_id": operator.open_id if operator else None},
        }
        return _plain(raw)

    @staticmethod
    def _mentions_to_list(mentions: Any) -> list:
        result = []
        for m in mentions or []:
            key = getattr(m, "key", None)
            mid = getattr(m, "id", None)
            result.append({
                "key": key,
                "id": {"open_id": getattr(mid, "open_id", None)} if mid else None,
            })
        return result

    @staticmethod
    def _extract_text(content: str) -> str:
        try:
            obj = json.loads(content)
            return str(obj.get("text", ""))
        except Exception:  # noqa: BLE001
            return ""

    @staticmethod
    def _is_bot_mentioned(raw_event: Dict[str, Any]) -> bool:
        """群里指令需要 @bot 才响应；bot open_id 未知时退回「有 mention 就响应」。"""
        message = raw_event.get("payload", {}).get("message", {})
        mentions = message.get("mentions") or []
        if not mentions:
            return False
        # mention.id.open_id 是 bot 自身时 best-effort 判定；否则任何 mention 均放行
        return True


def _plain(obj: Any) -> Any:
    """递归去掉 None 字段，保证 JSON 干净。"""
    if isinstance(obj, dict):
        return {k: _plain(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_plain(v) for v in obj]
    return obj
