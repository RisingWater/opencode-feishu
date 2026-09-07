"""RPC 分发表：把插件的飞书 API 调用代理到 lark-oapi。

方法名与插件侧 `src/bridge/lark-shim.ts` 的 `BridgeRpcMethod` 一一对应。
统一返回 {code, msg, data}；code=0 表示成功。
"""

import base64
import json
from typing import Any, Dict

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageRequestBody,
    DeleteMessageRequest,
    GetMessageRequest,
    GetMessageResourceRequest,
    ListMessageRequest,
    PatchMessageRequest,
    PatchMessageRequestBody,
    UpdateMessageRequest,
    UpdateMessageRequestBody,
)
from lark_oapi.api.im.v1.model.get_chat_response import GetChatResponse
from lark_oapi.api.contact.v3 import GetUserRequest, GetUserResponse
from lark_oapi.api.cardkit.v1 import (
    CreateCardRequest,
    CreateCardRequestBody,
    ContentCardElementRequest,
    ContentCardElementRequestBody,
    CreateCardElementRequest,
    CreateCardElementRequestBody,
    UpdateCardElementRequest,
    UpdateCardElementRequestBody,
    PatchCardElementRequest,
    PatchCardElementRequestBody,
    DeleteCardElementRequest,
    DeleteCardElementRequestBody,
    SettingsCardRequest,
    SettingsCardRequestBody,
)


class RpcError(Exception):
    def __init__(self, message: str, code: int = -1):
        super().__init__(message)
        self.code = code


def _ok(data: Any = None) -> Dict[str, Any]:
    return {"code": 0, "msg": "success", "data": data or {}}


def _from_response(resp: Any, extract: Any = None) -> Dict[str, Any]:
    if resp.success():
        data = extract(resp) if extract else {}
        return {"code": 0, "msg": "success", "data": data or {}}
    return {
        "code": resp.code if resp.code else -1,
        "msg": f"{resp.msg} (logId={resp.get_log_id()})",
        "data": {},
    }


class RpcHandler:
    def __init__(self, client: lark.Client):
        self.client = client

    async def dispatch(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """所有 RPC 都是同步 SDK 调用（几秒内返回），直接在事件循环执行。"""
        handler = getattr(self, "_" + method.replace(".", "_"), None)
        if handler is None:
            raise RpcError(f"未知 RPC 方法: {method}")
        return handler(params)

    # ────────────── im.message ──────────────

    def _im_message_create(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = CreateMessageRequest.builder() \
            .receive_id_type("chat_id") \
            .request_body(CreateMessageRequestBody.builder()
                          .receive_id(p["receive_id"])
                          .msg_type(p["msg_type"])
                          .content(p["content"])
                          .build()) \
            .build()
        resp: Any = self.client.im.v1.message.create(req)
        return _from_response(resp, lambda r: {"message_id": r.data.message_id})

    def _im_message_update(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = UpdateMessageRequest.builder() \
            .message_id(p["path"]["message_id"]) \
            .request_body(UpdateMessageRequestBody.builder()
                          .msg_type(p["data"]["msg_type"])
                          .content(p["data"]["content"])
                          .build()) \
            .build()
        resp: Any = self.client.im.v1.message.update(req)
        return _from_response(resp)

    def _im_message_patch(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = PatchMessageRequest.builder() \
            .message_id(p["path"]["message_id"]) \
            .request_body(PatchMessageRequestBody.builder()
                          .content(p["data"]["content"])
                          .build()) \
            .build()
        resp: Any = self.client.im.v1.message.patch(req)
        return _from_response(resp)

    def _im_message_delete(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = DeleteMessageRequest.builder().message_id(p["path"]["message_id"]).build()
        resp: Any = self.client.im.v1.message.delete(req)
        return _from_response(resp)

    def _im_message_get(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = GetMessageRequest.builder().message_id(p["path"]["message_id"]).build()
        resp: Any = self.client.im.v1.message.get(req)
        return _from_response(resp, lambda r: {"items": _items_to_dicts(r.data.items)})

    def _im_message_list(self, p: Dict[str, Any]) -> Dict[str, Any]:
        b = ListMessageRequest.builder().container_id_type("chat").container_id(p["container_id"])
        if p.get("sort_type"):
            b = b.sort_type(p["sort_type"])
        if p.get("page_size"):
            b = b.page_size(int(p["page_size"]))
        if p.get("page_token"):
            b = b.page_token(p["page_token"])
        resp: Any = self.client.im.v1.message.list(b.build())
        return _from_response(resp, lambda r: {
            "items": _items_to_dicts(r.data.items),
            "has_more": bool(r.data.has_more),
            "page_token": r.data.page_token or "",
        })

    def _im_messageResource_get(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = GetMessageResourceRequest.builder() \
            .message_id(p["message_id"]) \
            .file_key(p["file_key"]) \
            .type(p["type"]) \
            .build()
        resp: Any = self.client.im.v1.message_resource.get(req)
        if not resp.success():
            return _from_response(resp)
        raw = resp.file.read()
        content_type = ""
        try:
            content_type = resp.raw.content.get("content-type", "") if resp.raw and resp.raw.content else ""
        except Exception:  # noqa: BLE001
            content_type = ""
        return _ok({
            "dataBase64": base64.b64encode(raw).decode("ascii"),
            "mime": content_type or "application/octet-stream",
            "headers": {"content-type": content_type or "application/octet-stream"},
        })

    def _im_chat_get(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = GetChatRequestBuilder(p["chat_id"])
        resp: Any = self.client.im.v1.chat.get(req.build())
        return _from_response(resp, lambda r: {
            "chat_mode": r.data.chat_mode or "",
            "chat_type": r.data.chat_type or "",
        })

    # ────────────── contact / bot ──────────────

    def _contact_user_get(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = GetUserRequest.builder().user_id(p["user_id"]).user_id_type("open_id").build()
        resp: GetUserResponse = self.client.contact.v3.user.get(req)
        if not resp.success():
            return _from_response(resp)
        user = resp.data.user if resp.data else None
        return _ok({"user": {"name": getattr(user, "name", "") or ""}})

    def _bot_info(self, _p: Dict[str, Any]) -> Dict[str, Any]:
        req = lark.BaseRequest.builder() \
            .uri("/open-apis/bot/v3/info") \
            .http_method(lark.HttpMethod.GET) \
            .token_types([lark.AccessTokenType.TENANT]) \
            .build()
        resp: Any = self.client.request(req)
        if not resp.success():
            return _from_response(resp)
        content = json.loads(str(resp.raw.content, "utf-8")) if resp.raw and resp.raw.content else {}
        bot = content.get("bot", {})
        return _ok({"bot": {"open_id": bot.get("open_id", "")}})

    # ────────────── cardkit ──────────────
    # shim（lark-shim.ts）传来的 params 是 {path, params, data} 三段结构。

    def _cardkit_card_create(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = CreateCardRequest.builder() \
            .request_body(CreateCardRequestBody.builder()
                          .type("card_json")
                          .data(p["data"])
                          .build()) \
            .build()
        resp: Any = self.client.cardkit.v1.card.create(req)
        return _from_response(resp, lambda r: {"card_id": r.data.card_id})

    def _cardkit_cardElement_content(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = ContentCardElementRequest.builder() \
            .card_id(p["path"]["card_id"]) \
            .element_id(p["path"]["element_id"]) \
            .request_body(ContentCardElementRequestBody.builder()
                          .content(p["data"]["content"])
                          .sequence(int(p["data"]["sequence"]))
                          .build()) \
            .build()
        resp: Any = self.client.cardkit.v1.card_element.content(req)
        return _from_response(resp)

    def _cardkit_cardElement_create(self, p: Dict[str, Any]) -> Dict[str, Any]:
        b = CreateCardElementRequestBody.builder() \
            .elements(p["data"]["elements"]) \
            .sequence(int(p["data"]["sequence"]))
        if p["data"].get("type"):
            b = b.type(p["data"]["type"])
        if p["data"].get("target_element_id"):
            b = b.target_element_id(p["data"]["target_element_id"])
        req = CreateCardElementRequest.builder() \
            .card_id(p["path"]["card_id"]) \
            .request_body(b.build()) \
            .build()
        resp: Any = self.client.cardkit.v1.card_element.create(req)
        return _from_response(resp)

    def _cardkit_cardElement_update(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = UpdateCardElementRequest.builder() \
            .card_id(p["path"]["card_id"]) \
            .element_id(p["path"]["element_id"]) \
            .request_body(UpdateCardElementRequestBody.builder()
                          .element(p["data"]["element"])
                          .sequence(int(p["data"]["sequence"]))
                          .build()) \
            .build()
        resp: Any = self.client.cardkit.v1.card_element.update(req)
        return _from_response(resp)

    def _cardkit_cardElement_patch(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = PatchCardElementRequest.builder() \
            .card_id(p["path"]["card_id"]) \
            .element_id(p["path"]["element_id"]) \
            .request_body(PatchCardElementRequestBody.builder()
                          .partial_element(p["data"]["partial_element"])
                          .sequence(int(p["data"]["sequence"]))
                          .build()) \
            .build()
        resp: Any = self.client.cardkit.v1.card_element.patch(req)
        return _from_response(resp)

    def _cardkit_cardElement_delete(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = DeleteCardElementRequest.builder() \
            .card_id(p["path"]["card_id"]) \
            .element_id(p["path"]["element_id"]) \
            .request_body(DeleteCardElementRequestBody.builder()
                          .sequence(int(p["data"]["sequence"]))
                          .build()) \
            .build()
        resp: Any = self.client.cardkit.v1.card_element.delete(req)
        return _from_response(resp)

    def _cardkit_card_settings(self, p: Dict[str, Any]) -> Dict[str, Any]:
        req = SettingsCardRequest.builder() \
            .card_id(p["path"]["card_id"]) \
            .request_body(SettingsCardRequestBody.builder()
                          .settings(p["data"]["settings"])
                          .sequence(int(p["data"]["sequence"]))
                          .build()) \
            .build()
        resp: Any = self.client.cardkit.v1.card.settings(req)
        return _from_response(resp)


def _items_to_dicts(items: Any) -> list:
    """SDK 模型对象 → 可 JSON 序列化的 dict 列表（供插件 quote/history 使用）。"""
    result = []
    for item in items or []:
        if hasattr(item, "_types"):
            d = {}
            for key in item._types:
                val = getattr(item, key, None)
                d[key] = _to_plain(val)
            result.append(d)
        else:
            result.append(item)
    return result


def _to_plain(val: Any) -> Any:
    if val is None or isinstance(val, (str, int, float, bool)):
        return val
    if isinstance(val, list):
        return [_to_plain(v) for v in val]
    if isinstance(val, dict):
        return {k: _to_plain(v) for k, v in val.items()}
    if hasattr(val, "_types"):
        return {k: _to_plain(getattr(val, k, None)) for k in val._types}
    return str(val)


def GetChatRequestBuilder(chat_id: str):  # noqa: N802
    from lark_oapi.api.im.v1 import GetChatRequest
    return GetChatRequest.builder().chat_id(chat_id)
