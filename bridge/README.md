# feishu_bridge — 飞书 ↔ 多 OpenCode 工作区桥接服务

一个 Python 后台服务，解决「多个工作区共用一个飞书机器人」的问题：

```
飞书 ⇄ [本服务（唯一持有凭据，单 WS 长连接）]
            │ ws://127.0.0.1:8787
            ├─ 路由：每个聊天窗口指向一个 OpenCode 实例（可切换、持久化）
            ├─ 指令：/ws list | use | open | close | new | unbind
            ├─ 未绑定的窗口发消息时，自动弹「选择工作区」卡片
            └─ 进程管理：可按需启动/停止各工作区的 opencode
                  ↕ bridge 协议（WebSocket + RPC）
        [opencode-feishu 插件 ×N（bridge 模式），每个工作区一个 OpenCode]
```

插件侧所有会话/流式卡片/权限问答逻辑不变；本服务只做**飞书协议终端 + 路由 + 进程管理**。

## 安装

```bash
cd bridge
python3 -m venv .venv
.venv/bin/pip install lark-oapi websockets   # Python >= 3.8，建议 3.10+
```

## 配置

复制 `config.example.json` 为 `bridge.json`：

| 字段 | 必填 | 说明 |
|---|---|---|
| `appId` / `appSecret` | 是 | 飞书自建应用凭据（插件侧不再需要配置） |
| `host` / `port` | 否 | 本地 WS 监听地址，默认 `127.0.0.1:8787` |
| `token` | 否 | 插件连接鉴权 token；配置后插件侧必须一致 |
| `admins` | 否 | 可用 `/ws` 指令的 open_id 白名单；**不配则所有人可用** |
| `commandPrefix` | 否 | 指令前缀，默认 `/ws` |
| `basePort` | 否 | 自动给 `opencode serve` 分配端口的起始值，默认 14000 |
| `stateFile` | 否 | 绑定表持久化文件，默认 `bridge-state.json` |
| `workspaces` | 否 | 受管工作区列表；`command` 可自定义启动命令（`{port}`/`{path}` 占位） |

## 启动

```bash
cd bridge
.venv/bin/python -m feishu_bridge -c bridge.json
```

## 插件侧配置

每个工作区的 `~/.config/opencode/plugins/feishu.json`：

```json
{
  "bridge": { "url": "ws://127.0.0.1:8787", "token": "可选" },
  "directory": "/home/me/project-a"
}
```

- 配置了 `bridge` 即进入 bridge 模式：不需要 `appId`/`appSecret`
- `directory` 是该实例的工作区路径，bridge 用它区分实例（**同一 bridge 不允许重复**）

## 使用

1. **绑定**：在任意聊天窗口第一次 @bot / 私聊 bot 时，会收到「选择目标工作区」卡片：
   - **工作区下拉** = `bridge.json` 配置的工作区 ∪ 当前已连接的实例（未配置的在线实例标记为「未配置·在线」，一样可选）
   - **会话下拉** = 该工作区 opencode 的已有 session（最新在前，默认选中最近一个 = 复用），最后一项「🆕 新会话」
   - **新工作区路径输入框** = 填了则忽略下拉，注册并启动新工作区（目录须已存在，重启后需写入 bridge.json 持久化）
   - 提交后立即返回「处理中」toast，绑定完成（离线工作区会先启动并等插件连接，最多 15s）后在窗口发「✅ 已绑定 …」，排队消息自动补投
2. **切换**：`/ws use <n>`（编号来自 `/ws list`）；切走不影响目标实例运行
3. **启停**：`/ws open <n>` 启动工作区的 opencode；`/ws close <n>` 停止
4. **解绑**：`/ws unbind` 下次发消息重新弹选择卡片
5. **新工作区**：`/ws new <路径>` 或卡片输入框，运行时注册并启动

> 群里指令需要 @bot 才会响应。绑定是**按窗口**的：私聊窗口和每个群各自独立。

## 行为细节

- **绑定持久化**：`bridge-state.json` 记录 `chatId → 工作区/会话`，bridge 重启后自动恢复。
- **绑定重推**：插件注册（hello）后，bridge 会把该工作区的全部存量绑定重新推送（`bind` 控制消息）。
  插件内存 override 在 bridge 或插件任一方重启后都能恢复，消息不会错路由到默认 session。
- **实例离线**：窗口绑定了但插件断开时，消息会按「未绑定」处理（排队 + 弹卡片，卡片里可重选）。
- **重复注册保护**：同一 `directory` 的第二个连接会被拒绝（防止多开抢消息）。
- **RPC 结构**：插件侧 shim 把 Lark SDK 参数折叠成 `{path, params, data}` 三段发送；
  `api.py` 按各飞书接口的实际参数位置展开（path 参数如 `card_id`/`message_id` 从 `path` 取，body 参数从 `data` 取）。
- **卡片回调**：经 bridge 转发的按钮点击返回通用 toast（bridge 无法同步等待插件的异步处理，
  选择卡片提交为后台执行 + 文本消息反馈结果；权限/问答卡片的实际业务逻辑在插件侧正常执行）。
- **资源下载**：图片/文件经 bridge 以 base64 中转，内存占用约为文件大小的 1.3 倍；
  大文件场景建议调低插件侧 `maxResourceSize`。
- **调试日志**：bridge 全链路 info 日志（消息接收/路由/推送/RPC/指令），排障看 stdout 即可；
  插件侧设置 `FEISHU_DEBUG=1` 可在 TUI stderr 看到对应的事件到达日志。

## 协议速查（开发用）

插件 ↔ bridge 为 JSON 文本帧，方向标注：

| 方向 | 消息 |
|---|---|
| 插件→bridge | `{type:"hello", protoVersion:1, workspace, pid, token?}` |
| 插件→bridge | `{type:"rpc", id, method, params}` — 飞书 API 代理（params 为 `{path, params, data}` 三段） |
| 插件→bridge | `{type:"ping"}` / `{type:"respond_sessions", reqId, sessions}` |
| bridge→插件 | `{type:"hello_ok"}` / `{type:"hello_err", error}` |
| bridge→插件 | `{type:"event", eventType, payload}` — 飞书原始事件 |
| bridge→插件 | `{type:"bind", chatId, sessionId}` — 会话绑定控制（注册后重推存量 + 用户选择后实时推） |
| bridge→插件 | `{type:"list_sessions", reqId}` / `{type:"rpc_result", id, ok, result\|error}` |

RPC 方法名与插件 `src/bridge/relay-client.ts` 的 `BridgeRpcMethod` 一一对应，
由 `feishu_bridge/api.py` 分发到 lark-oapi。

## 本地自测

```bash
cd bridge
.venv/bin/python test_protocol.py   # mock 插件走完整协议：hello/RPC/绑定/补投/路由/指令/list_sessions
```

## 已知限制

- lark-oapi 1.7.3 的 WS 客户端会丢弃卡片回调帧，本项目用
  `CardAwareWsClient`（`larkgw.py`）子类补上；升级 SDK 后可验证是否仍需该补丁。
  同理，1.7.3 的 SDK handler 是同步调用，async 函数会被静默丢弃——
  `FeishuGateway._sync_handler` 用 `run_coroutine_threadsafe` 把协程调度回主事件循环。
- bridge 是单点：进程挂掉则所有窗口不可用（插件会自动重连，恢复后无需人工干预）。
- 选择卡片超时（`pickTimeoutSeconds`）当前未强制执行，排队消息会在下次绑定后补投。
