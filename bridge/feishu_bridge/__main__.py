"""feishu_bridge 入口：python -m feishu_bridge -c bridge.json"""

import argparse
import asyncio
import json
import logging
import signal
import sys

from .commands import CommandHandler
from .config import BridgeConfig
from .larkgw import FeishuGateway
from .procman import ProcManager
from .server import BridgeServer
from .state import BridgeState


def main() -> None:
    parser = argparse.ArgumentParser(description="opencode-feishu bridge 服务")
    parser.add_argument("-c", "--config", required=True, help="bridge.json 配置文件路径")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [bridge] %(message)s",
    )
    log = logging.getLogger("feishu_bridge")

    with open(args.config, "r", encoding="utf-8") as f:
        raw = json.load(f)
    config = BridgeConfig.from_dict(raw)

    state = BridgeState(config.state_file, log)
    procman = ProcManager(config, log)
    commands = CommandHandler(config, state, procman, log)
    gateway = FeishuGateway(config, state, procman, commands, log)
    server = BridgeServer(config, state, procman, gateway, log)
    gateway.server = server  # 选择卡片需要向插件查询 session 列表

    async def run() -> None:
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:  # Windows
                pass

        server_task = asyncio.create_task(server.serve())
        lark_task = asyncio.create_task(gateway.start())
        for t, name in ((server_task, "ws-server"), (lark_task, "lark-gateway")):
            t.add_done_callback(
                lambda tk, n=name: log.error("%s 任务异常退出: %s", n, tk.exception()) if not tk.cancelled() and tk.exception() else None
            )
        try:
            await stop_event.wait()
        finally:
            log.info("bridge 正在停止…")
            lark_task.cancel()
            server_task.cancel()
            procman.stop_all()
            state.save()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    log.info("bridge 已退出")
    sys.exit(0)


if __name__ == "__main__":
    main()
