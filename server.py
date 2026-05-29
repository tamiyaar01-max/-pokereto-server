import asyncio
import json
import logging
import os
import signal
import sys

import websockets
from websockets.server import WebSocketServerProtocol

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | pokereto | %(message)s'
)
log = logging.getLogger("pokereto")

PORT = int(os.environ.get("PORT", "10000"))
HOST = "0.0.0.0"
HELLO_TIMEOUT = 15.0

# bellID → WebSocket のマップ
clients: dict[str, WebSocketServerProtocol] = {}


async def handler(ws: WebSocketServerProtocol):
    bell_id = None
    peer = ws.remote_address
    log.info(f"CONNECT from {peer}")

    try:
        # HELLO 待ち
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=HELLO_TIMEOUT)
        except asyncio.TimeoutError:
            log.warning(f"HELLO timeout from {peer}")
            await ws.close(code=4000, reason="HELLO timeout")
            return

        try:
            hello = json.loads(raw)
            bell_id = hello.get("bellID")
        except (json.JSONDecodeError, AttributeError) as e:
            log.warning(f"Invalid HELLO from {peer}: {e}")
            await ws.close(code=4001, reason="Invalid HELLO")
            return

        if not bell_id:
            log.warning(f"No bellID in HELLO from {peer}")
            await ws.close(code=4002, reason="No bellID")
            return

        clients[bell_id] = ws
        log.info(f"HELLO {bell_id} (active={len(clients)})")

        # メッセージ受信ループ
        async for message in ws:
            try:
                packet = json.loads(message)
                from_id = packet.get("from", bell_id)
                to_id = packet.get("to", "*")
                log.info(f"MSG {from_id} -> {to_id}: {packet.get('code','')}")

                if to_id == "*":
                    # 全員に配信（送信者を除く）
                    for cid, cws in list(clients.items()):
                        if cid != from_id:
                            try:
                                await cws.send(message)
                                log.info(f"  -> broadcast to {cid}")
                            except Exception as e:
                                log.warning(f"  -> broadcast fail {cid}: {e}")
                else:
                    # 個別配信（特定のbellID宛て）
                    target_ws = clients.get(to_id)
                    if target_ws:
                        try:
                            await target_ws.send(message)
                            log.info(f"  -> sent to {to_id}")
                        except Exception as e:
                            log.warning(f"  -> send fail {to_id}: {e}")
                    else:
                        log.info(f"  -> {to_id} offline")

            except json.JSONDecodeError:
                log.warning(f"Invalid JSON from {bell_id}: {message[:100]}")
            except Exception as e:
                log.warning(f"Error from {bell_id}: {e}")

    except websockets.ConnectionClosed:
        pass
    except Exception as e:
        log.warning(f"Handler error {bell_id or peer}: {e}")
    finally:
        if bell_id and clients.get(bell_id) is ws:
            del clients[bell_id]
            log.info(f"DISCONNECT {bell_id} (active={len(clients)})")


async def main():
    log.info("=" * 60)
    log.info(f"PokeReto Server starting on {HOST}:{PORT}")
    log.info(f"Python: {sys.version.split()[0]}")
    log.info(f"websockets: {websockets.__version__}")
    log.info("=" * 60)
    log.info(f"Server ready: ws://{HOST}:{PORT}")

    stop = asyncio.Future()

    def shutdown(*args):
        log.info("Shutting down...")
        if not stop.done():
            stop.set_result(None)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass

    async with websockets.serve(handler, HOST, PORT):
        await stop


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
