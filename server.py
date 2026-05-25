#!/usr/bin/env python3
"""PokeReto WebSocket Server - Production Ready (websockets 12.0)"""

import asyncio
import http
import json
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Dict, Optional

import websockets
from websockets.server import WebSocketServerProtocol

import database

# =====================================================================
# Logging
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pokereto")
logging.getLogger("websockets.server").setLevel(logging.WARNING)


# =====================================================================
# Health Check for Render (websockets 12.0 official pattern)
# =====================================================================
async def health_check(path: str, request_headers):
    """
    Render sends plain HTTP requests to check if the service is alive.
    websockets calls this hook BEFORE the WebSocket handshake.

    - For health check paths (/, /health, /healthz): return 200 OK
    - For WebSocket upgrade requests: return None (let handshake proceed)
    """
    if path in ("/", "/health", "/healthz"):
        return http.HTTPStatus.OK, [("Content-Type", "text/plain")], b"OK\n"
    # None -> websockets continues with the normal WebSocket handshake
    return None


# =====================================================================
# Connection Manager
# =====================================================================
class ConnectionManager:
    def __init__(self):
        self.active: Dict[str, WebSocketServerProtocol] = {}

    async def connect(self, ws: WebSocketServerProtocol, bell_id: str):
        # Disconnect any existing connection for this bell_id (one device per id)
        if bell_id in self.active:
            old_ws = self.active[bell_id]
            try:
                await old_ws.close(code=1000, reason="Replaced by new connection")
            except Exception:
                pass
        self.active[bell_id] = ws
        log.info(f"CONNECT {bell_id} (active={len(self.active)})")
        await self.deliver_pending(bell_id, ws)

    def disconnect(self, bell_id: str, ws: WebSocketServerProtocol):
        # Only disconnect if this exact ws is still the active one
        if self.active.get(bell_id) is ws:
            del self.active[bell_id]
            log.info(f"DISCONNECT {bell_id} (active={len(self.active)})")

    async def deliver_pending(self, bell_id: str, ws: WebSocketServerProtocol):
        pending = database.get_pending_messages(bell_id)
        if not pending:
            return
        log.info(f"DELIVER_PENDING {bell_id}: {len(pending)} message(s)")
        for msg in pending:
            packet = {
                "from": msg["from_bell"],
                "to": msg["to_bell"],
                "code": msg["code"],
                "ts": msg["timestamp"],
                "offline": True,
            }
            try:
                await ws.send(json.dumps(packet))
                database.mark_delivered(msg["id"])
            except Exception as e:
                log.error(f"deliver_pending fail {msg['id']}: {e}")
                break

    async def send_to(self, bell_id: str, packet: dict) -> bool:
        ws = self.active.get(bell_id)
        if ws is None:
            return False
        try:
            await ws.send(json.dumps(packet))
            return True
        except Exception as e:
            log.error(f"send_to {bell_id} fail: {e}")
            return False

    async def broadcast(self, packet: dict, exclude: Optional[str] = None) -> int:
        delivered = 0
        for bell_id, ws in list(self.active.items()):
            if bell_id == exclude:
                continue
            try:
                await ws.send(json.dumps(packet))
                delivered += 1
            except Exception:
                pass
        return delivered

    def is_online(self, bell_id: str) -> bool:
        return bell_id in self.active


manager = ConnectionManager()


# =====================================================================
# Message Handling
# =====================================================================
async def handle_message(ws: WebSocketServerProtocol, bell_id: str, raw: str):
    try:
        packet = json.loads(raw)
    except json.JSONDecodeError:
        log.warning(f"Bad JSON from {bell_id}: {raw[:120]}")
        return

    from_bell = packet.get("from", bell_id)
    to_bell = packet.get("to", "*")
    code = packet.get("code", "")
    ts = packet.get("ts", datetime.now().timestamp())

    if not code:
        return

    log.info(f"MSG {from_bell} -> {to_bell}: {code}")
    msg_id = database.save_message(from_bell, to_bell, code, ts)
    out = {"from": from_bell, "to": to_bell, "code": code, "ts": ts}

    if to_bell == "*":
        delivered = await manager.broadcast(out, exclude=from_bell)
        log.info(f"  broadcast -> {delivered} recipient(s)")
        database.mark_delivered(msg_id)
    else:
        if manager.is_online(to_bell) and await manager.send_to(to_bell, out):
            database.mark_delivered(msg_id)
            log.info(f"  delivered -> {to_bell}")
        else:
            log.info(f"  offline, stored for {to_bell}")


# =====================================================================
# Connection Handler
# =====================================================================
async def handle_connection(ws: WebSocketServerProtocol, path: str = "/"):
    bell_id: Optional[str] = None
    try:
        # Wait for "hello" message to identify the device
        try:
            hello_raw = await asyncio.wait_for(ws.recv(), timeout=15)
        except asyncio.TimeoutError:
            log.warning("hello timeout, closing")
            return

        try:
            hello = json.loads(hello_raw)
        except json.JSONDecodeError:
            log.warning(f"hello not JSON: {hello_raw[:120]}")
            return

        bell_id = hello.get("bellID") or hello.get("from")
        if not bell_id:
            log.warning(f"hello missing bellID: {hello}")
            return

        database.register_user(bell_id)
        await manager.connect(ws, bell_id)

        # Send welcome
        await ws.send(json.dumps({
            "type": "welcome",
            "bellID": bell_id,
            "message": "Connected to PokeReto Cloud",
        }))

        # Main message loop
        async for raw in ws:
            await handle_message(ws, bell_id, raw)

    except websockets.ConnectionClosed:
        pass
    except Exception as e:
        log.error(f"connection error ({bell_id}): {type(e).__name__}: {e}")
    finally:
        if bell_id:
            manager.disconnect(bell_id, ws)


# =====================================================================
# Main
# =====================================================================
async def main():
    port = int(os.environ.get("PORT", 8080))
    host = "0.0.0.0"

    log.info("=" * 60)
    log.info(f"PokeReto Server starting on {host}:{port}")
    log.info(f"Python: {sys.version.split()[0]}")
    log.info(f"websockets: {websockets.__version__}")
    log.info("=" * 60)

    database.init_db()
    log.info("Database initialized")

    async with websockets.serve(
        handle_connection,
        host,
        port,
        process_request=health_check,
        ping_interval=30,
        ping_timeout=20,
        close_timeout=10,
    ):
        log.info(f"Server ready: ws://{host}:{port}")
        stop = asyncio.Future()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set_result, None)
            except NotImplementedError:
                pass
        await stop
        log.info("Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())
