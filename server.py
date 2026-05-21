#!/usr/bin/env python3
"""PokeReto WebSocket Server - DEBUG VERSION with verbose logging"""

import asyncio
import json
import logging
import os
import signal
import sys
import traceback
from datetime import datetime
from typing import Dict

import websockets

import database

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pokereto")

ws_log = logging.getLogger("websockets")
ws_log.setLevel(logging.DEBUG)

log.info(f"Python version: {sys.version}")
log.info(f"websockets version: {websockets.__version__}")


class ConnectionManager:
    def __init__(self):
        self.active: Dict[str, object] = {}

    async def connect(self, ws, bell_id):
        if bell_id in self.active:
            try:
                await self.active[bell_id].close()
            except Exception:
                pass
        self.active[bell_id] = ws
        log.info(f"CONNECTED: {bell_id} (total: {len(self.active)})")
        await self.deliver_pending(bell_id, ws)

    def disconnect(self, bell_id):
        if bell_id in self.active:
            del self.active[bell_id]
            log.info(f"DISCONNECTED: {bell_id} (total: {len(self.active)})")

    async def deliver_pending(self, bell_id, ws):
        pending = database.get_pending_messages(bell_id)
        if not pending:
            return
        log.info(f"OFFLINE DELIVERY: {bell_id} has {len(pending)} pending")
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
                log.error(f"Failed offline delivery {msg['id']}: {e}")
                break

    async def send_to(self, bell_id, packet):
        if bell_id not in self.active:
            return False
        try:
            await self.active[bell_id].send(json.dumps(packet))
            return True
        except Exception as e:
            log.error(f"Send fail {bell_id}: {e}")
            return False

    async def broadcast(self, packet, exclude=None):
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

    def is_online(self, bell_id):
        return bell_id in self.active


manager = ConnectionManager()


async def handle_message(ws, bell_id, raw):
    log.info(f"RECV from {bell_id}: {raw[:200]}")
    try:
        packet = json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning(f"Bad JSON from {bell_id}: {e}")
        return

    from_bell = packet.get("from", bell_id)
    to_bell = packet.get("to", "*")
    code = packet.get("code", "")
    ts = packet.get("ts", datetime.now().timestamp())

    if not code:
        log.warning(f"Empty code from {bell_id}, packet: {packet}")
        return

    log.info(f"MESSAGE: {from_bell} -> {to_bell}: {code}")
    msg_id = database.save_message(from_bell, to_bell, code, ts)
    out = {"from": from_bell, "to": to_bell, "code": code, "ts": ts}

    if to_bell == "*":
        delivered = await manager.broadcast(out, exclude=from_bell)
        log.info(f"BROADCAST: {delivered} recipient(s)")
        database.mark_delivered(msg_id)
    else:
        if manager.is_online(to_bell):
            if await manager.send_to(to_bell, out):
                database.mark_delivered(msg_id)
                log.info(f"DELIVERED: {from_bell} -> {to_bell}")
        else:
            log.info(f"OFFLINE: {to_bell} stored")


async def handle_connection(ws):
    bell_id = None
    log.info("=" * 60)
    log.info(f"NEW CONNECTION accepted")
    try:
        if hasattr(ws, "request"):
            req = ws.request
            log.info(f"  path: {getattr(req, 'path', 'N/A')}")
            if hasattr(req, "headers"):
                for h_name, h_val in req.headers.raw_items():
                    log.info(f"  header: {h_name}: {h_val}")
        elif hasattr(ws, "path"):
            log.info(f"  path: {ws.path}")
        if hasattr(ws, "remote_address"):
            log.info(f"  remote: {ws.remote_address}")
    except Exception as e:
        log.warning(f"Cannot inspect connection: {e}")

    try:
        log.info("Waiting for hello message (10s timeout)...")
        hello_raw = await asyncio.wait_for(ws.recv(), timeout=10)
        log.info(f"HELLO received: {hello_raw[:200]}")
        try:
            hello = json.loads(hello_raw)
        except json.JSONDecodeError as e:
            log.warning(f"Hello not valid JSON: {e}")
            await ws.close()
            return
        bell_id = hello.get("bellID") or hello.get("from")
        if not bell_id:
            log.warning(f"No bellID in hello: {hello}")
            await ws.close()
            return
        log.info(f"Hello parsed: bellID={bell_id}")
        database.register_user(bell_id)
        await manager.connect(ws, bell_id)
        await ws.send(json.dumps({
            "type": "welcome",
            "bellID": bell_id,
            "message": "Connected to PokeReto Cloud",
        }))
        log.info(f"Welcome sent to {bell_id}")
        async for raw in ws:
            await handle_message(ws, bell_id, raw)
    except asyncio.TimeoutError:
        log.warning("Hello timeout (no message in 10s)")
    except websockets.ConnectionClosed as e:
        log.info(f"Connection closed normally: {e}")
    except Exception as e:
        log.error(f"Connection error: {type(e).__name__}: {e}")
        log.error(traceback.format_exc())
    finally:
        if bell_id:
            manager.disconnect(bell_id)
        log.info("=" * 60)


async def main():
    port = int(os.environ.get("PORT", 8080))
    host = "0.0.0.0"

    log.info("=" * 60)
    log.info(f"PokeReto DEBUG Server starting on {host}:{port}")
    log.info(f"Python: {sys.version}")
    log.info(f"websockets: {websockets.__version__}")
    log.info("=" * 60)

    database.init_db()
    log.info("Database initialized")

    async with websockets.serve(
        handle_connection,
        host,
        port,
        ping_interval=30,
        ping_timeout=10,
    ) as server:
        log.info(f"Server ready and listening on ws://{host}:{port}")
        log.info(f"Server sockets: {server.sockets}")
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
