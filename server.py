#!/usr/bin/env python3
"""PokeReto WebSocket Server (Cloud + Offline Messages) - FIXED for websockets 12.0"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime
from http import HTTPStatus
from typing import Dict

import websockets

import database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pokereto")


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
    try:
        packet = json.loads(raw)
    except json.JSONDecodeError:
        log.warning(f"Bad JSON from {bell_id}")
        return

    from_bell = packet.get("from", bell_id)
    to_bell = packet.get("to", "*")
    code = packet.get("code", "")
    ts = packet.get("ts", datetime.now().timestamp())

    if not code:
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
    try:
        hello_raw = await asyncio.wait_for(ws.recv(), timeout=10)
        try:
            hello = json.loads(hello_raw)
        except json.JSONDecodeError:
            await ws.close()
            return
        bell_id = hello.get("bellID") or hello.get("from")
        if not bell_id:
            await ws.close()
            return
        database.register_user(bell_id)
        await manager.connect(ws, bell_id)
        await ws.send(json.dumps({
            "type": "welcome",
            "bellID": bell_id,
            "message": "Connected to PokeReto Cloud",
        }))
        async for raw in ws:
            await handle_message(ws, bell_id, raw)
    except asyncio.TimeoutError:
        log.warning("Hello timeout")
    except websockets.ConnectionClosed:
        pass
    except Exception as e:
        log.error(f"Conn error: {e}")
    finally:
        if bell_id:
            manager.disconnect(bell_id)


def health_check(connection, request):
    if request.path in ("/", "/health", "/healthz"):
        return connection.respond(HTTPStatus.OK, "OK - PokeReto Server\n")
    return None


async def main():
    port = int(os.environ.get("PORT", 8080))
    host = "0.0.0.0"

    log.info("=" * 60)
    log.info(f"PokeReto Server starting on {host}:{port}")
    log.info("=" * 60)

    database.init_db()
    log.info("Database initialized")

    async with websockets.serve(
        handle_connection,
        host,
        port,
        process_request=health_check,
        ping_interval=30,
        ping_timeout=10,
    ):
        log.info(f"Server ready and listening on ws://{host}:{port}")
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
