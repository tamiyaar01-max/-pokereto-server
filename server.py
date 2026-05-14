#!/usr/bin/env python3
"""
PokeReto WebSocket Server (Cloud + Offline Messages)
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime
from typing import Dict, Set

import websockets
from websockets.server import WebSocketServerProtocol

import database

# ============================================================
# Logging Setup
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pokereto")


# ============================================================
# Connection Manager
# ============================================================

class ConnectionManager:
    """Manages WebSocket connections keyed by bellID."""

    def __init__(self):
        # bellID -> WebSocket
        self.active: Dict[str, WebSocketServerProtocol] = {}

    async def connect(self, ws: WebSocketServerProtocol, bell_id: str):
        # If already connected with same ID, close the old connection
        if bell_id in self.active:
            old_ws = self.active[bell_id]
            try:
                await old_ws.close(reason="Replaced by new connection")
            except Exception:
                pass

        self.active[bell_id] = ws
        log.info(f"CONNECTED: {bell_id} (total: {len(self.active)})")

        # Send any pending offline messages
        await self.deliver_pending(bell_id, ws)

    def disconnect(self, bell_id: str):
        if bell_id in self.active:
            del self.active[bell_id]
            log.info(f"DISCONNECTED: {bell_id} (total: {len(self.active)})")

    async def deliver_pending(self, bell_id: str, ws: WebSocketServerProtocol):
        """Deliver offline messages when user reconnects."""
        pending = database.get_pending_messages(bell_id)
        if not pending:
            return

        log.info(f"OFFLINE DELIVERY: {bell_id} has {len(pending)} pending message(s)")

        for msg in pending:
            packet = {
                "from": msg["from_bell"],
                "to": msg["to_bell"],
                "code": msg["code"],
                "ts": msg["timestamp"],
                "offline": True,  # Flag: this was an offline message
            }
            try:
                await ws.send(json.dumps(packet))
                database.mark_delivered(msg["id"])
            except Exception as e:
                log.error(f"Failed to deliver offline message {msg['id']}: {e}")
                break

    async def send_to(self, bell_id: str, packet: dict) -> bool:
        """Send packet to specific user. Returns True if delivered."""
        if bell_id not in self.active:
            return False

        ws = self.active[bell_id]
        try:
            await ws.send(json.dumps(packet))
            return True
        except Exception as e:
            log.error(f"Failed to send to {bell_id}: {e}")
            return False

    async def broadcast(self, packet: dict, exclude: str = None):
        """Broadcast to all connected users (except sender)."""
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


# ============================================================
# Message Handler
# ============================================================

async def handle_message(ws: WebSocketServerProtocol, bell_id: str, raw: str):
    """Process incoming message."""
    try:
        packet = json.loads(raw)
    except json.JSONDecodeError:
        log.warning(f"Invalid JSON from {bell_id}: {raw[:80]}")
        return

    from_bell = packet.get("from", bell_id)
    to_bell = packet.get("to", "*")
    code = packet.get("code", "")
    ts = packet.get("ts", datetime.now().timestamp())

    if not code:
        log.warning(f"Empty code from {from_bell}")
        return

    log.info(f"MESSAGE: {from_bell} -> {to_bell}: {code}")

    # Save to database
    msg_id = database.save_message(
        from_bell=from_bell,
        to_bell=to_bell,
        code=code,
        timestamp=ts,
    )

    out_packet = {
        "from": from_bell,
        "to": to_bell,
        "code": code,
        "ts": ts,
    }

    if to_bell == "*":
        # Broadcast to all
        delivered = await manager.broadcast(out_packet, exclude=from_bell)
        log.info(f"BROADCAST: delivered to {delivered} recipient(s)")
        database.mark_delivered(msg_id)
    else:
        # Direct message
        if manager.is_online(to_bell):
            success = await manager.send_to(to_bell, out_packet)
            if success:
                database.mark_delivered(msg_id)
                log.info(f"DELIVERED: {from_bell} -> {to_bell}")
            else:
                log.info(f"PENDING: {to_bell} disconnected during send")
        else:
            log.info(f"OFFLINE: {to_bell} not connected, message stored")


# ============================================================
# Connection Handler
# ============================================================

async def handle_connection(ws: WebSocketServerProtocol):
    """Handle a WebSocket connection."""
    bell_id = None

    try:
        # Wait for hello message
        hello_raw = await asyncio.wait_for(ws.recv(), timeout=10)
        try:
            hello = json.loads(hello_raw)
        except json.JSONDecodeError:
            log.warning(f"Invalid hello message: {hello_raw[:80]}")
            await ws.close()
            return

        bell_id = hello.get("bellID") or hello.get("from")
        if not bell_id:
            log.warning("Hello message missing bellID")
            await ws.close()
            return

        # Register user
        database.register_user(bell_id)
        await manager.connect(ws, bell_id)

        # Send welcome
        await ws.send(json.dumps({
            "type": "welcome",
            "bellID": bell_id,
            "message": "Connected to PokeReto Cloud",
        }))

        # Message loop
        async for raw in ws:
            await handle_message(ws, bell_id, raw)

    except asyncio.TimeoutError:
        log.warning("Hello timeout")
    except websockets.ConnectionClosed:
        pass
    except Exception as e:
        log.error(f"Connection error: {e}", exc_info=True)
    finally:
        if bell_id:
            manager.disconnect(bell_id)


# ============================================================
# Health Check (for Render.com)
# ============================================================

async def health_check(path, request_headers):
    """HTTP health check endpoint for Render."""
    if path == "/health" or path == "/":
        return (200, [("Content-Type", "text/plain")], b"OK - PokeReto Server")
    return None


# ============================================================
# Main
# ============================================================

async def main():
    port = int(os.environ.get("PORT", 8080))
    host = "0.0.0.0"

    log.info(f"=" * 60)
    log.info(f"PokeReto Server starting on {host}:{port}")
    log.info(f"=" * 60)

    # Initialize database
    database.init_db()
    log.info("Database initialized")

    # Start WebSocket server
    async with websockets.serve(
        handle_connection,
        host,
        port,
        process_request=health_check,
        ping_interval=30,
        ping_timeout=10,
    ):
        log.info(f"Server ready and listening on ws://{host}:{port}")

        # Graceful shutdown
        stop = asyncio.Future()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set_result, None)
            except NotImplementedError:
                pass

        await stop
        log.info("Shutting down gracefully...")


if __name__ == "__main__":
    asyncio.run(main())
