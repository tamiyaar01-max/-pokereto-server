import asyncio
import json
import logging
import os
import signal
import sys

import websockets
from websockets.server import WebSocketServerProtocol

# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | pokereto | %(message)s'
)
log = logging.getLogger("pokereto")

# ============================================================
# Config
# ============================================================
PORT = int(os.environ.get("PORT", "10000"))
HOST = "0.0.0.0"
HELLO_TIMEOUT = 15.0

# ============================================================
# In-memory state
# ============================================================
clients: dict[str, WebSocketServerProtocol] = {}

# ============================================================
# Handler
# ============================================================
async def handler(ws: WebSocketServerProtocol):
    bell_id = None
    peer = ws.remote_address
    log.info(f"CONNECT from {peer}")
    
    try:
        # Wait for HELLO
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
        
        # Receive loop
        async for message in ws:
            try:
                packet = json.loads(message)
                log.info(f"RECV {bell_id}: {packet}")
                
                target = packet.get("to", "*")
                
                if target == "*":
                    # Broadcast to all except sender
                    for cid, cws in list(clients.items()):
                        if cid != bell_id:
                            try:
                                await cws.send(message)
                                log.info(f"  -> sent to {cid}")
                            except Exception as e:
                                log.warning(f"  -> failed to {cid}: {e}")
                else:
                    # Direct send
                    target_ws = clients.get(target)
                    if target_ws:
                        try:
                            await target_ws.send(message)
                            log.info(f"  -> sent to {target}")
                        except Exception as e:
                            log.warning(f"  -> failed to {target}: {e}")
                    else:
                        log.info(f"  -> target {target} offline")
            except json.JSONDecodeError:
                log.warning(f"Invalid JSON from {bell_id}: {message[:100]}")
            except Exception as e:
                log.warning(f"Error processing message from {bell_id}: {e}")
    
    except websockets.ConnectionClosed:
        pass
    except Exception as e:
        log.warning(f"Handler error for {bell_id or peer}: {e}")
    finally:
        if bell_id and clients.get(bell_id) is ws:
            del clients[bell_id]
            log.info(f"DISCONNECT {bell_id} (active={len(clients)})")

# ============================================================
# Main
# ============================================================
async def main():
    log.info("=" * 60)
    log.info(f"PokeReto Server starting on {HOST}:{PORT}")
    log.info(f"Python: {sys.version.split()[0]}")
    log.info(f"websockets: {websockets.__version__}")
    log.info("=" * 60)
    log.info("Database initialized")
    log.info(f"Server ready: ws://{HOST}:{PORT}")
    
    # Graceful shutdown
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
