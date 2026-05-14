#!/usr/bin/env python3
"""
Database operations for PokeReto Server.
Uses PostgreSQL on cloud, SQLite on local.
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Optional

log = logging.getLogger("pokereto.db")

# ============================================================
# Database Connection
# ============================================================

DATABASE_URL = os.environ.get("DATABASE_URL", "")
USE_POSTGRES = DATABASE_URL.startswith("postgres")

if USE_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    log.info("Using PostgreSQL")
else:
    import sqlite3
    log.info("Using SQLite (local)")


def get_connection():
    """Get a database connection."""
    if USE_POSTGRES:
        # Render uses postgres:// but psycopg2 needs postgresql://
        url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        return psycopg2.connect(url)
    else:
        conn = sqlite3.connect("/tmp/pokereto.db")
        conn.row_factory = sqlite3.Row
        return conn


# ============================================================
# Schema Initialization
# ============================================================

def init_db():
    """Create tables if they don't exist."""
    conn = get_connection()
    cur = conn.cursor()

    if USE_POSTGRES:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                bell_id TEXT PRIMARY KEY,
                nickname TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                from_bell TEXT NOT NULL,
                to_bell TEXT NOT NULL,
                code TEXT NOT NULL,
                timestamp DOUBLE PRECISION NOT NULL,
                delivered BOOLEAN DEFAULT FALSE,
                delivered_at TIMESTAMP NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_pending
            ON messages (to_bell, delivered)
            WHERE delivered = FALSE
        """)
    else:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                bell_id TEXT PRIMARY KEY,
                nickname TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_bell TEXT NOT NULL,
                to_bell TEXT NOT NULL,
                code TEXT NOT NULL,
                timestamp REAL NOT NULL,
                delivered INTEGER DEFAULT 0,
                delivered_at TIMESTAMP NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_pending
            ON messages (to_bell, delivered)
        """)

    conn.commit()
    cur.close()
    conn.close()


# ============================================================
# User Operations
# ============================================================

def register_user(bell_id: str, nickname: str = None):
    """Register or update a user."""
    conn = get_connection()
    cur = conn.cursor()

    if USE_POSTGRES:
        cur.execute("""
            INSERT INTO users (bell_id, nickname, last_seen)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (bell_id) DO UPDATE
            SET last_seen = CURRENT_TIMESTAMP,
                nickname = COALESCE(EXCLUDED.nickname, users.nickname)
        """, (bell_id, nickname))
    else:
        cur.execute("""
            INSERT INTO users (bell_id, nickname, last_seen)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(bell_id) DO UPDATE
            SET last_seen = CURRENT_TIMESTAMP
        """, (bell_id, nickname))

    conn.commit()
    cur.close()
    conn.close()


# ============================================================
# Message Operations
# ============================================================

def save_message(from_bell: str, to_bell: str, code: str, timestamp: float) -> int:
    """Save a message and return its ID."""
    conn = get_connection()
    cur = conn.cursor()

    if USE_POSTGRES:
        cur.execute("""
            INSERT INTO messages (from_bell, to_bell, code, timestamp)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (from_bell, to_bell, code, timestamp))
        msg_id = cur.fetchone()[0]
    else:
        cur.execute("""
            INSERT INTO messages (from_bell, to_bell, code, timestamp)
            VALUES (?, ?, ?, ?)
        """, (from_bell, to_bell, code, timestamp))
        msg_id = cur.lastrowid

    conn.commit()
    cur.close()
    conn.close()
    return msg_id


def mark_delivered(msg_id: int):
    """Mark a message as delivered."""
    conn = get_connection()
    cur = conn.cursor()

    if USE_POSTGRES:
        cur.execute("""
            UPDATE messages
            SET delivered = TRUE, delivered_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (msg_id,))
    else:
        cur.execute("""
            UPDATE messages
            SET delivered = 1, delivered_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (msg_id,))

    conn.commit()
    cur.close()
    conn.close()


def get_pending_messages(bell_id: str) -> List[Dict]:
    """Get all undelivered messages for a user."""
    conn = get_connection()

    if USE_POSTGRES:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, from_bell, to_bell, code, timestamp
            FROM messages
            WHERE to_bell = %s AND delivered = FALSE
            ORDER BY timestamp ASC
        """, (bell_id,))
    else:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, from_bell, to_bell, code, timestamp
            FROM messages
            WHERE to_bell = ? AND delivered = 0
            ORDER BY timestamp ASC
        """, (bell_id,))

    rows = cur.fetchall()
    result = []
    for row in rows:
        if USE_POSTGRES:
            result.append(dict(row))
        else:
            result.append({
                "id": row["id"],
                "from_bell": row["from_bell"],
                "to_bell": row["to_bell"],
                "code": row["code"],
                "timestamp": row["timestamp"],
            })

    cur.close()
    conn.close()
    return result


def get_message_history(bell_id: str, limit: int = 50) -> List[Dict]:
    """Get message history for a user."""
    conn = get_connection()

    if USE_POSTGRES:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, from_bell, to_bell, code, timestamp, delivered
            FROM messages
            WHERE from_bell = %s OR to_bell = %s OR to_bell = '*'
            ORDER BY timestamp DESC
            LIMIT %s
        """, (bell_id, bell_id, limit))
    else:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, from_bell, to_bell, code, timestamp, delivered
            FROM messages
            WHERE from_bell = ? OR to_bell = ? OR to_bell = '*'
            ORDER BY timestamp DESC
            LIMIT ?
        """, (bell_id, bell_id, limit))

    rows = cur.fetchall()
    result = [dict(row) if USE_POSTGRES else {
        "id": row["id"],
        "from_bell": row["from_bell"],
        "to_bell": row["to_bell"],
        "code": row["code"],
        "timestamp": row["timestamp"],
        "delivered": bool(row["delivered"]),
    } for row in rows]

    cur.close()
    conn.close()
    return result
