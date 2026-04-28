"""Postgres connection helper for Portugal Active import scripts.

Centralises connection setup and a couple of helpers used by every script:
- `connect()` — returns a psycopg connection from SUPABASE_DB_CONNECTION_STRING
- `count_rows()` — used at start/end of each script to log delta
- `get_entity_id()` — resolves the RTV entity UUID once
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def connect() -> psycopg.Connection:
    """Open a new Postgres connection. Caller is responsible for closing."""
    dsn = os.environ.get("SUPABASE_DB_CONNECTION_STRING")
    if not dsn:
        raise RuntimeError(
            "SUPABASE_DB_CONNECTION_STRING is not set. "
            "Copy .env.example to .env and fill in the value."
        )
    conn = psycopg.connect(dsn, autocommit=False)
    with conn.cursor() as cur:
        cur.execute("SET application_name = 'pa_import_scripts'")
        # The audit trigger reads current_setting('app.current_user', TRUE).
        # SET with a dotted name is invalid syntax → use set_config().
        cur.execute("SELECT set_config('app.current_user', 'import_script', false)")
    return conn


def count_rows(conn: psycopg.Connection, table: str, where: str = "") -> int:
    sql = f"SELECT COUNT(*) FROM {table}"
    if where:
        sql += f" WHERE {where}"
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return int(row[0]) if row else 0


def get_entity_id(conn: psycopg.Connection, primavera_company_code: str = "RTV") -> str:
    """Returns the entity UUID for the given primavera_company_code. Required FK on
    almost every table.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM entities WHERE primavera_company_code = %s",
            (primavera_company_code,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                f"No entity with primavera_company_code='{primavera_company_code}' "
                "found. Did migration 002 run?"
            )
        return str(row[0])


def upsert_owner(conn: psycopg.Connection, entity_id: str, legal_name: str,
                 notes: Optional[str] = None) -> str:
    """SELECT-then-INSERT to dodge missing UNIQUE constraint on owners.

    Returns the owner UUID. Idempotent on (entity_id, legal_name).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM owners WHERE entity_id = %s AND legal_name = %s LIMIT 1",
            (entity_id, legal_name),
        )
        r = cur.fetchone()
        if r:
            return str(r[0])
        cur.execute(
            """
            INSERT INTO owners (entity_id, legal_name, is_company, active, notes)
            VALUES (%s, %s, FALSE, TRUE, %s) RETURNING id
            """,
            (entity_id, legal_name, notes),
        )
        return str(cur.fetchone()[0])


def upsert_property_by_doc_unico(
    conn: psycopg.Connection, entity_id: str, doc_unico_id: str,
    fields: dict,
) -> str:
    """SELECT-then-INSERT/UPDATE on (entity_id, doc_unico_id). Returns property UUID."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM properties WHERE entity_id = %s AND doc_unico_id = %s LIMIT 1",
            (entity_id, doc_unico_id),
        )
        r = cur.fetchone()
        if r:
            pid = str(r[0])
            sets = ", ".join(f"{k} = COALESCE(%s, {k})" for k in fields)
            cur.execute(
                f"UPDATE properties SET {sets}, updated_at = NOW() WHERE id = %s",
                (*fields.values(), pid),
            )
            return pid
        cols = ["entity_id", "doc_unico_id"] + list(fields)
        placeholders = ", ".join(["%s"] * len(cols))
        cur.execute(
            f"INSERT INTO properties ({', '.join(cols)}) VALUES ({placeholders}) RETURNING id",
            (entity_id, doc_unico_id, *fields.values()),
        )
        return str(cur.fetchone()[0])


def get_or_create_guest_by_email(conn: psycopg.Connection, email: Optional[str],
                                 name: Optional[str] = None,
                                 phone: Optional[str] = None,
                                 country_code: Optional[str] = None,
                                 city: Optional[str] = None) -> Optional[str]:
    """If email is provided, upsert by email_normalized (UNIQUE). If no email,
    return None — name-only matching is unsafe and is deferred to Sprint 2.
    """
    if not email or "@" not in email:
        return None
    # idx_guests_email is a partial unique index: ON guests(email_normalized) WHERE email_normalized IS NOT NULL.
    # ON CONFLICT must mirror the predicate to use it.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO guests (email_normalized, name, phone, country_code, city)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (email_normalized) WHERE email_normalized IS NOT NULL
            DO UPDATE SET
                name = COALESCE(guests.name, EXCLUDED.name),
                phone = COALESCE(guests.phone, EXCLUDED.phone),
                country_code = COALESCE(guests.country_code, EXCLUDED.country_code),
                city = COALESCE(guests.city, EXCLUDED.city)
            RETURNING id
            """,
            (email, name, phone, country_code, city),
        )
        return str(cur.fetchone()[0])


def source_data_dir() -> Path:
    """Where the source Excel files live. Defaults to ~/Downloads."""
    raw = os.environ.get("SOURCE_DATA_DIR")
    if raw:
        return Path(raw).expanduser()
    return Path.home() / "Downloads"


def find_source_file(*candidates: str) -> Optional[Path]:
    """Try each candidate filename in SOURCE_DATA_DIR, return first that exists."""
    base = source_data_dir()
    for name in candidates:
        p = base / name
        if p.exists():
            return p
    return None
