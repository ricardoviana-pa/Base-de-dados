"""Sprint 1.5 — De-duplicate cross-source properties and reservations.

Problem: the COMISSÕES seeder creates one property per RR/Guesty row even when
they refer to the same physical house (RR uses 'T1 - Ocean Bliss - Beach & BBQ'
while Guesty uses 'T1-Ocean Bliss'). This causes:
  (a) duplicate properties in the catalog
  (b) duplicate reservations across sources, double-counting revenue

Strategy:
  1) Group properties by an aggressive canonical key (lowercased, drop "by Portugal
     Active", "Heated Pool", parentheticals, MB/QF location suffixes, then strip
     non-alphanumerics) AND (tipologia, bedrooms). Within each group, pick the
     property with the most external IDs as canonical (prefer Guesty when tied).
  2) Re-point all FK references from non-canonical → canonical, copy missing
     external IDs, then delete the non-canonical row.
  3) After property merge, find reservations that share (property_id, checkin,
     checkout) across different source_systems. Keep the Guesty version as
     'current'; set effective_to=NOW() on the duplicate's current state and add
     a NOTE_ADDED event documenting the supersedence. The duplicate reservation
     row stays — only its current state is closed, so views that filter
     `effective_to IS NULL` automatically exclude it.

Idempotent: re-running finds 0 dupes. Wrapped in a single transaction so a
failure halfway leaves the DB unchanged.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.db import connect
from common.logging_utils import setup_logging

# All FK columns that reference properties.id (verified via information_schema)
PROPERTY_FK_REFS = [
    ("owner_contracts", "property_id"),
    ("property_tier_history", "property_id"),
    ("reservations", "property_id"),
    ("cleanings", "property_id"),
    ("laundry", "property_id"),
    ("property_expenses", "property_id"),
    ("pricing_decisions", "property_id"),
    ("pricelabs_snapshots", "property_id"),
    ("budget_lines_property", "property_id"),
    ("daily_property_snapshots", "property_id"),
    ("properties_pipeline", "property_id"),
]


def canonical_key(name: str) -> str:
    n = (name or "").lower()
    n = re.sub(r"\bby portugal active\b", "", n)
    n = re.sub(r"\bportugal active\b", "", n)
    n = re.sub(r"\bheated pool\b", "", n)
    n = re.sub(r"\bagroturismo\b", "", n)
    n = re.sub(r"\(.*?\)", "", n)
    n = re.sub(r"_moimenta da beira", "-mb", n)
    n = re.sub(r"\bmoimenta da beira\b", "-mb", n)
    n = re.sub(r"-qf\b", "", n)
    n = re.sub(r"-mb\b", "", n)
    n = re.sub(r"[^a-z0-9]", "", n)
    return n


def _do_property_merge(cur, canonical_id: str, victim_id: str) -> None:
    """Re-point all FK refs from victim to canonical, drop colliding rows on
    UNIQUE constraints, then delete the victim row.
    """
    # Copy IDs the victim has but canonical doesn't
    for col in ("rental_ready_id", "guesty_id", "doc_unico_id"):
        cur.execute(
            f"UPDATE properties SET {col} = COALESCE({col}, "
            f"(SELECT {col} FROM properties WHERE id = %s::uuid)) WHERE id = %s::uuid",
            (victim_id, canonical_id),
        )
    # Pre-delete victim rows that would collide on UNIQUE constraints
    cur.execute(
        """
        DELETE FROM owner_contracts WHERE property_id = %s::uuid
          AND effective_from IN (SELECT effective_from FROM owner_contracts WHERE property_id = %s::uuid)
        """,
        (victim_id, canonical_id),
    )
    cur.execute(
        """
        DELETE FROM budget_lines_property WHERE property_id = %s::uuid
          AND (budget_id, year, month) IN (
              SELECT budget_id, year, month FROM budget_lines_property WHERE property_id = %s::uuid
          )
        """,
        (victim_id, canonical_id),
    )
    cur.execute(
        """
        DELETE FROM daily_property_snapshots WHERE property_id = %s::uuid
          AND snapshot_date IN (SELECT snapshot_date FROM daily_property_snapshots WHERE property_id = %s::uuid)
        """,
        (victim_id, canonical_id),
    )
    # Re-point everything else
    for table, col in PROPERTY_FK_REFS:
        cur.execute(
            f"UPDATE {table} SET {col} = %s::uuid WHERE {col} = %s::uuid",
            (canonical_id, victim_id),
        )
    cur.execute("DELETE FROM properties WHERE id = %s::uuid", (victim_id,))


def merge_via_shared_reservations(conn, log) -> int:
    """Phase 1b — discover same-house pairs by reservation overlap.

    Two properties (one only-RR, one only-Guesty) refer to the same physical
    house if they have ≥1 reservation pair with same (checkin, checkout) and
    gross within 10%. Pick the best 1-1 mapping (highest shared count) and
    merge RR → Guesty. This is more reliable than name matching.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH rr_props AS (
                SELECT id FROM properties WHERE rental_ready_id IS NOT NULL AND guesty_id IS NULL
            ),
            gu_props AS (
                SELECT id FROM properties WHERE guesty_id IS NOT NULL AND rental_ready_id IS NULL
            )
            SELECT ar.property_id, br.property_id, COUNT(*)
            FROM reservation_states a
            JOIN reservations ar ON ar.id = a.reservation_id AND ar.source_system = 'rental_ready'
            JOIN reservation_states b
              ON b.checkin_date = a.checkin_date AND b.checkout_date = a.checkout_date
             AND b.effective_to IS NULL
            JOIN reservations br ON br.id = b.reservation_id AND br.source_system = 'guesty'
            WHERE a.effective_to IS NULL
              AND ar.property_id IN (SELECT id FROM rr_props)
              AND br.property_id IN (SELECT id FROM gu_props)
              AND a.gross_total > 0 AND b.gross_total > 0
              AND ABS(a.gross_total - b.gross_total) / GREATEST(a.gross_total, b.gross_total) < 0.10
            GROUP BY ar.property_id, br.property_id
            ORDER BY 3 DESC
        """)
        pairs = cur.fetchall()

    if not pairs:
        return 0

    # 1-1 mapping: each RR maps to the Guesty with most shared, no Guesty is reused
    best_for_rr: dict = {}
    for rr_pid, gu_pid, shared in pairs:
        rr_key, gu_key = str(rr_pid), str(gu_pid)
        if rr_key not in best_for_rr or best_for_rr[rr_key][1] < shared:
            best_for_rr[rr_key] = (gu_key, shared)
    by_gu: dict = {}
    for rr, (gu, shared) in best_for_rr.items():
        if gu not in by_gu or by_gu[gu][1] < shared:
            by_gu[gu] = (rr, shared)
    final = {by_gu[gu][0]: gu for gu in by_gu}

    log.info(f"  Discovered {len(final)} same-house pairs via shared reservations")
    with conn.cursor() as cur:
        for rr_id, gu_id in final.items():
            _do_property_merge(cur, canonical_id=gu_id, victim_id=rr_id)
    return len(final)


def merge_properties(conn, log) -> int:
    """Merge non-canonical properties into canonical ones. Returns # merged."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, display_name, tipologia, bedrooms, rental_ready_id, guesty_id, doc_unico_id
            FROM properties
        """)
        props = cur.fetchall()

    groups: dict = {}
    for p in props:
        pid, name, tipo, beds, rr, gu, du = p
        key = (canonical_key(name), tipo, beds)
        groups.setdefault(key, []).append({
            "id": str(pid), "name": name, "rr": rr, "gu": gu, "du": du,
        })

    merged = 0
    with conn.cursor() as cur:
        for key, members in groups.items():
            if len(members) < 2:
                continue
            members_sorted = sorted(members, key=lambda m: (
                -((1 if m["rr"] else 0) + (1 if m["gu"] else 0) + (1 if m["du"] else 0)),
                0 if m["gu"] else 1,
            ))
            canonical = members_sorted[0]
            others = members_sorted[1:]
            log.info(f"  Merging {len(others)} into canonical {canonical['name'][:40]}")

            for victim in others:
                _do_property_merge(cur, canonical_id=canonical["id"], victim_id=victim["id"])
                merged += 1
    return merged


def supersede_duplicate_reservations(conn, log) -> int:
    """Find pairs of reservations sharing (property_id, checkin, checkout) but
    different source_system, and close the current state on the non-Guesty side.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH ranked AS (
                SELECT r.id, r.source_system, r.property_id,
                       rs.id AS state_id, rs.checkin_date, rs.checkout_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY r.property_id, rs.checkin_date, rs.checkout_date
                           ORDER BY CASE r.source_system
                               WHEN 'guesty' THEN 1
                               WHEN 'rental_ready' THEN 2
                               WHEN 'doc_unico' THEN 3
                           END
                       ) AS rn,
                       COUNT(*) OVER (
                           PARTITION BY r.property_id, rs.checkin_date, rs.checkout_date
                       ) AS group_size
                FROM reservations r
                JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
                WHERE rs.status IN ('CONFIRMED', 'COMPLETED')
            )
            SELECT id, source_system, state_id
            FROM ranked
            WHERE group_size > 1 AND rn > 1
        """)
        victims = cur.fetchall()
        log.info(f"  Found {len(victims)} duplicate reservations to supersede")

        if not victims:
            return 0

        # Close their current state and add a NOTE_ADDED event
        cur.executemany(
            "UPDATE reservation_states SET effective_to = NOW() WHERE id = %s",
            [(v[2],) for v in victims],
        )
        cur.executemany(
            """
            INSERT INTO reservation_events (
                reservation_id, event_type, event_at, source_system, triggered_by, details
            ) VALUES (%s, 'NOTE_ADDED', NOW(), %s, 'DEDUPE_SCRIPT',
                     '{"reason": "superseded_by_canonical_source", "canonical": "guesty"}'::jsonb)
            """,
            [(v[0], v[1]) for v in victims],
        )
        return len(victims)


def main() -> int:
    log = setup_logging("dedupe")
    conn = connect()
    try:
        log.info("Step 1a: Merge duplicate properties by canonical name key")
        merged_a = merge_properties(conn, log)
        log.info(f"  → {merged_a} merged via name canonicalisation")

        log.info("Step 1b: Merge same-house pairs discovered by shared reservations")
        merged_b = merge_via_shared_reservations(conn, log)
        log.info(f"  → {merged_b} merged via reservation overlap")

        log.info("Step 2: Supersede duplicate reservations across sources")
        superseded = supersede_duplicate_reservations(conn, log)
        log.info(f"  → {superseded} reservation states marked superseded")

        conn.commit()

        # Validation
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM properties")
            n_props = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FILTER (WHERE rental_ready_id IS NOT NULL AND guesty_id IS NOT NULL)
                FROM properties
            """)
            n_both = cur.fetchone()[0]
            cur.execute("""
                SELECT EXTRACT(YEAR FROM rs.checkin_date)::INT y,
                       COUNT(*) reservas,
                       ROUND(SUM(rs.gross_total)::numeric, 0) gross
                FROM reservations r
                JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
                WHERE rs.status IN ('CONFIRMED','COMPLETED')
                  AND EXTRACT(YEAR FROM rs.checkin_date) BETWEEN 2024 AND 2026
                GROUP BY y ORDER BY y
            """)
            log.info("=" * 60)
            log.info(f"Properties: {n_props} ({n_both} have both RR + Guesty IDs)")
            log.info("Confirmed reservations by check-in year:")
            for r in cur.fetchall():
                log.info(f"  {r[0]}  {r[1]:>5} reservas  €{r[2]:>10,}")
        return 0
    except Exception as exc:
        conn.rollback()
        log.exception(f"Dedupe failed: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
