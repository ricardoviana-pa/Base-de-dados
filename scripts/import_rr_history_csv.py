"""Sprint 1.12 — Import the live RR history dump (CSV) which is the
authoritative source for 2023-2025 (the RR era).

Why this is needed: the Excel `Accounting_RentalReady - NOVEMBRO - 2025` only
contains CONFIRMED reservations and is missing all cancellations + a number
of valid reservations the Looker dashboard counts. The CSV at
~/Downloads/guesty/data/rental_ready_history.csv is a direct dump of the RR
live database and contains:

  • 6,708 reservations across 2023-2026 (all statuses)
  • HOST STAY entries (owner stays, not revenue) — skipped
  • cancelled reservations identified by chiffre_affaire = 0

What this script does:
  1. For each existing reservation matched by rr_source_id, UPDATE state with
     fresh values + set status='CANCELLED' if cancelled in the CSV.
  2. For new reservations not yet imported (the cancellations + extras),
     INSERT them with proper status.

Match key: id (RR reservation ID, same as the Excel one).
Skips HOST STAY rows (owner stays, not revenue).
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.db import connect, count_rows, get_entity_id
from common.logging_utils import setup_logging
from common.property_match import PropertyResolver

CSV_PATH = Path.home() / "Downloads" / "guesty" / "data" / "rental_ready_history.csv"
SOURCE_SYSTEM = "rental_ready"

CHANNEL_MAP = {
    "AIRBNB": "AIRBNB", "BOOKING": "BOOKING", "BOOKING.COM": "BOOKING",
    "VRBO": "VRBO", "OLIVERS TRAVEL": "OLIVERS_TRAVEL", "HOLIDU": "HOLIDU",
    "HOLIDU_OLD": "HOLIDU", "PORTUGAL ACTIVE": "DIRECT", "PORTUGALACTIVE": "DIRECT",
    "DIRECT": "DIRECT", "MANUAL": "MANUAL",
}


def parse_date(s):
    if not s: return None
    s = s.split()[0]
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try: return datetime.strptime(s, fmt).date()
        except: pass
    return None


def parse_dt(s):
    if not s: return None
    for fmt in ("%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(s, fmt)
        except: pass
    return None


def parse_dec(s):
    if s is None or s == "": return None
    try: return Decimal(str(s).replace(",", "."))
    except: return None


def main() -> int:
    log = setup_logging("import_rr_history_csv")

    if not CSV_PATH.exists():
        log.error(f"CSV not found: {CSV_PATH}")
        return 1
    log.info(f"Source: {CSV_PATH}")

    conn = connect()
    try:
        entity_id = get_entity_id(conn, "RTV")
        resolver = PropertyResolver.from_db(conn, entity_id)

        # Build property lookup by display_name (for rental_pk_as_float matching)
        prop_by_rr_name = {}
        with conn.cursor() as cur:
            cur.execute("SELECT id, rental_ready_id, display_name FROM properties")
            for pid, rr_id, dn in cur.fetchall():
                if rr_id: prop_by_rr_name[str(rr_id).strip()] = str(pid)

        rows = list(csv.DictReader(CSV_PATH.open()))
        log.info(f"Read {len(rows)} CSV rows")

        before_resv = count_rows(conn, "reservations", "source_system='rental_ready'")
        log.info(f"reservations RR before: {before_resv}")

        skipped_host_stay = updated = inserted = cancelled_set = orphans = 0

        with conn.cursor() as cur:
            for row in rows:
                if row.get("sql_platform") == "HOST STAY":
                    skipped_host_stay += 1
                    continue

                rr_id = row.get("id", "").strip()
                if not rr_id: continue

                checkin  = parse_date(row.get("date_debut_reservation"))
                checkout = parse_date(row.get("date_fin_reservation"))
                if not checkin or not checkout: continue

                created_at = parse_dt(row.get("created_at"))
                if not created_at:
                    created_at = datetime.combine(checkin, datetime.min.time())

                gross    = parse_dec(row.get("chiffre_affaire"))
                cleaning = parse_dec(row.get("cleaning_fee"))
                commiss  = parse_dec(row.get("frais_plateforme"))
                tourist  = parse_dec(row.get("taxe_sejour"))
                guest_name = (row.get("sql_guest_fullname") or "").strip()

                # CANCELLED if gross is 0 or NULL
                is_cancelled = (gross is None or gross == 0)
                status = "CANCELLED" if is_cancelled else "CONFIRMED"

                # Property: rental_pk_as_float looks like "82705 - T2 - Bamboo AP Sea n'..."
                rr_prop_field = (row.get("rental_pk_as_float") or "").strip()
                # Try to match to existing property — first by exact rr_source name
                property_uuid = None
                # Strip the leading "<id> - " prefix
                if " - " in rr_prop_field:
                    name_part = rr_prop_field.split(" - ", 1)[1].strip()
                    property_uuid = resolver.resolve(name_part)
                if not property_uuid:
                    property_uuid = resolver.resolve(rr_prop_field)
                if not property_uuid:
                    orphans += 1
                    continue

                channel = CHANNEL_MAP.get(row.get("sql_platform","").upper(), "OTHER")

                # Snapshot pa_rate
                cur.execute(
                    "SELECT pa_commission_pct FROM owner_contracts WHERE property_id=%s "
                    "AND effective_from <= %s AND (effective_to IS NULL OR effective_to >= %s) "
                    "ORDER BY effective_from DESC LIMIT 1",
                    (property_uuid, checkin, checkin),
                )
                r = cur.fetchone()
                pa_rate = r[0] if r else Decimal("0.40")

                raw = {
                    "platform": row.get("sql_platform"),
                    "host_net_earnings": float(row["host_net_earnings"]) if row.get("host_net_earnings") else None,
                    "received_amount": float(row["sql_received_amount"]) if row.get("sql_received_amount") else None,
                    "pms_commission": float(row["pms_commission"]) if row.get("pms_commission") else None,
                    "tourist_tax": float(tourist) if tourist else None,
                    "host_full_name": row.get("sql_host_full_name"),
                    "guest_full_name": guest_name,
                    "id_plateforme": row.get("id_plateforme"),
                    "source": "rr_history_csv",
                }

                # UPSERT reservation header (rr_id is unique within source_system='rental_ready')
                cur.execute(
                    """
                    INSERT INTO reservations (
                        entity_id, source_system, source_id, rr_source_id,
                        property_id, channel, booked_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_system, source_id) DO UPDATE SET
                        rr_source_id = EXCLUDED.rr_source_id,
                        property_id = EXCLUDED.property_id,
                        channel = EXCLUDED.channel,
                        booked_at = LEAST(reservations.booked_at, EXCLUDED.booked_at)
                    RETURNING id, (xmax = 0) AS is_insert
                    """,
                    (entity_id, SOURCE_SYSTEM, rr_id, rr_id, property_uuid, channel, created_at),
                )
                res = cur.fetchone()
                reservation_id = str(res[0])
                if res[1]:
                    inserted += 1
                else:
                    updated += 1

                # Close any current state, insert fresh one with correct status
                cur.execute(
                    "UPDATE reservation_states SET effective_to=NOW() WHERE reservation_id=%s AND effective_to IS NULL",
                    (reservation_id,),
                )
                cur.execute(
                    """
                    INSERT INTO reservation_states (
                        reservation_id, status, checkin_date, checkout_date,
                        gross_total, cleaning_fee_gross, cleaning_fee_net,
                        channel_commission, pa_commission_rate, effective_from,
                        source_system, raw_payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (reservation_id, status, checkin, checkout,
                     gross or 0, cleaning or 0, cleaning or 0,
                     abs(commiss) if commiss else 0, pa_rate, created_at,
                     SOURCE_SYSTEM, json.dumps(raw)),
                )

                if is_cancelled:
                    cancelled_set += 1

        conn.commit()

        after_resv = count_rows(conn, "reservations", "source_system='rental_ready'")
        log.info(f"reservations RR after: {after_resv} (+{after_resv - before_resv})")
        log.info(f"  inserted={inserted} updated={updated} skipped_host_stay={skipped_host_stay} "
                 f"cancelled_set={cancelled_set} orphans={orphans}")

        # Per-year breakdown
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXTRACT(YEAR FROM rs.checkin_date)::INT y,
                       rs.status, COUNT(*),
                       ROUND(SUM(rs.gross_total)::numeric,0) gross,
                       ROUND(SUM(rs.cleaning_fee_gross)::numeric,0) cleaning
                FROM reservations r
                JOIN reservation_states rs ON rs.reservation_id=r.id AND rs.effective_to IS NULL
                WHERE r.source_system='rental_ready'
                  AND EXTRACT(YEAR FROM rs.checkin_date) BETWEEN 2022 AND 2027
                GROUP BY 1, 2 ORDER BY 1, 2
            """)
            log.info("Per year × status:")
            for r in cur.fetchall():
                log.info(f"  {r[0]} {r[1]:<10} {r[2]:>5}  gross €{r[3] or 0:>9,}  cleaning €{r[4] or 0:>5,}")
        return 0
    except Exception as exc:
        conn.rollback()
        log.exception(f"Import failed: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
