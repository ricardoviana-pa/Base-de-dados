"""Sprint 1, Script 04 — Import 2024-2025 reservations from
Accounting_RentalReady - NOVEMBRO - 2025 (1).xlsm, sheet 'Export'.

Each row → one row in `reservations` + one in `reservation_states` (current).
source_system = 'rental_ready', source_id = column 'ID'.

Property column is text like 'T2 - Divine Waves Duplex' — resolved via
PropertyResolver against properties.display_name + variants. If no match,
log orphan and skip.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.commissoes_seeder import seed_from_commissoes
from common.db import connect, count_rows, find_source_file, get_entity_id
from common.excel_utils import (
    get_cell,
    iter_data_rows,
    open_workbook,
    to_date,
    to_datetime,
    to_decimal,
    to_int,
    to_str,
)
from common.logging_utils import setup_logging
from common.property_match import PropertyResolver

SOURCE_FILENAMES = (
    "Accounting_RentalReady - NOVEMBRO - 2025 (1).xlsm",
    "Accounting_RentalReady - NOVEMBRO - 2025.xlsm",
    "Accounting_RentalReady_-_NOVEMBRO_-_2025__1_.xlsm",
)
SOURCE_SYSTEM = "rental_ready"

CHANNEL_MAP = {
    "AIRBNB": "AIRBNB",
    "BOOKING": "BOOKING",
    "BOOKING.COM": "BOOKING",
    "VRBO": "VRBO",
    "HOMEAWAY": "HOMEAWAY",
    "OLIVERS TRAVEL": "OLIVERS_TRAVEL",
    "HOLIDU": "HOLIDU",
    "PORTUGAL ACTIVE": "DIRECT",
    "PORTUGALACTIVE": "DIRECT",
    "DIRECT": "DIRECT",
    "MANUAL": "MANUAL",
}


def map_channel(raw: Optional[str]) -> str:
    if not raw:
        return "OTHER"
    return CHANNEL_MAP.get(raw.strip().upper(), "OTHER")


def main() -> int:
    log = setup_logging("import_excel_rr")
    src = find_source_file(*SOURCE_FILENAMES)
    if not src:
        log.error(f"Source file not found in any of: {SOURCE_FILENAMES}")
        return 1
    log.info(f"Source file: {src}")

    conn = connect()
    try:
        entity_id = get_entity_id(conn, "RTV")
        resolver = PropertyResolver.from_db(conn, entity_id)
        log.info(f"PropertyResolver loaded {len(list(resolver.all_known()))} aliases")

        before_resv = count_rows(conn, "reservations", "source_system = 'rental_ready'")
        log.info(f"reservations (rental_ready) before: {before_resv}")

        wb = open_workbook(src)

        # Pre-seed properties + owners + contracts from COMISSÕES — recovers
        # 'orphan' Export rows whose property name doesn't yet exist in the DB.
        if "COMISSÕES" in wb.sheetnames:
            seed_from_commissoes(conn, log, wb["COMISSÕES"], entity_id, "rental_ready_id")
            conn.commit()
            resolver = PropertyResolver.from_db(conn, entity_id)
            log.info(f"PropertyResolver re-loaded with {len(list(resolver.all_known()))} aliases")

        sheet = wb["Export"]
        log.info(f"Reading sheet 'Export' ({sheet.max_row} rows)")

        # Header on row 1 (1-based)
        headers = {}
        for c in sheet[1]:
            if c.value is not None:
                headers[str(c.value).strip()] = c.column

        def col(name: str) -> Optional[int]:
            return headers.get(name)

        inserted = updated = skipped = orphans = 0

        with conn.cursor() as cur:
            for row in iter_data_rows(sheet, header_row=1):
                created_at = to_datetime(get_cell(row, col("Created at") or 2))
                rr_id = to_str(get_cell(row, col("ID") or 3))
                platform = to_str(get_cell(row, col("Plataforma") or 4))
                platform_id = to_str(get_cell(row, col("ID da plataforma") or 5))
                property_label = to_str(get_cell(row, col("Property") or 6))
                guest_name = to_str(get_cell(row, col("Hóspede") or 7))
                checkin = to_date(get_cell(row, col("Data de início") or 8))
                checkout = to_date(get_cell(row, col("Data de fim") or 9))
                gross_paid = to_decimal(get_cell(row, col("Total Pago Pelo Hóspede") or 10))
                stay_no_vat = to_decimal(get_cell(row, col("TOTAL ESTADIA ANTES IVA") or 11))
                vat = to_decimal(get_cell(row, col("IVA") or 12))
                stay_value = to_decimal(get_cell(row, col("Valor da Estadia") or 13))
                received = to_decimal(get_cell(row, col("Valor recebido") or 14))
                commission = to_decimal(get_cell(row, col("Comissão") or 15))
                liquid = to_decimal(get_cell(row, col("Rendimento líquido") or 16))
                nights = to_int(get_cell(row, col("Número de noites") or 17))
                adults = to_int(get_cell(row, col("Número de adultos") or 18))

                if not rr_id or not checkin or not checkout:
                    skipped += 1
                    continue

                property_uuid = resolver.resolve(property_label or "")
                if not property_uuid:
                    orphans += 1
                    log.debug(f"Orphan rental_ready id={rr_id}: property '{property_label}' not matched")
                    continue

                if not created_at:
                    created_at = datetime.combine(checkin - timedelta(days=14), datetime.min.time())

                channel = map_channel(platform)

                # Snapshot pa_commission_rate at checkin
                cur.execute(
                    """
                    SELECT pa_commission_pct FROM owner_contracts
                    WHERE property_id = %s AND effective_from <= %s
                      AND (effective_to IS NULL OR effective_to >= %s)
                    ORDER BY effective_from DESC LIMIT 1
                    """,
                    (property_uuid, checkin, checkin),
                )
                r = cur.fetchone()
                pa_rate = r[0] if r else Decimal("0.40")

                # Guest stays NULL when there's no email. RR's Export sheet only carries
                # a free-text 'Hóspede' name; name-based dedup is unsafe (Sprint 2 problem).
                guest_uuid = None

                # Update rental_ready_id on property if not set yet (only if we used display_name match)
                cur.execute(
                    """
                    UPDATE properties SET rental_ready_id = %s
                    WHERE id = %s AND rental_ready_id IS NULL
                    """,
                    (property_label, property_uuid),
                )

                # Upsert reservation
                cur.execute(
                    """
                    INSERT INTO reservations (
                        entity_id, source_system, source_id, property_id, guest_id,
                        channel, booked_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_system, source_id) DO UPDATE SET
                        property_id = EXCLUDED.property_id,
                        guest_id = COALESCE(reservations.guest_id, EXCLUDED.guest_id),
                        channel = EXCLUDED.channel,
                        booked_at = EXCLUDED.booked_at
                    RETURNING id, (xmax = 0) AS is_insert
                    """,
                    (entity_id, SOURCE_SYSTEM, rr_id, property_uuid, guest_uuid,
                     channel, created_at),
                )
                res = cur.fetchone()
                reservation_id = str(res[0])
                if res[1]:
                    inserted += 1
                else:
                    updated += 1

                cur.execute(
                    "UPDATE reservation_states SET effective_to = NOW() WHERE reservation_id = %s AND effective_to IS NULL",
                    (reservation_id,),
                )

                raw = {
                    "platform": platform, "platform_id": platform_id,
                    "stay_value": float(stay_value) if stay_value else None,
                    "received": float(received) if received else None,
                    "liquid": float(liquid) if liquid else None,
                }

                cur.execute(
                    """
                    INSERT INTO reservation_states (
                        reservation_id, status, checkin_date, checkout_date,
                        adults, gross_total, vat_stay,
                        channel_commission, pa_commission_rate, effective_from,
                        source_system, raw_payload
                    ) VALUES (%s, 'CONFIRMED', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (reservation_id, checkin, checkout, adults,
                     gross_paid or stay_no_vat or 0, vat or 0,
                     abs(commission) if commission else 0, pa_rate, created_at,
                     SOURCE_SYSTEM, json.dumps(raw)),
                )

                cur.execute(
                    """
                    INSERT INTO reservation_events (
                        reservation_id, event_type, event_at, source_system, triggered_by
                    )
                    SELECT %s, 'BOOKED', %s, %s, 'IMPORT_RENTAL_READY'
                    WHERE NOT EXISTS (
                        SELECT 1 FROM reservation_events
                        WHERE reservation_id = %s AND event_type = 'BOOKED'
                    )
                    """,
                    (reservation_id, created_at, SOURCE_SYSTEM, reservation_id),
                )

        conn.commit()
        wb.close()

        after_resv = count_rows(conn, "reservations", "source_system = 'rental_ready'")
        log.info(f"reservations (rental_ready) after: {after_resv} (+{after_resv - before_resv})")
        log.info(f"  inserted={inserted} updated={updated} skipped={skipped} orphans={orphans}")
        return 0
    except Exception as exc:
        conn.rollback()
        log.exception(f"Import failed: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
