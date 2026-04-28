"""Sprint 1, Script 05 — Import 2026+ reservations from
Teste_Accounting_Guesty - 2026.xlsm, sheet 'Export'.

For Sprint 1 we ingest the historical Excel snapshot. Sprint 6 will replace this
with a live Guesty API pull (cron). Source IDs are alphanumeric Guesty hashes.

Mostly identical to import_excel_rr.py, with a different column order (no Agência,
'Hóspede' moved to col 3) and different source_system constant.
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
    "Teste_Accounting_Guesty - 2026.xlsm",
    "Teste_Accounting_Guesty_-_2026.xlsm",
)
SOURCE_SYSTEM = "guesty"

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
    log = setup_logging("import_guesty")
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

        before_resv = count_rows(conn, "reservations", "source_system = 'guesty'")
        log.info(f"reservations (guesty) before: {before_resv}")

        wb = open_workbook(src)

        if "COMISSÕES" in wb.sheetnames:
            seed_from_commissoes(conn, log, wb["COMISSÕES"], entity_id, "guesty_id")
            conn.commit()
            resolver = PropertyResolver.from_db(conn, entity_id)
            log.info(f"PropertyResolver re-loaded with {len(list(resolver.all_known()))} aliases")

        sheet = wb["Export"]
        log.info(f"Reading sheet 'Export' ({sheet.max_row} rows)")

        headers = {}
        for c in sheet[1]:
            if c.value is not None:
                headers[str(c.value).strip()] = c.column

        def col(name: str) -> Optional[int]:
            return headers.get(name)

        inserted = updated = skipped = orphans = 0

        with conn.cursor() as cur:
            for row in iter_data_rows(sheet, header_row=1):
                created_at = to_datetime(get_cell(row, col("Created at") or 1))
                guesty_id = to_str(get_cell(row, col("ID") or 2))
                guest_name = to_str(get_cell(row, col("Hóspede") or 3))
                platform = to_str(get_cell(row, col("Plataforma") or 4))
                platform_id = to_str(get_cell(row, col("ID da plataforma") or 5))
                property_label = to_str(get_cell(row, col("Property") or 6))
                checkin = to_date(get_cell(row, col("Data de início") or 7))
                checkout = to_date(get_cell(row, col("Data de fim") or 8))
                gross_paid = to_decimal(get_cell(row, col("Total Pago Pelo Hóspede") or 9))
                stay_no_vat = to_decimal(get_cell(row, col("TOTAL ESTADIA ANTES IVA") or 10))
                vat = to_decimal(get_cell(row, col("IVA") or 11))
                stay_value = to_decimal(get_cell(row, col("Valor da Estadia") or 12))
                received = to_decimal(get_cell(row, col("Valor recebido") or 13))
                commission = to_decimal(get_cell(row, col("Comissão") or 14))
                liquid = to_decimal(get_cell(row, col("Rendimento líquido") or 15))
                nights = to_int(get_cell(row, col("Número de noites") or 16))
                adults = to_int(get_cell(row, col("Número de adultos") or 17))
                children = to_int(get_cell(row, col("Número de crianças") or 18))

                if not guesty_id or not checkin or not checkout:
                    skipped += 1
                    continue

                property_uuid = resolver.resolve(property_label or "")
                if not property_uuid:
                    orphans += 1
                    log.debug(f"Orphan guesty id={guesty_id}: property '{property_label}' not matched")
                    continue

                if not created_at:
                    created_at = datetime.combine(checkin - timedelta(days=14), datetime.min.time())

                channel = map_channel(platform)

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

                # Guest stays NULL — Excel Guesty export has no email column.
                # Sprint 6's live Guesty API will populate guest_id with full guest data.
                guest_uuid = None

                # Note: 'platform_id' is the Airbnb/Booking RESERVATION ID (e.g. 'HM5NW4P8PK'),
                # not the Guesty PROPERTY ID. Storing it on properties.guesty_id was a Sprint 1
                # bug that caused dedupe to skip pairs like 'Portugal Active Cabedelo Beach Lodge'
                # ↔ 'T7-Cabedelo Lodge'. Real Guesty property IDs (hex hashes) only arrive in
                # Sprint 6 via the live API. Until then, the COMISSÕES seeder stores the
                # canonical property NAME in guesty_id as a placeholder marker.

                cur.execute(
                    """
                    INSERT INTO reservations (
                        entity_id, source_system, source_id, property_id, guest_id,
                        channel, booked_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_system, source_id) DO UPDATE SET
                        property_id = EXCLUDED.property_id,
                        guest_id = COALESCE(reservations.guest_id, EXCLUDED.guest_id),
                        channel = EXCLUDED.channel
                    RETURNING id, (xmax = 0) AS is_insert
                    """,
                    (entity_id, SOURCE_SYSTEM, guesty_id, property_uuid, guest_uuid,
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
                        adults, children, gross_total, vat_stay,
                        channel_commission, pa_commission_rate, effective_from,
                        source_system, raw_payload
                    ) VALUES (%s, 'CONFIRMED', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (reservation_id, checkin, checkout, adults, children,
                     gross_paid or stay_no_vat or 0, vat or 0,
                     abs(commission) if commission else 0, pa_rate, created_at,
                     SOURCE_SYSTEM, json.dumps(raw)),
                )

                cur.execute(
                    """
                    INSERT INTO reservation_events (
                        reservation_id, event_type, event_at, source_system, triggered_by
                    )
                    SELECT %s, 'BOOKED', %s, %s, 'IMPORT_GUESTY'
                    WHERE NOT EXISTS (
                        SELECT 1 FROM reservation_events
                        WHERE reservation_id = %s AND event_type = 'BOOKED'
                    )
                    """,
                    (reservation_id, created_at, SOURCE_SYSTEM, reservation_id),
                )

        conn.commit()
        wb.close()

        after_resv = count_rows(conn, "reservations", "source_system = 'guesty'")
        log.info(f"reservations (guesty) after: {after_resv} (+{after_resv - before_resv})")
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
