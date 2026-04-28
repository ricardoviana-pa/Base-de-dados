"""Sprint 1, Script 01 — Import cost centers from Listagem Centros de Custo.xlsx
into the `cost_centers` table.

Source layout (row 1 is header):
  A: Centro de custo (4-digit short code, e.g. '0001')
  B: Descrição (e.g. 'RTV - T6 - ATLANTIC LODGE')
  C: Exercício (year, e.g. 2026)
  D: Tipo (e.g. 'M' — Movimento)
  E: Marker — 'saiu' = offboarded (active=FALSE), 'nova' = placeholder (skipped)

cc_type derivation:
  '0001'                    -> 'STRUCTURE'
  '0071'                    -> 'COWORK'
  '0072'                    -> 'PARK'
  any other 4-digit code    -> 'PROPERTY'

Idempotent via UPSERT on (entity_id, code_short, fiscal_year).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.db import connect, count_rows, find_source_file, get_entity_id
from common.excel_utils import (
    get_cell,
    iter_data_rows,
    open_workbook,
    to_int,
    to_str,
)
from common.logging_utils import setup_logging

SOURCE_FILENAMES = (
    "Listagem Centros de Custo.xlsx",
    "Listagem_Centros_de_Custo.xlsx",
)
DEFAULT_FISCAL_YEAR = 2026


def derive_cc_type(code_short: str) -> str:
    if code_short == "0001":
        return "STRUCTURE"
    if code_short == "0071":
        return "COWORK"
    if code_short == "0072":
        return "PARK"
    return "PROPERTY"


def main() -> int:
    log = setup_logging("import_cost_centers")

    src = find_source_file(*SOURCE_FILENAMES)
    if not src:
        log.error(f"Source file not found in any of: {SOURCE_FILENAMES}")
        return 1
    log.info(f"Source file: {src}")

    conn = connect()
    try:
        entity_id = get_entity_id(conn, "RTV")
        log.info(f"RTV entity_id = {entity_id}")
        rows_before = count_rows(conn, "cost_centers")
        log.info(f"cost_centers rows before: {rows_before}")

        wb = open_workbook(src)
        sheet = wb.active
        log.info(f"Reading sheet '{sheet.title}'")

        upserted = 0
        skipped_placeholders = 0
        marked_offboarded = 0

        with conn.cursor() as cur:
            for row in iter_data_rows(sheet, header_row=1):
                code_short = to_str(get_cell(row, 1))
                description = to_str(get_cell(row, 2))
                fiscal_year = to_int(get_cell(row, 3)) or DEFAULT_FISCAL_YEAR
                primavera_type = to_str(get_cell(row, 4)) or "M"
                marker = (to_str(get_cell(row, 5)) or "").lower()

                if not code_short or not description:
                    skipped_placeholders += 1
                    log.debug(f"Skipped row (no code/desc): code={code_short!r} desc={description!r}")
                    continue

                # Pad short numeric codes to 4 digits (Excel may strip leading zeros)
                if code_short.isdigit() and len(code_short) < 4:
                    code_short = code_short.zfill(4)

                active = marker != "saiu"
                if not active:
                    marked_offboarded += 1

                cc_type = derive_cc_type(code_short)

                cur.execute(
                    """
                    INSERT INTO cost_centers (
                        entity_id, code_short, description, cc_type,
                        fiscal_year, primavera_type, active
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (entity_id, code_short, fiscal_year) DO UPDATE SET
                        description = EXCLUDED.description,
                        cc_type = EXCLUDED.cc_type,
                        primavera_type = EXCLUDED.primavera_type,
                        active = EXCLUDED.active,
                        updated_at = NOW()
                    """,
                    (
                        entity_id,
                        code_short,
                        description,
                        cc_type,
                        fiscal_year,
                        primavera_type[:1],
                        active,
                    ),
                )
                upserted += 1

        conn.commit()
        wb.close()

        rows_after = count_rows(conn, "cost_centers")
        new_rows = rows_after - rows_before
        log.info(f"Upserted {upserted} rows ({marked_offboarded} marked inactive, {skipped_placeholders} placeholder rows skipped)")
        log.info(f"cost_centers rows after: {rows_after} (delta: +{new_rows})")

        # Validation summary
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cc_type, COUNT(*) FILTER (WHERE active) AS active,
                       COUNT(*) FILTER (WHERE NOT active) AS inactive
                FROM cost_centers WHERE entity_id = %s AND fiscal_year = %s
                GROUP BY cc_type ORDER BY cc_type
                """,
                (entity_id, DEFAULT_FISCAL_YEAR),
            )
            log.info("Distribution by cc_type:")
            for row in cur.fetchall():
                log.info(f"  {row[0]:10s}  active={row[1]:3d}  inactive={row[2]:3d}")

        return 0
    except Exception as exc:
        conn.rollback()
        log.exception(f"Import failed: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
