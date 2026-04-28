"""Sprint 1, Script 02 — Import cleaning service catalog, consumables baseline,
and laundry baseline from PA_Ops_Costs_final.xlsx.

Source sheets used:
  - 'Master'      → cleaning_service_catalog (matrix tier × tipologia × service_type)
  - 'Consumiveis' → consumables_baseline     (per-tipologia)
  - 'Lavandaria'  → laundry_baseline         (per-tipologia)

Master sheet header is on row 3 (1-based):
  A: Cat. (STD/PRE/LUX)
  B: Tipologia
  C: Horas
  D: Colab.
  E: s/ IVA
  F: c/ IVA 6%
  G: c/ IVA 23%
  H: Lav. (Sim/Nao)
  I: MO (s/IVA)
  J: Prod. Limpeza
  K: Transp.
  L: Tipo de Limpeza (free text — mapped to enum)

Consumiveis header on row 3:
  A: Artigo, ..., E..O: per-tipologia totals (T0..T10)

Lavandaria header on row 4:
  A: Tipologia, B: Quartos, C: Kg Roupa, D: Custo s/IVA, E: c/IVA 23%, F: c/IVA 6%

Idempotency: cleaning_service_catalog has UNIQUE(tier, tipologia, service_type, effective_from)
— we use today's effective_from. consumables_baseline / laundry_baseline have no UNIQUE
constraint, so we close existing open rows (effective_to=today) and insert fresh ones.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.db import connect, count_rows, find_source_file
from common.excel_utils import (
    get_cell,
    iter_data_rows,
    normalize_header,
    open_workbook,
    to_bool,
    to_decimal,
    to_int,
    to_str,
)
from common.logging_utils import setup_logging

SOURCE_FILENAMES = ("PA_Ops_Costs_final.xlsx",)

TIER_MAP = {
    "STD": "STANDARD", "STA": "STANDARD", "STANDARD": "STANDARD",
    "PRE": "PREMIUM",  "PREMIUM": "PREMIUM",
    "LUX": "LUXURY",   "LUXURY": "LUXURY",
}

# Map Tipo de Limpeza free text → cleaning_service_type enum.
# Matching is contains-based (case-insensitive), longest-substring-first.
SERVICE_TYPE_PATTERNS = [
    ("OUT/IN-", "OUT_IN_MINUS"),
    ("OUT/IN+", "OUT_IN_PLUS"),
    ("PERM+TC", "PERM_TC"),
    ("CO+L", "CO_L"),
    ("REFRESH", "REFRESH"),
    ("PERM", "PERM"),
    ("BEDS", "BEDS"),
    ("OBRA", "OBRA"),
    ("DEEP", "DEEP_CLEAN"),
    ("INSPECTION", "INSPECTION"),
    ("CO ", "CO"),  # last because "CO+L" / "OUT/IN+" already taken
    ("CO–", "CO"),
    ("CO-", "CO"),
]


def map_service_type(raw: str) -> str | None:
    if not raw:
        return None
    s = raw.upper()
    for needle, enum_val in SERVICE_TYPE_PATTERNS:
        if needle in s:
            return enum_val
    return None


def import_cleaning_catalog(conn, log, sheet, effective_from: date) -> int:
    """Master sheet → cleaning_service_catalog."""
    log.info(f"Reading 'Master' sheet ({sheet.max_row} rows)")
    inserted = updated = skipped = 0

    with conn.cursor() as cur:
        for row in iter_data_rows(sheet, header_row=3):
            cat_raw = to_str(get_cell(row, 1))
            tipologia = to_str(get_cell(row, 2))
            hours = to_decimal(get_cell(row, 3))
            staff = to_int(get_cell(row, 4))
            cost_net = to_decimal(get_cell(row, 5))
            cost_vat6 = to_decimal(get_cell(row, 6))
            cost_vat23 = to_decimal(get_cell(row, 7))
            has_laundry = to_bool(get_cell(row, 8))
            labor = to_decimal(get_cell(row, 9))
            products = to_decimal(get_cell(row, 10))
            transport = to_decimal(get_cell(row, 11))
            tipo_limpeza = to_str(get_cell(row, 12))

            # Header rows like "STANDARD"/"PREMIUM" appear with only col A populated.
            if not tipologia or not tipo_limpeza:
                skipped += 1
                continue

            tier = TIER_MAP.get((cat_raw or "").upper().strip())
            if not tier:
                log.debug(f"Skipped: unknown tier {cat_raw!r} for tipologia {tipologia!r}")
                skipped += 1
                continue

            service_type = map_service_type(tipo_limpeza)
            if not service_type:
                log.debug(f"Skipped: unmapped tipo_limpeza {tipo_limpeza!r}")
                skipped += 1
                continue

            if hours is None or staff is None or cost_net is None:
                log.debug(f"Skipped: missing core values for {tier}/{tipologia}/{service_type}")
                skipped += 1
                continue

            cur.execute(
                """
                INSERT INTO cleaning_service_catalog (
                    tier, tipologia, service_type, hours, staff_count, has_laundry,
                    labor_cost, cleaning_products_cost, transport_cost,
                    cost_net, cost_with_vat_6, cost_with_vat_23,
                    effective_from
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (tier, tipologia, service_type, effective_from) DO UPDATE SET
                    hours = EXCLUDED.hours,
                    staff_count = EXCLUDED.staff_count,
                    has_laundry = EXCLUDED.has_laundry,
                    labor_cost = EXCLUDED.labor_cost,
                    cleaning_products_cost = EXCLUDED.cleaning_products_cost,
                    transport_cost = EXCLUDED.transport_cost,
                    cost_net = EXCLUDED.cost_net,
                    cost_with_vat_6 = EXCLUDED.cost_with_vat_6,
                    cost_with_vat_23 = EXCLUDED.cost_with_vat_23
                RETURNING (xmax = 0) AS inserted
                """,
                (
                    tier, tipologia, service_type,
                    hours, staff, bool(has_laundry) if has_laundry is not None else False,
                    labor or 0, products or 0, transport or 0,
                    cost_net, cost_vat6 or 0, cost_vat23 or 0,
                    effective_from,
                ),
            )
            res = cur.fetchone()
            if res and res[0]:
                inserted += 1
            else:
                updated += 1

    log.info(f"  cleaning_service_catalog: inserted={inserted} updated={updated} skipped={skipped}")
    return inserted + updated


def import_consumables(conn, log, sheet, effective_from: date) -> int:
    """Consumiveis sheet → consumables_baseline. Each tipologia column (T0..T10) becomes
    one row with the column total as cost_per_booking_net.
    """
    log.info(f"Reading 'Consumiveis' sheet ({sheet.max_row} rows)")
    # Header row 3 (1-based). Tipologia headers from col E (5) onward, e.g. "T0\n(1 WC)".
    header_row_idx = 3
    headers = [c.value for c in sheet[header_row_idx]]

    tipologia_cols: list[tuple[str, int, int]] = []  # (tipologia_label, col_idx, bathrooms)
    for col_idx in range(5, len(headers) + 1):
        cell_val = headers[col_idx - 1]
        s = to_str(cell_val) or ""
        if s.upper().startswith("T"):
            label_first_line = s.split("\n", 1)[0].strip()
            bathrooms = 1
            for ch in s:
                if ch.isdigit() and "WC" in s.upper():
                    # parse "(N WC)"
                    pass
            # more reliable: extract digit before "WC"
            import re as _re
            m = _re.search(r"(\d+)\s*WC", s.upper())
            if m:
                bathrooms = int(m.group(1))
            tipologia_cols.append((label_first_line, col_idx, bathrooms))

    if not tipologia_cols:
        log.warning("  consumables: no tipologia columns detected, skipping")
        return 0

    # Sum totals per tipologia column over all data rows (skipping section dividers).
    totals: dict[str, tuple[int, "_Sum"]] = {}

    class _Sum:
        __slots__ = ("v",)
        def __init__(self): self.v = 0

    for label, _col, _br in tipologia_cols:
        totals[label] = (_br, _Sum())

    for row in iter_data_rows(sheet, header_row=header_row_idx):
        artigo = to_str(get_cell(row, 1)) or ""
        # Skip section dividers (── COZINHA ──, ── WC ──, etc.)
        if artigo.startswith("──") or not artigo:
            continue
        for label, col_idx, _br in tipologia_cols:
            val = to_decimal(get_cell(row, col_idx))
            if val is not None:
                _br_stored, summer = totals[label]
                summer.v += float(val)

    inserted = 0
    with conn.cursor() as cur:
        # Close any currently-open rows for these tipologias.
        cur.execute(
            "UPDATE consumables_baseline SET effective_to = %s WHERE effective_to IS NULL",
            (effective_from,),
        )
        for label, (bathrooms, summer) in totals.items():
            if summer.v <= 0:
                continue
            cur.execute(
                """
                INSERT INTO consumables_baseline (
                    tipologia, bathrooms, cost_per_booking_net,
                    effective_from, effective_to
                ) VALUES (%s, %s, %s, %s, NULL)
                """,
                (label, bathrooms, round(summer.v, 2), effective_from),
            )
            inserted += 1

    log.info(f"  consumables_baseline: inserted={inserted} new rows")
    return inserted


def import_laundry(conn, log, sheet, effective_from: date) -> int:
    """Lavandaria sheet → laundry_baseline."""
    log.info(f"Reading 'Lavandaria' sheet ({sheet.max_row} rows)")
    inserted = 0
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE laundry_baseline SET effective_to = %s WHERE effective_to IS NULL",
            (effective_from,),
        )
        for row in iter_data_rows(sheet, header_row=4):
            tipologia = to_str(get_cell(row, 1))
            bedrooms = to_int(get_cell(row, 2)) or 0
            kg = to_decimal(get_cell(row, 3))
            cost_net = to_decimal(get_cell(row, 4))
            cost_vat23 = to_decimal(get_cell(row, 5))
            cost_vat6 = to_decimal(get_cell(row, 6))
            note = to_str(get_cell(row, 7))

            if not tipologia or kg is None or cost_net is None:
                continue

            cur.execute(
                """
                INSERT INTO laundry_baseline (
                    tipologia, bedrooms, kg_roupa, cost_net,
                    cost_with_vat_6, cost_with_vat_23,
                    effective_from, effective_to, notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, %s)
                """,
                (
                    tipologia, bedrooms, kg, cost_net,
                    cost_vat6 or 0, cost_vat23 or 0,
                    effective_from, note,
                ),
            )
            inserted += 1

    log.info(f"  laundry_baseline: inserted={inserted} new rows")
    return inserted


def main() -> int:
    log = setup_logging("import_cleaning_catalog")
    src = find_source_file(*SOURCE_FILENAMES)
    if not src:
        log.error(f"Source file not found in any of: {SOURCE_FILENAMES}")
        return 1
    log.info(f"Source file: {src}")

    today = date.today()
    conn = connect()
    try:
        before_cat = count_rows(conn, "cleaning_service_catalog")
        before_cons = count_rows(conn, "consumables_baseline")
        before_laun = count_rows(conn, "laundry_baseline")
        log.info(f"Rows before: catalog={before_cat} consumables={before_cons} laundry={before_laun}")

        wb = open_workbook(src)

        # Resolve sheet names case-insensitively
        sheet_names = {normalize_header(n): n for n in wb.sheetnames}
        master = wb[sheet_names.get("master", "Master")]
        consumiveis = wb[sheet_names.get("consumiveis", "Consumiveis")]
        lavandaria = wb[sheet_names.get("lavandaria", "Lavandaria")]

        import_cleaning_catalog(conn, log, master, today)
        import_consumables(conn, log, consumiveis, today)
        import_laundry(conn, log, lavandaria, today)

        conn.commit()
        wb.close()

        after_cat = count_rows(conn, "cleaning_service_catalog")
        after_cons = count_rows(conn, "consumables_baseline")
        after_laun = count_rows(conn, "laundry_baseline")
        log.info(
            f"Rows after: catalog={after_cat} (+{after_cat - before_cat})  "
            f"consumables={after_cons} (+{after_cons - before_cons})  "
            f"laundry={after_laun} (+{after_laun - before_laun})"
        )
        return 0
    except Exception as exc:
        conn.rollback()
        log.exception(f"Import failed: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
