"""Sprint 1, Script 06 — Import the 2026 budget from PTAC_BUD26_EBITDA.xlsx.

Three sheets used:
  - 'Tabelas'         (cost-center → property name index, ignored for now beyond label)
  - 'Dados por casa'  (property × month, pivoted into budget_lines_property)
  - 'Dados globais'   (company-level, into budget_lines_company)

'Dados por casa' layout: each property is a 12-row block:
  row 0: '###', '<Property Name>'  (block header)
  row 1: 'Taxa ocupação',   <annual>, jan, fev, ..., dez
  row 2: 'Numero reservas', <annual>, jan, fev, ..., dez
  row 3: 'Valor / dia',     ...
  row 4: 'Receita',         ...
  row 5: 'Proprietario',    ...
  row 6: 'Plataforma',      ...
  row 7: 'Publicidade',     ...
  row 8: 'Limpeza',         ...
  row 9: 'Manutenção',      ...
  row 10: 'Check-in',       ...
  row 11: 'On boarding',    ...
  row 12: 'Outros',         ...
  (blank)
  row 14: 'Margem',         ...
  row 15: 'Margem %',       ...

Each block is pivoted into 12 budget_lines_property rows (one per month).
Properties that match an existing properties row → property_id set.
Otherwise → placeholder_label set.

Idempotency: budget_lines_property has UNIQUE(budget_id, COALESCE(property_id::text, placeholder_label), year, month).
ON CONFLICT we update amounts in place. The budget header is also upserted on
UNIQUE(entity_id, fiscal_year, version_name).
"""
from __future__ import annotations

import re
import sys
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.db import connect, count_rows, find_source_file, get_entity_id
from common.excel_utils import (
    get_cell,
    open_workbook,
    to_decimal,
    to_str,
)
from common.logging_utils import setup_logging
from common.property_match import PropertyResolver

SOURCE_FILENAMES = ("PTAC_BUD26_EBITDA.xlsx",)
FISCAL_YEAR = 2026
VERSION_NAME = "BUD2026_v1"

# Column index of months in 'Dados por casa' (1-based). Jan starts at col 4.
MONTH_COLS = {m: 3 + m for m in range(1, 13)}  # 4..15

METRIC_TO_FIELD = {
    "TAXA OCUPACAO": "occupancy_pct",
    "TAXA OCUPAÇÃO": "occupancy_pct",
    "NUMERO RESERVAS": "num_reservations",
    "NÚMERO RESERVAS": "num_reservations",
    "VALOR / DIA": "avg_daily_rate",
    "VALOR/DIA": "avg_daily_rate",
    "RECEITA": "revenue_amount",
    "PROPRIETARIO": "owner_share_amount",
    "PROPRIETÁRIO": "owner_share_amount",
    "PLATAFORMA": "platform_fees_amount",
    "PUBLICIDADE": "marketing_amount",
    "MARKETING": "marketing_amount",
    "LIMPEZA": "cleaning_amount",
    "MANUTENCAO": "maintenance_amount",
    "MANUTENÇÃO": "maintenance_amount",
    "CHECK-IN": "checkin_amount",
    "CHECKIN": "checkin_amount",
    "ON BOARDING": "onboarding_amount",
    "ONBOARDING": "onboarding_amount",
    "OUTROS": "other_amount",
    "MARGEM": "margin_amount",
    "MARGEM %": "margin_pct",
}


def normalize_metric(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip().upper()
    return METRIC_TO_FIELD.get(s)


def parse_blocks(sheet) -> List[Tuple[str, Dict[str, Dict[int, Decimal]]]]:
    """Walks 'Dados por casa', returning a list of (property_name, {field: {month: value}}).
    A new block starts when col A contains a digit and col B has text.
    """
    blocks: List[Tuple[str, Dict[str, Dict[int, Decimal]]]] = []
    current_name: Optional[str] = None
    current: Dict[str, Dict[int, Decimal]] = {}

    for row in sheet.iter_rows(min_row=2):
        a = to_str(get_cell(row, 1))
        b = to_str(get_cell(row, 2))

        # Block header — col A has '###' value (a number), col B has the property name
        if a and a.isdigit() and b:
            if current_name and current:
                blocks.append((current_name, current))
            current_name = b
            current = {}
            continue

        if not b or not current_name:
            continue

        field = normalize_metric(b)
        if not field:
            continue

        # Capture the 12 monthly values
        per_month: Dict[int, Decimal] = {}
        for month, col_idx in MONTH_COLS.items():
            val = to_decimal(get_cell(row, col_idx))
            if val is not None:
                per_month[month] = val
        if per_month:
            current[field] = per_month

    if current_name and current:
        blocks.append((current_name, current))
    return blocks


def parse_globals(sheet) -> List[Tuple[str, int, Decimal]]:
    """Walk 'Dados globais' and return a list of (category, month, amount)."""
    out: List[Tuple[str, int, Decimal]] = []
    for row in sheet.iter_rows(min_row=4):  # header on row 3, data starts at 4
        b = to_str(get_cell(row, 2))
        if not b:
            continue
        category = b.upper().replace(" ", "_")
        for month, col_idx in MONTH_COLS.items():
            val = to_decimal(get_cell(row, col_idx))
            if val is None:
                continue
            out.append((category, month, val))
    return out


def main() -> int:
    log = setup_logging("import_budget_2026")
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

        before = {
            "budgets": count_rows(conn, "budgets"),
            "blp": count_rows(conn, "budget_lines_property"),
            "blc": count_rows(conn, "budget_lines_company"),
        }
        log.info(f"Rows before: {before}")

        wb = open_workbook(src)

        # Upsert budget header
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO budgets (entity_id, fiscal_year, version_name, status, source_file)
                VALUES (%s, %s, %s, 'DRAFT', %s)
                ON CONFLICT (entity_id, fiscal_year, version_name) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    updated_at = NOW()
                RETURNING id
                """,
                (entity_id, FISCAL_YEAR, VERSION_NAME, src.name),
            )
            res = cur.fetchone()
            budget_id = str(res[0])
            log.info(f"Budget header id={budget_id}")

        # ----- Dados por casa -----
        sheet_per_house = wb["Dados por casa"]
        blocks = parse_blocks(sheet_per_house)
        log.info(f"Parsed {len(blocks)} property blocks from 'Dados por casa'")

        property_lines = 0
        placeholder_count = 0

        with conn.cursor() as cur:
            for prop_name, fields in blocks:
                property_uuid = resolver.resolve(prop_name)
                placeholder = None if property_uuid else prop_name
                if placeholder:
                    placeholder_count += 1

                for month in range(1, 13):
                    args = {f: None for f in (
                        "occupancy_pct", "num_reservations", "avg_daily_rate",
                        "revenue_amount", "owner_share_amount", "platform_fees_amount",
                        "marketing_amount", "cleaning_amount", "maintenance_amount",
                        "checkin_amount", "onboarding_amount", "other_amount",
                        "margin_amount", "margin_pct",
                    )}
                    for f, per_month in fields.items():
                        if month in per_month:
                            args[f] = per_month[month]

                    if all(v is None for v in args.values()):
                        continue

                    cur.execute(
                        """
                        INSERT INTO budget_lines_property (
                            budget_id, property_id, placeholder_label, property_label,
                            year, month,
                            occupancy_pct, num_reservations, avg_daily_rate,
                            revenue_amount, owner_share_amount, platform_fees_amount,
                            marketing_amount, cleaning_amount, maintenance_amount,
                            checkin_amount, onboarding_amount, other_amount,
                            margin_amount, margin_pct
                        ) VALUES (%s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (budget_id, COALESCE(property_id::text, placeholder_label), year, month)
                        DO UPDATE SET
                            property_label = EXCLUDED.property_label,
                            occupancy_pct = EXCLUDED.occupancy_pct,
                            num_reservations = EXCLUDED.num_reservations,
                            avg_daily_rate = EXCLUDED.avg_daily_rate,
                            revenue_amount = EXCLUDED.revenue_amount,
                            owner_share_amount = EXCLUDED.owner_share_amount,
                            platform_fees_amount = EXCLUDED.platform_fees_amount,
                            marketing_amount = EXCLUDED.marketing_amount,
                            cleaning_amount = EXCLUDED.cleaning_amount,
                            maintenance_amount = EXCLUDED.maintenance_amount,
                            checkin_amount = EXCLUDED.checkin_amount,
                            onboarding_amount = EXCLUDED.onboarding_amount,
                            other_amount = EXCLUDED.other_amount,
                            margin_amount = EXCLUDED.margin_amount,
                            margin_pct = EXCLUDED.margin_pct
                        """,
                        (
                            budget_id, property_uuid, placeholder, prop_name,
                            FISCAL_YEAR, month,
                            args["occupancy_pct"], args["num_reservations"], args["avg_daily_rate"],
                            args["revenue_amount"], args["owner_share_amount"], args["platform_fees_amount"],
                            args["marketing_amount"], args["cleaning_amount"], args["maintenance_amount"],
                            args["checkin_amount"], args["onboarding_amount"], args["other_amount"],
                            args["margin_amount"], args["margin_pct"],
                        ),
                    )
                    property_lines += 1

        log.info(
            f"  budget_lines_property: upserted={property_lines} "
            f"(placeholders: {placeholder_count} of {len(blocks)} blocks)"
        )

        # ----- Dados globais -----
        try:
            sheet_global = wb["Dados globais"]
        except KeyError:
            sheet_global = None
            log.warning("Sheet 'Dados globais' not found — skipping company budget lines")

        company_lines = 0
        if sheet_global is not None:
            globals_data = parse_globals(sheet_global)
            log.info(f"Parsed {len(globals_data)} company-level cells from 'Dados globais'")
            with conn.cursor() as cur:
                # Idempotency: replace all company lines for this budget_id (no UNIQUE on the table)
                cur.execute("DELETE FROM budget_lines_company WHERE budget_id = %s", (budget_id,))
                for category, month, amount in globals_data:
                    cur.execute(
                        """
                        INSERT INTO budget_lines_company (
                            budget_id, year, month, category, amount
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (budget_id, FISCAL_YEAR, month, category, amount),
                    )
                    company_lines += 1
            log.info(f"  budget_lines_company: inserted={company_lines}")

        conn.commit()
        wb.close()

        after = {
            "budgets": count_rows(conn, "budgets"),
            "blp": count_rows(conn, "budget_lines_property"),
            "blc": count_rows(conn, "budget_lines_company"),
        }
        for t in before:
            log.info(f"  {t}: {before[t]} → {after[t]} (+{after[t] - before[t]})")
        log.info(f"Budget {VERSION_NAME} status=DRAFT — approve manually before Sprint 2.")
        return 0
    except Exception as exc:
        conn.rollback()
        log.exception(f"Import failed: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
