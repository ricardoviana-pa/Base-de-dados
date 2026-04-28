"""Seed properties + owners from the 'COMISSÕES' sheet of the Rental Ready / Guesty
template Excel files. This sheet is the canonical PA-side property list (one row
per property, with owner contact, tier, bedrooms, pool/garden flags, and
year-launched).

Running this BEFORE processing the 'Export' sheet makes the PropertyResolver
recognize every property by its display_name, recovering orphan reservations.

Source columns expected (1-based):
  A: PROPERTY (display_name, e.g. 'T2 - Divine Waves Duplex')
  B: COMMISSION RATE (PA share, e.g. 0.40)
  C: NOME OWNER
  D: MAIL OWNER
  E: CONSUMIVEIS (per-booking cost €)
  F: LAVANDARIA (per-booking cost €)
  G: LIMPEZA (per-booking cost €)
  H: TIER (STANDARD/PREMIUM/LUXURY)
  I: QUARTOS (bedrooms count)
  J: PISCINA (SIM/blank)
  K: JARDIM (SIM/blank)
  L: ANO LANÇAMENTO
  M: STATUS (ONLINE/OFFLINE)
  N: BCG (STAR/VACA/LIXO)
"""
from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from typing import Optional, Tuple

from .db import get_or_create_guest_by_email, upsert_owner
from .excel_utils import get_cell, iter_data_rows, to_decimal, to_int, to_str

TIER_ALIAS = {
    "STD": "STANDARD", "STANDARD": "STANDARD", "STA": "STANDARD",
    "PRE": "PREMIUM", "PREMIUM": "PREMIUM",
    "LUX": "LUXURY", "LUXURY": "LUXURY",
}


def _tipologia_from_name(name: str) -> Optional[str]:
    if not name:
        return None
    m = re.search(r"\bT\d+(?:\+\d)?\b", name.upper())
    return m.group(0) if m else None


def _yes(v) -> bool:
    s = (to_str(v) or "").upper().strip()
    return s in ("SIM", "YES", "Y", "S", "TRUE", "X", "1")


def _ensure_property(conn, entity_id: str, display_name: str,
                     owner_id: Optional[str], tier: Optional[str],
                     bedrooms: Optional[int], has_pool: bool, has_garden: bool,
                     onboarded_year: Optional[int],
                     source_id_field: str, source_id_value: str,
                     status: str) -> Tuple[str, bool]:
    """SELECT-then-INSERT/UPDATE on (entity_id, <source_id_field>=value).

    Returns (property_uuid, was_created).
    """
    if source_id_field not in ("rental_ready_id", "guesty_id"):
        raise ValueError(f"Unsupported source_id_field: {source_id_field}")

    tipologia = _tipologia_from_name(display_name)
    onboarded_at = date(onboarded_year, 1, 1) if onboarded_year else None

    with conn.cursor() as cur:
        # Try to find by source-specific ID first
        cur.execute(
            f"SELECT id FROM properties WHERE entity_id = %s AND {source_id_field} = %s LIMIT 1",
            (entity_id, source_id_value),
        )
        r = cur.fetchone()
        if r:
            pid = str(r[0])
            cur.execute(
                f"""
                UPDATE properties SET
                    canonical_name = COALESCE(canonical_name, %s),
                    display_name = %s,
                    tipologia = COALESCE(tipologia, %s),
                    bedrooms = COALESCE(bedrooms, %s),
                    has_pool = (has_pool OR %s),
                    has_garden = (has_garden OR %s),
                    current_tier = COALESCE(current_tier, %s::property_tier),
                    onboarded_at = COALESCE(onboarded_at, %s),
                    owner_id = COALESCE(owner_id, %s),
                    status = CASE WHEN status IN ('DRAFT','ONBOARDING') THEN %s::property_status ELSE status END,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (display_name, display_name, tipologia, bedrooms,
                 has_pool, has_garden, tier, onboarded_at, owner_id, status, pid),
            )
            return pid, False

        # Try by display_name (might already exist from Doc Único)
        cur.execute(
            "SELECT id FROM properties WHERE entity_id = %s AND display_name = %s LIMIT 1",
            (entity_id, display_name),
        )
        r = cur.fetchone()
        if r:
            pid = str(r[0])
            cur.execute(
                f"""
                UPDATE properties SET
                    {source_id_field} = COALESCE({source_id_field}, %s),
                    tipologia = COALESCE(tipologia, %s),
                    bedrooms = COALESCE(bedrooms, %s),
                    has_pool = (has_pool OR %s),
                    has_garden = (has_garden OR %s),
                    current_tier = COALESCE(current_tier, %s::property_tier),
                    onboarded_at = COALESCE(onboarded_at, %s),
                    owner_id = COALESCE(owner_id, %s),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (source_id_value, tipologia, bedrooms, has_pool, has_garden,
                 tier, onboarded_at, owner_id, pid),
            )
            return pid, False

        # Insert new
        cur.execute(
            f"""
            INSERT INTO properties (
                entity_id, canonical_name, display_name, {source_id_field},
                tipologia, bedrooms, has_pool, has_garden,
                current_tier, onboarded_at, owner_id, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::property_tier, %s, %s, %s::property_status)
            RETURNING id
            """,
            (entity_id, display_name, display_name, source_id_value,
             tipologia, bedrooms, has_pool, has_garden,
             tier, onboarded_at, owner_id, status),
        )
        return str(cur.fetchone()[0]), True


def _ensure_contract(conn, property_id: str, owner_id: Optional[str],
                     pa_pct: Optional[Decimal],
                     laundry: Optional[Decimal],
                     consumiveis: Optional[Decimal],
                     limpeza: Optional[Decimal]) -> bool:
    """Ensure there's an open-ended owner_contract for this property starting today
    (or earlier if earlier ones exist). Returns True if a new contract was inserted.
    """
    if pa_pct is None or owner_id is None:
        return False
    owner_pct = (Decimal("1") - pa_pct).quantize(Decimal("0.0001"))
    eff_from = date(2025, 1, 1)  # current contract era

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO owner_contracts (
                property_id, owner_id, effective_from, effective_to,
                pa_commission_pct, owner_commission_pct, owner_vat_rate,
                laundry_cost_per_booking, consumable_cost_per_booking,
                cleaning_fee_default
            ) VALUES (%s, %s, %s, NULL, %s, %s, 0, %s, %s, %s)
            ON CONFLICT (property_id, effective_from) DO UPDATE SET
                pa_commission_pct = EXCLUDED.pa_commission_pct,
                owner_commission_pct = EXCLUDED.owner_commission_pct,
                laundry_cost_per_booking = EXCLUDED.laundry_cost_per_booking,
                consumable_cost_per_booking = EXCLUDED.consumable_cost_per_booking,
                cleaning_fee_default = EXCLUDED.cleaning_fee_default,
                updated_at = NOW()
            RETURNING (xmax = 0) AS inserted
            """,
            (property_id, owner_id, eff_from, pa_pct, owner_pct, laundry, consumiveis, limpeza),
        )
        r = cur.fetchone()
        return bool(r[0]) if r else False


def seed_from_commissoes(conn, log, sheet, entity_id: str,
                         source_id_field: str) -> int:
    """Walk the COMISSÕES sheet and upsert one property + owner per row.

    `source_id_field` must be 'rental_ready_id' or 'guesty_id' — the source-specific
    ID column on properties that we'll use as identity for re-runs.
    """
    log.info(f"[COMISSÕES] reading {sheet.max_row} rows for {source_id_field}")
    created = updated = skipped = contracts_new = 0

    for row in iter_data_rows(sheet, header_row=1):
        property_label = to_str(get_cell(row, 1))
        commission_rate = to_decimal(get_cell(row, 2))
        owner_name = to_str(get_cell(row, 3))
        owner_email = to_str(get_cell(row, 4))
        consumiveis = to_decimal(get_cell(row, 5))
        lavandaria = to_decimal(get_cell(row, 6))
        limpeza = to_decimal(get_cell(row, 7))
        tier_raw = to_str(get_cell(row, 8))
        bedrooms = to_int(get_cell(row, 9))
        has_pool = _yes(get_cell(row, 10))
        has_garden = _yes(get_cell(row, 11))
        ano = to_int(get_cell(row, 12))
        status_raw = to_str(get_cell(row, 13))

        if not property_label:
            skipped += 1
            continue

        tier = TIER_ALIAS.get((tier_raw or "").upper().strip())
        status = "ACTIVE" if (status_raw or "").upper() == "ONLINE" else "PAUSED"

        # Owner — placeholder if no name given
        if owner_name:
            owner_id = upsert_owner(conn, entity_id, owner_name,
                                    notes="Imported from COMISSÕES sheet")
            # Link email if available
            if owner_email and "@" in owner_email:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE owners SET contact_email = COALESCE(contact_email, %s) WHERE id = %s",
                        (owner_email, owner_id),
                    )
        else:
            owner_id = upsert_owner(
                conn, entity_id,
                f"Owner of {property_label}",
                notes="Placeholder from COMISSÕES (owner name missing)",
            )

        property_id, was_created = _ensure_property(
            conn, entity_id, property_label, owner_id, tier, bedrooms,
            has_pool, has_garden, ano,
            source_id_field, property_label, status,
        )
        if was_created:
            created += 1
        else:
            updated += 1

        # Add the current-era contract
        if _ensure_contract(conn, property_id, owner_id, commission_rate,
                            lavandaria, consumiveis, limpeza):
            contracts_new += 1

    log.info(f"  → properties: created={created} updated={updated} skipped={skipped}; "
             f"contracts new/updated={contracts_new}")
    return created + updated
