"""Sprint 1, Script 03 — Import legacy 2020-2024 data from
v30-DOCUMENTO ÚNICO_Release_2.1.1.xlsm.

Processes 7 sheets in order:
  1. LODGES_INFO                    → owners (placeholder), properties, owner_contracts
  2. CLIENT_INFO                    → guests
  3. INOUT_LODGES                   → reservations + reservation_states + reservation_events
  4. CLEAN                          → cleanings
  5. LAUNDRY                        → laundry
  6. LODGES_EXPENSES                → property_expenses
  7. INOUT EXPERIENCIES & SERVICES  → experience_bookings

All idempotent. Reservations are keyed on (source_system='doc_unico', source_id=Booking Number).
Properties are keyed on (entity_id, doc_unico_id).
Owners are placeholder ("Owner of <Building>") because Doc Único does not expose owner names —
Sprint 4 will reconcile against Primavera balancete (account 2782101xxx).

Strategy for blank/garbage rows: skip rows missing essential keys, log a warning, and continue.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.db import (
    connect, count_rows, find_source_file, get_entity_id,
    get_or_create_guest_by_email, upsert_owner, upsert_property_by_doc_unico,
)
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

SOURCE_FILENAMES = (
    "v30-DOCUMENTO ÚNICO_Release_2.1.1.xlsm",
    "v30-DOCUMENTO_U_NICO_Release_2_1_1.xlsm",
    "DOCUMENTO ÚNICO_Release_2.1.1.xlsm",
)
SOURCE_SYSTEM = "doc_unico"

# Booking Origin (Doc Único free text) → booking_channel enum
CHANNEL_MAP = {
    "AIRBNB": "AIRBNB",
    "BOOKING": "BOOKING",
    "BOOKING.COM": "BOOKING",
    "VRBO": "VRBO",
    "HOMEAWAY": "HOMEAWAY",
    "OLIVERS TRAVEL": "OLIVERS_TRAVEL",
    "OLIVERS": "OLIVERS_TRAVEL",
    "HOLIDU": "HOLIDU",
    "GETYOURGUIDE": "GETYOURGUIDE",
    "PORTUGALACTIVE": "DIRECT",
    "PORTUGAL ACTIVE": "DIRECT",
    "DIRECT": "DIRECT",
    "MANUAL": "MANUAL",
    "FEEL VIANA": "OTHER",
    "OUTROS LODGES": "OTHER",
    "GOOGLE": "DIRECT",
    "WEBSITE": "DIRECT",
    "SECRETPLACES": "OTHER",
}

# Doc Único Status free text → reservation_status enum
STATUS_MAP = {
    "CONFIRMED": "CONFIRMED",
    "BOOKED": "CONFIRMED",
    "PAID": "CONFIRMED",
    "COMPLETED": "COMPLETED",
    "FINISHED": "COMPLETED",
    "CHECKED-OUT": "COMPLETED",
    "CANCELLED": "CANCELLED",
    "CANCELED": "CANCELLED",
    "NO SHOW": "NO_SHOW",
    "NO-SHOW": "NO_SHOW",
    "PENDING": "PENDING",
}

CONTRACT_YEARS = [2020, 2021, 2022, 2023, 2024]


def map_channel(raw: Optional[str]) -> str:
    if not raw:
        return "OTHER"
    s = raw.strip().upper()
    return CHANNEL_MAP.get(s, "OTHER")


def map_status(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip().upper()
    if "CANCEL" in s:
        return "CANCELLED"
    if s in STATUS_MAP:
        return STATUS_MAP[s]
    if "NOT AVAILABLE" in s or s == "":
        return None
    return None


def derive_tipologia(building_or_name: str) -> Optional[str]:
    """Try to extract 'TN' / 'TN+1' from a property name. Returns None if not found."""
    if not building_or_name:
        return None
    m = re.search(r"\bT\d+(?:\+\d)?\b", building_or_name.upper())
    return m.group(0) if m else None


# ─────────────────────────── LODGES_INFO ───────────────────────────


def process_lodges_info(conn, log, sheet, entity_id: str) -> Dict[str, str]:
    """Upserts owners (placeholder), properties, and owner_contracts (5 yearly rows
    per property). Returns map {property_id_str: property_uuid}."""
    log.info(f"[LODGES_INFO] reading {sheet.max_row} rows")
    header_row = 2  # row 2 is header (1-based)
    headers = {}
    for c in sheet[header_row]:
        if c.value is not None:
            headers[str(c.value).strip()] = c.column

    def col(name: str) -> Optional[int]:
        return headers.get(name)

    property_map: Dict[str, str] = {}
    owners_created = 0
    properties_upserted = 0
    contracts_upserted = 0

    with conn.cursor() as cur:
        for row in iter_data_rows(sheet, header_row=header_row):
            mgmt_status = to_str(get_cell(row, col("Management Status") or 1))
            doc_id = to_str(get_cell(row, col("Property ID") or 2))
            building = to_str(get_cell(row, col("Building") or 3))
            city = to_str(get_cell(row, col("City") or 4))
            display_name = to_str(get_cell(row, col("Accommodation name") or 5)) or building
            rooms = to_int(get_cell(row, col("Rooms") or 6))
            guests_max = to_int(get_cell(row, col("Guests") or 7))

            if not doc_id or not display_name:
                continue

            tipologia = derive_tipologia(display_name) or derive_tipologia(building or "")
            status = "ACTIVE" if (mgmt_status or "").upper() == "ON" else "OFFBOARDED"

            # Placeholder owner — Sprint 4 will reconcile against Primavera 2782101xxx accounts.
            owner_legal_name = f"Owner of {building or display_name}".strip()
            owner_id = upsert_owner(
                conn, entity_id, owner_legal_name,
                notes="Placeholder created from Doc Único LODGES_INFO. Reconcile in Sprint 4.",
            )
            owners_created += 1

            property_uuid = upsert_property_by_doc_unico(
                conn, entity_id, doc_id,
                fields={
                    "canonical_name": display_name,
                    "display_name": display_name,
                    "building": building,
                    "city": city,
                    "max_guests": guests_max,
                    "bedrooms": rooms,
                    "tipologia": tipologia,
                    "owner_id": owner_id,
                    "status": status,
                },
            )
            properties_upserted += 1
            property_map[doc_id] = property_uuid

            # Owner contracts: one per year 2020-2024
            for year in CONTRACT_YEARS:
                pa_pct_raw = get_cell(row, col(f"{year} P.A. Comission [%]") or 0)
                owner_pct_raw = get_cell(row, col(f"{year} Owner Commission [%]") or 0)
                laundry_raw = get_cell(row, col(f"{year}_Laundry_Cost") or 0)
                consum_raw = get_cell(row, col(f"{year}_Consumable_Cost") or 0)
                vat_raw = get_cell(row, col(f"{year}_Owner_VAT") or 0)

                pa_pct = to_decimal(pa_pct_raw)
                owner_pct = to_decimal(owner_pct_raw)
                laundry_cost = to_decimal(laundry_raw)
                consum_cost = to_decimal(consum_raw)
                vat_rate = to_decimal(vat_raw)

                # Skip year if no usable data
                if pa_pct is None or owner_pct is None:
                    continue

                # Some rows have rounding noise like 0.4499... — coerce sum to 1 if close
                total = pa_pct + owner_pct
                if abs(total - Decimal("1")) > Decimal("0.01"):
                    log.debug(f"  contract {doc_id}/{year}: pct sum {total}, skipping")
                    continue
                # Force exact sum to 1 to satisfy CHECK constraint
                pa_pct = (Decimal("1") - owner_pct).quantize(Decimal("0.0001"))

                eff_from = date(year, 1, 1)
                eff_to = date(year, 12, 31)

                cur.execute(
                    """
                    INSERT INTO owner_contracts (
                        property_id, owner_id,
                        effective_from, effective_to,
                        pa_commission_pct, owner_commission_pct,
                        owner_vat_rate, laundry_cost_per_booking, consumable_cost_per_booking
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (property_id, effective_from) DO UPDATE SET
                        effective_to = EXCLUDED.effective_to,
                        pa_commission_pct = EXCLUDED.pa_commission_pct,
                        owner_commission_pct = EXCLUDED.owner_commission_pct,
                        owner_vat_rate = EXCLUDED.owner_vat_rate,
                        laundry_cost_per_booking = EXCLUDED.laundry_cost_per_booking,
                        consumable_cost_per_booking = EXCLUDED.consumable_cost_per_booking,
                        updated_at = NOW()
                    """,
                    (
                        property_uuid, owner_id,
                        eff_from, eff_to,
                        pa_pct, owner_pct,
                        vat_rate or 0, laundry_cost, consum_cost,
                    ),
                )
                contracts_upserted += 1

    log.info(
        f"  → owners +{owners_created}, properties upserted={properties_upserted}, "
        f"contracts inserted={contracts_upserted}"
    )
    return property_map


# ─────────────────────────── CLIENT_INFO ───────────────────────────


def process_client_info(conn, log, sheet) -> Dict[str, str]:
    """Returns map {client_id_str: guest_uuid}."""
    log.info(f"[CLIENT_INFO] reading {sheet.max_row} rows")
    header_row = 2
    guest_map: Dict[str, str] = {}
    upserted = skipped_no_email = 0

    for row in iter_data_rows(sheet, header_row=header_row):
        client_id = to_str(get_cell(row, 2))
        name = to_str(get_cell(row, 3))
        surname = to_str(get_cell(row, 4))
        country = to_str(get_cell(row, 6))
        city = to_str(get_cell(row, 7))
        phone = to_str(get_cell(row, 8))
        email = to_str(get_cell(row, 9))

        if not client_id or client_id.startswith("Delete"):
            continue

        full_name = " ".join(p for p in [name, surname] if p) or None
        country_code = None
        if country:
            cc_guess = country.strip()[:2].upper() if len(country.strip()) <= 3 else None
            country_code = cc_guess if cc_guess and cc_guess.isalpha() else None

        guest_uuid = get_or_create_guest_by_email(
            conn, email=email, name=full_name, phone=phone,
            country_code=country_code, city=city,
        )
        if guest_uuid:
            guest_map[client_id] = guest_uuid
            upserted += 1
        else:
            skipped_no_email += 1

    log.info(f"  → guests upserted={upserted}, skipped (no email): {skipped_no_email}")
    return guest_map


# ─────────────────────────── INOUT_LODGES ───────────────────────────


def process_inout_lodges(
    conn, log, sheet, entity_id: str,
    property_map: Dict[str, str],
    guest_map: Dict[str, str],
) -> int:
    """Reservations + reservation_states + reservation_events (BOOKED only)."""
    log.info(f"[INOUT_LODGES] reading {sheet.max_row} rows")
    header_row = 7  # 1-based; r6 in 0-based dump

    headers: Dict[str, int] = {}
    for c in sheet[header_row]:
        if c.value is not None:
            headers[str(c.value).strip()] = c.column

    def col(name: str) -> Optional[int]:
        return headers.get(name)

    inserted_resv = updated_resv = skipped = orphans = 0

    with conn.cursor() as cur:
        for row in iter_data_rows(sheet, header_row=header_row):
            booking_no = to_str(get_cell(row, col("Booking Number") or 2))
            doc_property_id = to_str(get_cell(row, col("Property ID") or 3))
            client_id = to_str(get_cell(row, col("Client ID") or 4))
            booked_at = to_datetime(get_cell(row, col("Reservation Date") or 6))
            status_raw = to_str(get_cell(row, col("Status") or 7))
            checkin = to_date(get_cell(row, col("Check-IN") or 11))
            checkout = to_date(get_cell(row, col("Check-OUT") or 12))
            adults = to_int(get_cell(row, col("Adults") or 20))
            children = to_int(get_cell(row, col("Children") or 21))
            babies = to_int(get_cell(row, col("Babies") or 22))
            booking_origin = to_str(get_cell(row, col("Booking Origin") or 17))
            gross_total_with_vat = to_decimal(get_cell(row, col("Booking Total (with VAT)") or 25))
            gross_total_no_vat = to_decimal(get_cell(row, col("Booking Total (without VAT)") or 26))
            portal_comm = to_decimal(get_cell(row, col("Portal Commission") or 27))
            portal_comm_pct = to_decimal(get_cell(row, col("Portal Commission [%]") or 28))

            if not booking_no or not doc_property_id or not checkin or not checkout:
                skipped += 1
                continue

            status = map_status(status_raw)
            if not status:
                skipped += 1
                continue
            # Filter out blocking calendar entries (multi-year fake rows)
            if checkin.year > 2030 or (checkout - checkin).days > 365:
                skipped += 1
                continue

            property_uuid = property_map.get(doc_property_id)
            if not property_uuid:
                orphans += 1
                log.debug(f"Orphan booking {booking_no}: property {doc_property_id} not found")
                continue

            guest_uuid = guest_map.get(client_id) if client_id else None
            channel = map_channel(booking_origin)

            if not booked_at:
                booked_at = datetime.combine(checkin - timedelta(days=14), datetime.min.time())

            gross_total = gross_total_with_vat if gross_total_with_vat is not None else (gross_total_no_vat or Decimal("0"))

            # Snapshot of contract in effect at checkin
            cur.execute(
                """
                SELECT pa_commission_pct FROM owner_contracts
                WHERE property_id = %s
                  AND effective_from <= %s
                  AND (effective_to IS NULL OR effective_to >= %s)
                ORDER BY effective_from DESC LIMIT 1
                """,
                (property_uuid, checkin, checkin),
            )
            r = cur.fetchone()
            pa_rate = r[0] if r else Decimal("0.40")  # fallback

            # Upsert reservation header
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
                (entity_id, SOURCE_SYSTEM, booking_no, property_uuid, guest_uuid,
                 channel, booked_at),
            )
            res = cur.fetchone()
            reservation_id = str(res[0])
            if res[1]:
                inserted_resv += 1
            else:
                updated_resv += 1

            # Build raw payload for state
            raw = {
                "booking_origin": booking_origin,
                "status_raw": status_raw,
                "portal_commission": float(portal_comm) if portal_comm else None,
                "portal_commission_pct": float(portal_comm_pct) if portal_comm_pct else None,
            }

            # Close any existing current state for this reservation, then insert new
            cur.execute(
                "UPDATE reservation_states SET effective_to = NOW() WHERE reservation_id = %s AND effective_to IS NULL",
                (reservation_id,),
            )
            cur.execute(
                """
                INSERT INTO reservation_states (
                    reservation_id, status, checkin_date, checkout_date,
                    adults, children, babies, gross_total,
                    channel_commission, channel_commission_pct,
                    pa_commission_rate, effective_from, effective_to,
                    source_system, raw_payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s::jsonb)
                """,
                (
                    reservation_id, status, checkin, checkout,
                    adults, children, babies, gross_total or 0,
                    portal_comm or 0, portal_comm_pct,
                    pa_rate, booked_at,
                    SOURCE_SYSTEM, json.dumps(raw),
                ),
            )

            # Booked event (idempotent: only insert if no BOOKED event for this reservation yet)
            cur.execute(
                """
                INSERT INTO reservation_events (
                    reservation_id, event_type, event_at, source_system, triggered_by
                )
                SELECT %s, 'BOOKED', %s, %s, 'IMPORT_DOC_UNICO'
                WHERE NOT EXISTS (
                    SELECT 1 FROM reservation_events
                    WHERE reservation_id = %s AND event_type = 'BOOKED'
                )
                """,
                (reservation_id, booked_at, SOURCE_SYSTEM, reservation_id),
            )

    log.info(
        f"  → reservations: inserted={inserted_resv} updated={updated_resv} "
        f"skipped={skipped} orphans={orphans}"
    )
    return inserted_resv + updated_resv


# ─────────────────────────── CLEAN ───────────────────────────


def process_cleanings(conn, log, sheet, property_map_by_lodge: Dict[str, str]) -> int:
    """CLEAN sheet → cleanings table. Lodge column is the Building name (string),
    so we resolve it via the lodge→property_uuid map.
    """
    log.info(f"[CLEAN] reading {sheet.max_row} rows")
    header_row = 3
    inserted = orphans = 0
    with conn.cursor() as cur:
        for row in iter_data_rows(sheet, header_row=header_row):
            clean_id = to_str(get_cell(row, 2))
            service = to_str(get_cell(row, 3))
            cdate = to_date(get_cell(row, 4))
            lodge = to_str(get_cell(row, 5))
            staff_count = to_int(get_cell(row, 11))
            cost_net = to_decimal(get_cell(row, 13))
            cost_gross = to_decimal(get_cell(row, 14))
            fuel = to_decimal(get_cell(row, 15))

            if not lodge or not cdate:
                continue
            property_uuid = property_map_by_lodge.get(lodge.upper())
            if not property_uuid:
                orphans += 1
                continue

            # Service text from Doc Único is free text — best-effort map
            stype = "CO_L"  # default; Sprint 2 will refine

            cur.execute(
                """
                INSERT INTO cleanings (
                    property_id, service_type, cleaning_date, staff_count,
                    cost_net, cost_gross, fuel_cost, notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (property_uuid, stype, cdate, staff_count, cost_net, cost_gross, fuel,
                 f"clean_id={clean_id}; service={service}"),
            )
            inserted += 1

    log.info(f"  → cleanings inserted={inserted} orphans={orphans}")
    return inserted


# ─────────────────────────── LAUNDRY ───────────────────────────


def process_laundry_sheet(conn, log, sheet, default_property_id: Optional[str]) -> int:
    """LAUNDRY sheet has no per-property column in the header dump — all rows are
    company-level laundry totals. We attach to the COWORK property if one exists,
    otherwise skip with a warning. Sprint 2 will refine.
    """
    log.info(f"[LAUNDRY] reading {sheet.max_row} rows")
    header_row = 3
    inserted = 0
    if not default_property_id:
        log.warning("  No default property to attach laundry rows to — skipping all")
        return 0
    with conn.cursor() as cur:
        for row in iter_data_rows(sheet, header_row=header_row):
            ldate = to_date(get_cell(row, 2))
            unit_price = to_decimal(get_cell(row, 6))
            total_paid = to_decimal(get_cell(row, 7))
            paid_flag = (to_str(get_cell(row, 8)) or "").upper()
            paid_at = to_date(get_cell(row, 9))
            invoice_date = to_date(get_cell(row, 10))
            notes = to_str(get_cell(row, 11))

            if not ldate or total_paid is None:
                continue

            cur.execute(
                """
                INSERT INTO laundry (
                    property_id, service_date, unit_price, total_paid,
                    paid_at, notes
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (default_property_id, ldate, unit_price, total_paid,
                 paid_at if paid_flag == "YES" else None, notes),
            )
            inserted += 1

    log.info(f"  → laundry inserted={inserted}")
    return inserted


# ─────────────────────────── LODGES_EXPENSES ───────────────────────────


def process_lodges_expenses(conn, log, sheet, property_map_by_lodge: Dict[str, str]) -> int:
    log.info(f"[LODGES_EXPENSES] reading {sheet.max_row} rows")
    header_row = 3
    inserted = orphans = 0
    with conn.cursor() as cur:
        for row in iter_data_rows(sheet, header_row=header_row):
            edate = to_date(get_cell(row, 2))
            lodge = to_str(get_cell(row, 3))
            etype = to_str(get_cell(row, 4))
            supplier = to_str(get_cell(row, 8))
            value_gross = to_decimal(get_cell(row, 9))
            value_net = to_decimal(get_cell(row, 10))
            paid = (to_str(get_cell(row, 11)) or "").upper()
            paid_at = to_date(get_cell(row, 12))
            responsible = to_str(get_cell(row, 13))
            invoice = to_str(get_cell(row, 14))
            notes = to_str(get_cell(row, 15))

            if not lodge or not edate or value_gross is None:
                continue

            property_uuid = property_map_by_lodge.get(lodge.upper())
            if not property_uuid:
                orphans += 1
                continue

            vat_amount = (value_gross - value_net) if (value_gross is not None and value_net is not None) else None

            cur.execute(
                """
                INSERT INTO property_expenses (
                    property_id, expense_date, category, supplier_name,
                    amount_gross, amount_net, vat_amount,
                    invoice_ref, paid_at, responsible_user, description
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    property_uuid, edate, (etype or "OTHER").upper(), supplier,
                    value_gross, value_net, vat_amount,
                    invoice, paid_at if paid in ("YES", "SIM") else None, responsible, notes,
                ),
            )
            inserted += 1

    log.info(f"  → property_expenses inserted={inserted} orphans={orphans}")
    return inserted


# ─────────────────────────── EXPERIENCES ───────────────────────────


def process_experiences(conn, log, sheet, entity_id: str) -> int:
    log.info(f"[INOUT EXPERIENCIES & SERVICES] reading {sheet.max_row} rows")
    header_row = 3
    inserted = unmatched = 0

    # Map Doc Único free text to seeded experience codes (best-effort)
    keyword_to_code = [
        ("HORSE", "HORSE_RIDING"),
        ("CANYON", "CANYONING"),
        ("CAN-AM", "CAN_AM_TOUR"),
        ("SAIL", "SAILING"),
        ("BIKE", "BIKE_TOUR"),
        ("BICY", "BIKE_TOUR"),
        ("CHEF", "PRIVATE_CHEF"),
        ("TRANSFER", "TRANSFER"),
        ("MASSAG", "MASSAGE"),
        ("YOGA", "YOGA"),
        ("GROCER", "GROCERY_SHOPPING"),
        ("HIKING", "HIKING_DIVING_DINNER"),
        ("DIVING", "HIKING_DIVING_DINNER"),
        ("EVENT", "EVENT"),
        ("TURIST", "TURISTIC_TOUR"),
        ("TOUR", "TURISTIC_TOUR"),
    ]

    with conn.cursor() as cur:
        # Cache experience code → uuid
        cur.execute("SELECT code, id FROM experiences")
        exp_lookup = {row[0]: str(row[1]) for row in cur.fetchall()}

        for row in iter_data_rows(sheet, header_row=header_row):
            activity = to_str(get_cell(row, 2))
            booking_date = to_date(get_cell(row, 3))
            service_date = to_date(get_cell(row, 4))
            name = to_str(get_cell(row, 8))
            pax = to_int(get_cell(row, 13))
            origin = to_str(get_cell(row, 15))
            value_per_pax_raw = get_cell(row, 17)

            if not activity or not booking_date:
                continue

            up = activity.upper()
            code = None
            for kw, exp_code in keyword_to_code:
                if kw in up:
                    code = exp_code
                    break
            if not code:
                # Use a generic "EVENT" bucket if not matched
                code = "EVENT"
                unmatched += 1

            exp_id = exp_lookup.get(code)
            if not exp_id:
                continue

            value_per_pax = to_decimal(value_per_pax_raw)
            total = (value_per_pax * pax) if (value_per_pax is not None and pax) else value_per_pax

            # experience_bookings.total_value is NUMERIC(8,2): cap at 999_999.99
            if total is not None and abs(total) >= Decimal("1000000"):
                log.warning(f"Capping experience total {total} for activity '{activity}' (exceeds NUMERIC(8,2))")
                total = None  # leave NULL rather than truncate silently

            cur.execute(
                """
                INSERT INTO experience_bookings (
                    entity_id, experience_id, booking_date, service_date, pax,
                    channel, total_value, guest_name
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (entity_id, exp_id, booking_date, service_date, pax,
                 map_channel(origin), total, name),
            )
            inserted += 1

    log.info(f"  → experience_bookings inserted={inserted} (unmatched activity types: {unmatched})")
    return inserted


# ─────────────────────────── main ───────────────────────────


def build_lodge_lookup(property_map: Dict[str, str], conn) -> Dict[str, str]:
    """Build a Building-name → property_uuid lookup (uppercased)."""
    out: Dict[str, str] = {}
    if not property_map:
        return out
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, building, display_name FROM properties WHERE id = ANY(%s)",
            (list(property_map.values()),),
        )
        for pid, building, dn in cur.fetchall():
            for label in [building, dn]:
                if label:
                    out[label.strip().upper()] = str(pid)
    return out


def main() -> int:
    log = setup_logging("import_doc_unico")
    src = find_source_file(*SOURCE_FILENAMES)
    if not src:
        log.error(f"Source file not found in any of: {SOURCE_FILENAMES}")
        return 1
    log.info(f"Source file: {src}")

    conn = connect()
    try:
        entity_id = get_entity_id(conn, "RTV")
        log.info(f"RTV entity_id = {entity_id}")

        before = {t: count_rows(conn, t) for t in
                  ("owners", "properties", "owner_contracts", "guests",
                   "reservations", "reservation_states", "cleanings", "laundry",
                   "property_expenses", "experience_bookings")}
        log.info(f"Rows before: {before}")

        wb = open_workbook(src)

        # Resolve sheets we care about
        wanted = {
            "LODGES_INFO": None,
            "CLIENT_INFO": None,
            "INOUT_LODGES": None,
            "CLEAN": None,
            "LAUNDRY": None,
            "LODGES_EXPENSES": None,
            "INOUT EXPERIENCIES & SERVICES": None,
        }
        for name in wb.sheetnames:
            if name in wanted:
                wanted[name] = wb[name]

        missing = [k for k, v in wanted.items() if v is None]
        if missing:
            log.warning(f"Missing sheets in source: {missing}")

        # 1) Properties + owners + contracts (foundational)
        property_map = process_lodges_info(conn, log, wanted["LODGES_INFO"], entity_id) \
            if wanted["LODGES_INFO"] else {}
        conn.commit()

        # 2) Guests
        guest_map = process_client_info(conn, log, wanted["CLIENT_INFO"]) \
            if wanted["CLIENT_INFO"] else {}
        conn.commit()

        # 3) Reservations
        if wanted["INOUT_LODGES"]:
            process_inout_lodges(conn, log, wanted["INOUT_LODGES"], entity_id, property_map, guest_map)
            conn.commit()

        # 4-6) Need lodge-name lookup
        lodge_lookup = build_lodge_lookup(property_map, conn)

        if wanted["CLEAN"]:
            process_cleanings(conn, log, wanted["CLEAN"], lodge_lookup)
            conn.commit()
        if wanted["LAUNDRY"]:
            # Try to find a 'COWORK' or similar default property
            default_pid = lodge_lookup.get("CONNECTED LODGE") or next(iter(property_map.values()), None)
            process_laundry_sheet(conn, log, wanted["LAUNDRY"], default_pid)
            conn.commit()
        if wanted["LODGES_EXPENSES"]:
            process_lodges_expenses(conn, log, wanted["LODGES_EXPENSES"], lodge_lookup)
            conn.commit()
        if wanted["INOUT EXPERIENCIES & SERVICES"]:
            process_experiences(conn, log, wanted["INOUT EXPERIENCIES & SERVICES"], entity_id)
            conn.commit()

        wb.close()

        after = {t: count_rows(conn, t) for t in before}
        for t in before:
            log.info(f"  {t}: {before[t]} → {after[t]} (+{after[t] - before[t]})")
        return 0
    except Exception as exc:
        conn.rollback()
        log.exception(f"Import failed: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
