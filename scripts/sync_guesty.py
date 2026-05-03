"""Sprint 4 — Guesty Open API live sync.

Direct Python implementation (no Edge Function deploy needed for first run).
The Edge Function scaffold in supabase/functions/guesty-sync/ remains valid
for the daily cron — this script is for the initial full backfill and
ad-hoc re-syncs.

Usage:
  .venv/bin/python scripts/sync_guesty.py listings           # only listings → guesty_listing_map
  .venv/bin/python scripts/sync_guesty.py backfill           # full reservations backfill
  .venv/bin/python scripts/sync_guesty.py incremental [DATE] # since DATE (or last_sync)
  .venv/bin/python scripts/sync_guesty.py all                # listings + backfill (default)

What it does:

  Listings (always first):
    GET /v1/listings → upsert each into guesty_listing_map.
    Auto-attempts to link guesty_listing_id → property_id via fuzzy match
    on properties.canonical_name / display_name.

  Reservations:
    GET /v1/reservations with `since` filter for incremental.
    Each reservation upserts:
      • reservations row (source_system='guesty', source_id=Guesty _id,
                           guesty_source_id same)
      • reservation_states row with full money breakdown:
          gross_total      = money.fareAccommodation
          cleaning_fee_*   = money.fareCleaning
          channel_commission = money.commission (or hostServiceFee)
          vat_stay         = derived (~6% of fareAccommodation)
      • reservation_events row (BOOKED) when first seen
      • guests row enriched with country/email/phone (the missing piece!)

  Cancellations:
    Status mapping handles all Guesty statuses including 'canceled',
    'declined', 'denied', 'expired' → CANCELLED.

  Cross-source matching (preserve master-data integrity):
    For each Guesty reservation, before inserting, check if a row already
    exists with the same (property_id, checkin±1d, checkout±1d). If yes,
    just enrich it with guesty_source_id (don't double-count).

Rate limiting:
  Builds in 100ms inter-request delay + automatic backoff on 429.
  Token cached for its full TTL (24h).
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

import requests
from dotenv import load_dotenv

from common.db import connect, get_entity_id
from common.logging_utils import setup_logging
from common.property_match import PropertyResolver

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

GUESTY_API_BASE = "https://open-api.guesty.com/v1"
GUESTY_TOKEN_URL = "https://open-api.guesty.com/oauth2/token"
SOURCE_SYSTEM = "guesty"

# Guesty status → reservation_status enum
STATUS_MAP = {
    "confirmed": "CONFIRMED",
    "checked_in": "CONFIRMED",
    "checked_out": "COMPLETED",
    "completed": "COMPLETED",
    "canceled": "CANCELLED",
    "cancelled": "CANCELLED",
    "declined": "CANCELLED",
    "denied": "CANCELLED",
    "expired": "CANCELLED",
    "no_show": "NO_SHOW",
    "noshow": "NO_SHOW",
    "inquiry": "PENDING",
    "reserved": "PENDING",
    "tentative": "PENDING",
    "awaiting_payment": "PENDING",
}

# Guesty source/integration → booking_channel enum
CHANNEL_MAP = {
    "airbnb": "AIRBNB", "airbnb2": "AIRBNB", "airbnb_official": "AIRBNB",
    "booking.com": "BOOKING", "bookingcom": "BOOKING", "booking": "BOOKING",
    "vrbo": "VRBO", "homeaway": "HOMEAWAY",
    "manual": "MANUAL", "direct": "DIRECT", "website": "DIRECT",
    "guesty": "DIRECT", "internal": "DIRECT",
    "expedia": "EXPEDIA", "tripadvisor": "TRIPADVISOR", "google": "GOOGLE",
    "holidu": "HOLIDU", "olivers": "OLIVERS_TRAVEL", "olivers_travel": "OLIVERS_TRAVEL",
}


# ─── HTTP layer ────────────────────────────────────────────────────────────


class GuestyClient:
    def __init__(self, log):
        self.log = log
        self._token = None
        self._token_expires = 0
        self._session = requests.Session()

    def _get_token(self):
        if self._token and time.time() < self._token_expires - 60:
            return self._token
        r = requests.post(
            GUESTY_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": os.environ["GUESTY_CLIENT_ID"],
                "client_secret": os.environ["GUESTY_CLIENT_SECRET"],
                "scope": "open-api",
            },
            timeout=20,
        )
        r.raise_for_status()
        body = r.json()
        self._token = body["access_token"]
        self._token_expires = time.time() + int(body.get("expires_in", 3600))
        self.log.info(f"  OAuth token acquired (expires_in={body.get('expires_in')}s)")
        return self._token

    def get(self, path: str, params: dict | None = None) -> dict:
        url = f"{GUESTY_API_BASE}{path}"
        for attempt in range(5):
            token = self._get_token()
            r = self._session.get(
                url, headers={"Authorization": f"Bearer {token}"},
                params=params or {}, timeout=30,
            )
            if r.status_code == 429:
                wait = 2 ** attempt
                self.log.warning(f"  429 rate-limited on {path} — waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 401:
                self._token = None
                continue
            r.raise_for_status()
            time.sleep(0.1)  # gentle inter-request delay
            return r.json()
        raise RuntimeError(f"Failed after retries: {path}")

    def get_paginated(self, path: str, params: dict | None = None, page_size: int = 100):
        """Yield items across all pages of a Guesty list endpoint."""
        params = dict(params or {})
        params["limit"] = page_size
        params["skip"] = 0
        seen = 0
        while True:
            data = self.get(path, params)
            results = data.get("results") or []
            for item in results:
                yield item
            seen += len(results)
            total = data.get("count") or 0
            if seen >= total or not results:
                break
            params["skip"] += page_size


# ─── Helpers ───────────────────────────────────────────────────────────────


def to_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = str(s)
    try:
        # Guesty ISO format with Z
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def to_date_str(dt: Optional[datetime | str]):
    if not dt:
        return None
    if isinstance(dt, str):
        return dt[:10]
    return dt.date().isoformat()


def map_status(s: str) -> str:
    return STATUS_MAP.get((s or "").lower(), "CONFIRMED")


def map_channel(source: Optional[str], integration: Optional[dict]) -> str:
    candidates = []
    if integration and isinstance(integration, dict):
        if integration.get("platform"):
            candidates.append(str(integration["platform"]).lower())
        if integration.get("type"):
            candidates.append(str(integration["type"]).lower())
    if source:
        candidates.append(str(source).lower())
    for c in candidates:
        if c in CHANNEL_MAP:
            return CHANNEL_MAP[c]
        for k, v in CHANNEL_MAP.items():
            if k in c:
                return v
    return "OTHER"


def to_decimal(v) -> Optional[Decimal]:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


# ─── Phase 1: Listings sync ────────────────────────────────────────────────


def sync_listings(client: GuestyClient, conn, log, auto_create: bool = True) -> dict:
    log.info("=== Phase 1: Listings sync ===")
    entity_id = get_entity_id(conn, "RTV")
    resolver = PropertyResolver.from_db(conn, entity_id)

    listings = list(client.get_paginated(
        "/listings",
        params={"fields": "_id title active address tags type accommodates bedrooms bathrooms"},
    ))
    log.info(f"  Fetched {len(listings)} listings from Guesty")

    matched = auto_created = updated_existing_guesty = 0
    with conn.cursor() as cur:
        for L in listings:
            gid = L.get("_id")
            title = (L.get("title") or "").strip()
            active = bool(L.get("active", True))
            bedrooms = L.get("bedrooms")
            bathrooms = L.get("bathrooms")
            max_guests = L.get("accommodates")
            ptype = L.get("type")
            address = L.get("address") or {}
            city = address.get("city") if isinstance(address, dict) else None
            if not gid:
                continue

            property_id = resolver.resolve(title)

            if property_id:
                matched += 1
                cur.execute(
                    """
                    UPDATE properties SET
                        guesty_id = %s,
                        bedrooms = COALESCE(bedrooms, %s),
                        bathrooms = COALESCE(bathrooms, %s),
                        max_guests = COALESCE(max_guests, %s),
                        property_type = COALESCE(property_type, %s),
                        city = COALESCE(city, %s)
                    WHERE id = %s
                      AND (guesty_id IS NULL OR guesty_id NOT SIMILAR TO '[a-f0-9]{24}')
                    """,
                    (gid, bedrooms, bathrooms, max_guests, ptype, city, property_id),
                )
                updated_existing_guesty += 1
            elif auto_create:
                # Extract tipologia from name (regex T<N>)
                tip_match = re.search(r"\bT\d+(?:\+\d)?\b", title.upper())
                tipologia = tip_match.group(0) if tip_match else (
                    f"T{bedrooms}" if bedrooms else None
                )
                cur.execute(
                    """
                    INSERT INTO properties (
                        entity_id, canonical_name, display_name,
                        guesty_id, city, max_guests, bedrooms, bathrooms,
                        property_type, tipologia, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              CASE WHEN %s THEN 'ACTIVE'::property_status ELSE 'PAUSED'::property_status END)
                    RETURNING id
                    """,
                    (entity_id, title, title, gid, city, max_guests, bedrooms,
                     bathrooms, ptype, tipologia, active),
                )
                property_id = str(cur.fetchone()[0])
                auto_created += 1
                # Refresh resolver so next iteration can match if there's near-duplicate
                resolver.add_alias(title, property_id)

            cur.execute(
                """
                INSERT INTO guesty_listing_map (
                    guesty_listing_id, property_id, guesty_title, guesty_active, synced_at
                ) VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (guesty_listing_id) DO UPDATE SET
                    property_id = COALESCE(EXCLUDED.property_id, guesty_listing_map.property_id),
                    guesty_title = EXCLUDED.guesty_title,
                    guesty_active = EXCLUDED.guesty_active,
                    synced_at = NOW()
                """,
                (gid, property_id, title, active),
            )
    conn.commit()

    log.info(f"  → {len(listings)} listings: matched_existing={matched}, "
             f"auto_created={auto_created}, updated_with_guesty_id={updated_existing_guesty}")
    if auto_created:
        log.warning(f"  {auto_created} new properties created from Guesty listings — "
                    "Sprint 5 alias table will dedupe against existing")
    return {"listings": len(listings), "matched": matched, "auto_created": auto_created}


# ─── Phase 2: Reservations sync ────────────────────────────────────────────


def sync_reservations(client: GuestyClient, conn, log, since: Optional[str] = None) -> dict:
    log.info(f"=== Phase 2: Reservations sync (since={since or 'ALL'}) ===")
    entity_id = get_entity_id(conn, "RTV")

    # Build listing_id → property_id lookup
    with conn.cursor() as cur:
        cur.execute("SELECT guesty_listing_id, property_id FROM guesty_listing_map WHERE property_id IS NOT NULL")
        listing_map = dict(cur.fetchall())
    log.info(f"  Listing map: {len(listing_map)} listings → properties")

    # Open sync_log entry
    sync_log_id = None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_log (source_system, sync_type, started_at, status)
            VALUES ('guesty', %s, NOW(), 'running') RETURNING id
            """,
            ("incremental" if since else "full_backfill",),
        )
        sync_log_id = cur.fetchone()[0]
    conn.commit()

    fetched = 0
    inserted_resv = matched_existing = updated_existing = 0
    states_inserted = events_inserted = 0
    cancellations = 0
    skipped_unmapped = orphans = 0
    errors = []

    params = {
        "fields": ",".join([
            "_id", "status", "confirmationCode", "source", "integration",
            "listingId", "guestId", "guest", "checkIn", "checkOut", "nightsCount",
            "guestsCount", "createdAt", "money",
        ]),
    }
    if since:
        params["filters"] = json.dumps([{"field": "lastUpdatedAt", "operator": "$gte", "value": since}])

    try:
        for r in client.get_paginated("/reservations", params=params):
            fetched += 1
            try:
                ok = process_reservation(r, conn, listing_map, entity_id, log)
                if ok == "matched":
                    matched_existing += 1
                elif ok == "inserted":
                    inserted_resv += 1
                    states_inserted += 1
                    events_inserted += 1
                elif ok == "updated":
                    updated_existing += 1
                    states_inserted += 1
                elif ok == "skipped_unmapped":
                    skipped_unmapped += 1
                elif ok == "cancellation":
                    cancellations += 1
                # commit per-batch
                if fetched % 50 == 0:
                    conn.commit()
                    log.info(f"  Processed {fetched} reservations…")
            except Exception as e:
                errors.append(f"{r.get('_id')}: {e}")
                conn.rollback()
                if len(errors) < 5:
                    log.warning(f"  Error on {r.get('_id')}: {e}")

        conn.commit()

        # Update sync_log
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sync_log SET
                    finished_at = NOW(),
                    status = %s,
                    reservations_fetched = %s,
                    reservations_upserted = %s,
                    states_created = %s,
                    cancellations_detected = %s,
                    skipped_unmapped = %s,
                    error_message = %s,
                    metadata = %s::jsonb
                WHERE id = %s
                """,
                (
                    "success" if not errors else ("partial_failure" if len(errors) < fetched/2 else "error"),
                    fetched, inserted_resv + updated_existing + matched_existing,
                    states_inserted, cancellations, skipped_unmapped,
                    " | ".join(errors[:5]) if errors else None,
                    json.dumps({"matched_existing": matched_existing,
                                "inserted_new": inserted_resv,
                                "updated_existing": updated_existing,
                                "events_inserted": events_inserted,
                                "total_errors": len(errors)}),
                    sync_log_id,
                ),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sync_log SET finished_at=NOW(), status='error', error_message=%s WHERE id=%s",
                (str(e)[:1000], sync_log_id),
            )
        conn.commit()
        raise

    log.info(f"  → fetched={fetched} new={inserted_resv} matched_existing={matched_existing} "
             f"updated={updated_existing} cancellations={cancellations} unmapped={skipped_unmapped} "
             f"errors={len(errors)}")
    return {
        "fetched": fetched, "inserted": inserted_resv, "matched": matched_existing,
        "updated": updated_existing, "cancellations": cancellations,
        "skipped_unmapped": skipped_unmapped, "errors": len(errors),
    }


def process_reservation(r: dict, conn, listing_map: dict, entity_id: str, log) -> str:
    gid = r.get("_id")
    if not gid:
        return "skipped_unmapped"

    listing_id = r.get("listingId")
    property_id = listing_map.get(listing_id) if listing_id else None
    if not property_id:
        return "skipped_unmapped"

    status = map_status(r.get("status"))
    is_cancelled = status == "CANCELLED"

    checkin = to_dt(r.get("checkIn"))
    checkout = to_dt(r.get("checkOut"))
    if not checkin or not checkout:
        return "skipped_unmapped"

    booked_at = to_dt(r.get("createdAt")) or checkin
    channel = map_channel(r.get("source"), r.get("integration"))

    money = r.get("money") or {}
    gross_total      = to_decimal(money.get("fareAccommodation")) or Decimal("0")
    cleaning_fee     = to_decimal(money.get("fareCleaning")) or Decimal("0")
    channel_comm     = to_decimal(money.get("commission") or money.get("hostServiceFee")) or Decimal("0")
    # Estimate VAT @ 6% of fare accommodation (Portuguese AL standard)
    vat_stay = (gross_total * Decimal("0.06")).quantize(Decimal("0.01"))

    # Guest enrichment
    guest = r.get("guest") or {}
    guest_email = guest.get("email") or guest.get("emailAddress")
    guest_phone = guest.get("phone")
    guest_country = (guest.get("address") or {}).get("country") if guest.get("address") else None
    guest_city = (guest.get("address") or {}).get("city") if guest.get("address") else None
    guest_name = guest.get("fullName") or " ".join(
        x for x in [guest.get("firstName"), guest.get("lastName")] if x
    ) or None

    # Country code: Guesty often returns full country name; convert if 2-letter
    if guest_country:
        guest_country = str(guest_country).strip()
        if len(guest_country) > 2:
            # Common name → ISO map (small subset; Sprint 5 expands)
            COUNTRY_NAME_TO_ISO = {
                "portugal": "PT", "spain": "ES", "france": "FR", "germany": "DE",
                "united kingdom": "GB", "uk": "GB", "england": "GB",
                "netherlands": "NL", "italy": "IT", "switzerland": "CH",
                "united states": "US", "usa": "US", "us": "US",
                "brazil": "BR", "brasil": "BR", "ireland": "IE",
                "belgium": "BE", "luxembourg": "LU", "austria": "AT",
                "poland": "PL", "denmark": "DK", "sweden": "SE", "norway": "NO",
                "finland": "FI", "czech republic": "CZ", "canada": "CA",
                "australia": "AU", "israel": "IL", "south africa": "ZA",
                "russia": "RU", "ukraine": "UA",
            }
            iso = COUNTRY_NAME_TO_ISO.get(guest_country.lower())
            guest_country = iso if iso else guest_country[:2].upper()

    with conn.cursor() as cur:
        # ─── 0. If this Guesty ID is already imported (re-run), skip the
        #       cross-source match and just refresh state.
        cur.execute(
            """
            SELECT id FROM reservations
            WHERE source_system='guesty' AND source_id=%s
               OR guesty_source_id=%s
            LIMIT 1
            """,
            (gid, gid),
        )
        already = cur.fetchone()

        # ─── Guest upsert ─────────────────────────────────────────────────
        guest_uuid = None
        if guest_email and "@" in guest_email:
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
                (guest_email, guest_name, guest_phone, guest_country, guest_city),
            )
            guest_uuid = str(cur.fetchone()[0])

        # ─── If already imported (re-run), refresh state directly ───────
        if already:
            reservation_id = str(already[0])
            # Update channel/booked_at/guesty_source_id idempotently
            cur.execute(
                """
                UPDATE reservations SET
                    guesty_source_id = COALESCE(guesty_source_id, %s),
                    guest_id = COALESCE(guest_id, %s),
                    channel = COALESCE(channel, %s),
                    booked_at = LEAST(booked_at, %s)
                WHERE id = %s
                """,
                (gid, guest_uuid, channel, booked_at, reservation_id),
            )

            # Snapshot pa_rate
            cur.execute(
                """
                SELECT pa_commission_pct FROM owner_contracts
                WHERE property_id = %s AND effective_from <= %s
                  AND (effective_to IS NULL OR effective_to >= %s)
                ORDER BY effective_from DESC LIMIT 1
                """,
                (property_id, to_date_str(checkin), to_date_str(checkin)),
            )
            row = cur.fetchone()
            pa_rate = row[0] if row else Decimal("0.40")

            cur.execute(
                "UPDATE reservation_states SET effective_to = NOW() WHERE reservation_id = %s AND effective_to IS NULL",
                (reservation_id,),
            )
            raw = {
                "guesty_source": r.get("source"),
                "integration_platform": (r.get("integration") or {}).get("platform"),
                "confirmation_code": r.get("confirmationCode"),
                "host_payout": float(money.get("hostPayout") or 0),
                "guests_count": r.get("guestsCount"),
                "guesty_status_raw": r.get("status"),
            }
            cur.execute(
                """
                INSERT INTO reservation_states (
                    reservation_id, status, checkin_date, checkout_date,
                    gross_total, vat_stay, cleaning_fee_gross, cleaning_fee_net,
                    channel_commission, pa_commission_rate,
                    effective_from, source_system, raw_payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    reservation_id, status, to_date_str(checkin), to_date_str(checkout),
                    gross_total, vat_stay, cleaning_fee, cleaning_fee,
                    channel_comm, pa_rate, booked_at, SOURCE_SYSTEM,
                    json.dumps(raw),
                ),
            )
            return "cancellation" if is_cancelled else "updated"

        # ─── Cross-source match: existing reservation? ───────────────────
        # When the Guesty reservation is brand-new, look for an RR row that
        # matches by (property_id, checkin±1d, checkout±1d) and enrich it.
        cur.execute(
            """
            SELECT r.id FROM reservations r
            JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
            WHERE r.property_id = %s
              AND ABS(rs.checkin_date - %s::date) <= 1
              AND ABS(rs.checkout_date - %s::date) <= 1
              AND r.guesty_source_id IS NULL
              AND rs.status IN ('CONFIRMED', 'COMPLETED')
            ORDER BY ABS(rs.checkin_date - %s::date) + ABS(rs.checkout_date - %s::date)
            LIMIT 1
            """,
            (property_id, to_date_str(checkin), to_date_str(checkout),
             to_date_str(checkin), to_date_str(checkout)),
        )
        match = cur.fetchone()

        if match and not is_cancelled:
            # Existing row (e.g. RR) matches this Guesty reservation — enrich
            existing_id = str(match[0])
            cur.execute(
                """
                UPDATE reservations SET
                    guesty_source_id = %s,
                    guest_id = COALESCE(guest_id, %s)
                WHERE id = %s
                """,
                (gid, guest_uuid, existing_id),
            )
            return "matched"

        # ─── Snapshot pa_commission_rate at checkin ───────────────────────
        cur.execute(
            """
            SELECT pa_commission_pct FROM owner_contracts
            WHERE property_id = %s AND effective_from <= %s
              AND (effective_to IS NULL OR effective_to >= %s)
            ORDER BY effective_from DESC LIMIT 1
            """,
            (property_id, to_date_str(checkin), to_date_str(checkin)),
        )
        row = cur.fetchone()
        pa_rate = row[0] if row else Decimal("0.40")

        # ─── Reservation upsert ───────────────────────────────────────────
        cur.execute(
            """
            INSERT INTO reservations (
                entity_id, source_system, source_id, guesty_source_id,
                property_id, guest_id, channel, booked_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_system, source_id) DO UPDATE SET
                property_id = EXCLUDED.property_id,
                guesty_source_id = EXCLUDED.guesty_source_id,
                guest_id = COALESCE(reservations.guest_id, EXCLUDED.guest_id),
                channel = EXCLUDED.channel,
                booked_at = LEAST(reservations.booked_at, EXCLUDED.booked_at)
            RETURNING id, (xmax = 0) AS is_insert
            """,
            (entity_id, SOURCE_SYSTEM, gid, gid, property_id, guest_uuid, channel, booked_at),
        )
        res = cur.fetchone()
        reservation_id, is_new = str(res[0]), res[1]

        # Close any current state, insert fresh one
        cur.execute(
            "UPDATE reservation_states SET effective_to = NOW() WHERE reservation_id = %s AND effective_to IS NULL",
            (reservation_id,),
        )

        raw = {
            "guesty_source": r.get("source"),
            "integration_platform": (r.get("integration") or {}).get("platform"),
            "confirmation_code": r.get("confirmationCode"),
            "host_payout": float(money.get("hostPayout") or 0),
            "net_income_formula": money.get("netIncomeFormula"),
            "guests_count": r.get("guestsCount"),
            "guesty_status_raw": r.get("status"),
        }

        cur.execute(
            """
            INSERT INTO reservation_states (
                reservation_id, status, checkin_date, checkout_date,
                gross_total, vat_stay, cleaning_fee_gross, cleaning_fee_net,
                channel_commission, pa_commission_rate,
                effective_from, source_system, raw_payload
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                reservation_id, status, to_date_str(checkin), to_date_str(checkout),
                gross_total, vat_stay, cleaning_fee, cleaning_fee,
                channel_comm, pa_rate, booked_at, SOURCE_SYSTEM,
                json.dumps(raw),
            ),
        )

        if is_new:
            cur.execute(
                """
                INSERT INTO reservation_events (
                    reservation_id, event_type, event_at, source_system, triggered_by
                ) VALUES (%s, 'BOOKED', %s, %s, 'GUESTY_API_SYNC')
                """,
                (reservation_id, booked_at, SOURCE_SYSTEM),
            )
            return "cancellation" if is_cancelled else "inserted"
        else:
            return "cancellation" if is_cancelled else "updated"


# ─── Main ──────────────────────────────────────────────────────────────────


def main() -> int:
    log = setup_logging("sync_guesty")
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    since_arg = sys.argv[2] if len(sys.argv) > 2 else None

    if mode not in ("listings", "backfill", "incremental", "all"):
        log.error(f"Unknown mode: {mode} (use listings|backfill|incremental|all)")
        return 1

    conn = connect()
    client = GuestyClient(log)

    try:
        if mode in ("listings", "all"):
            sync_listings(client, conn, log)

        if mode == "backfill" or mode == "all":
            sync_reservations(client, conn, log, since=None)
        elif mode == "incremental":
            since = since_arg
            if not since:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT TO_CHAR(MAX(finished_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') "
                        "FROM sync_log WHERE source_system='guesty' AND status='success'"
                    )
                    row = cur.fetchone()
                    since = row[0] if row and row[0] else None
            sync_reservations(client, conn, log, since=since)

        log.info("✓ Guesty sync complete")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
