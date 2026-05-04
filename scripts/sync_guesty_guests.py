"""Enrich Guesty-sourced guests with full address + email + phone.

The /reservations endpoint returns minimal guest info (name + phone). To get
country, city, email we must query /guests/{id} per guest. This script does
that for every guest linked to a Guesty reservation that lacks country.

Usage:
  .venv/bin/python scripts/sync_guesty_guests.py [--limit N]

Pulls each unique guesty guestId once, updates guests.country_code/city/email/phone.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import json
import requests
from dotenv import load_dotenv

from common.db import connect, get_or_create_guest_by_email
from common.logging_utils import setup_logging
from sync_guesty import GuestyClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

# Country name → ISO 3166-1 alpha-2
COUNTRY_NAME_TO_ISO = {
    "portugal":"PT","spain":"ES","españa":"ES","france":"FR","germany":"DE","deutschland":"DE",
    "united kingdom":"GB","uk":"GB","england":"GB","great britain":"GB","scotland":"GB","wales":"GB",
    "ireland":"IE","netherlands":"NL","holland":"NL","italy":"IT","italia":"IT",
    "switzerland":"CH","schweiz":"CH","suiza":"CH","suisse":"CH",
    "united states":"US","usa":"US","u.s.":"US","u.s.a.":"US",
    "canada":"CA","brazil":"BR","brasil":"BR","mexico":"MX","argentina":"AR","chile":"CL","colombia":"CO",
    "belgium":"BE","luxembourg":"LU","austria":"AT","österreich":"AT",
    "poland":"PL","polska":"PL","denmark":"DK","danmark":"DK",
    "sweden":"SE","sverige":"SE","norway":"NO","norge":"NO",
    "finland":"FI","suomi":"FI","czech republic":"CZ","czechia":"CZ",
    "australia":"AU","new zealand":"NZ","japan":"JP","china":"CN","south korea":"KR","korea":"KR",
    "israel":"IL","south africa":"ZA","russia":"RU","ukraine":"UA",
    "greece":"GR","romania":"RO","hungary":"HU","slovakia":"SK","slovenia":"SI","croatia":"HR",
    "bulgaria":"BG","serbia":"RS","turkey":"TR","cyprus":"CY","malta":"MT",
    "iceland":"IS","estonia":"EE","latvia":"LV","lithuania":"LT","albania":"AL",
    "morocco":"MA","tunisia":"TN","egypt":"EG","united arab emirates":"AE","uae":"AE",
    "saudi arabia":"SA","india":"IN","singapore":"SG","hong kong":"HK","taiwan":"TW",
    "thailand":"TH","philippines":"PH","indonesia":"ID","malaysia":"MY","vietnam":"VN",
}


def normalize_country(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    if len(s) == 2 and s.isalpha():
        return s.upper()
    iso = COUNTRY_NAME_TO_ISO.get(s.lower())
    if iso:
        return iso
    # Fallback: first two letters uppercase
    return s[:2].upper() if len(s) >= 2 else None


def main() -> int:
    log = setup_logging("sync_guesty_guests")
    limit = None
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        if idx + 1 < len(sys.argv):
            limit = int(sys.argv[idx + 1])

    conn = connect()
    client = GuestyClient(log)

    try:
        # Find all reservations with Guesty source_id but no guest_id linked
        # OR with a linked guest who lacks country_code
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT r.guesty_source_id
                FROM reservations r
                LEFT JOIN guests g ON g.id = r.guest_id
                WHERE r.guesty_source_id IS NOT NULL
                  AND (g.id IS NULL OR g.country_code IS NULL OR g.email_normalized IS NULL)
            """)
            guesty_reservation_ids = [row[0] for row in cur.fetchall()]
        log.info(f"Need to enrich {len(guesty_reservation_ids)} reservations")
        if limit:
            guesty_reservation_ids = guesty_reservation_ids[:limit]
            log.info(f"  Limited to {len(guesty_reservation_ids)} for this run")

        enriched = skipped = errors = 0
        # We have the guestId from each reservation — fetch /reservations/{id} to get
        # the full guest sub-object (or fetch /guests/{guestId})
        for i, gid in enumerate(guesty_reservation_ids):
            try:
                # Use /reservations/{id} which returns the full reservation with embedded guest
                data = client.get(f"/reservations/{gid}", params={
                    "fields": "_id guestId guest checkIn"
                })
                guest = data.get("guest") or {}
                guest_id = data.get("guestId")
                if not guest:
                    skipped += 1
                    continue

                email = guest.get("email") or guest.get("emailAddress")
                phone = guest.get("phone")
                name = guest.get("fullName") or " ".join(
                    x for x in [guest.get("firstName"), guest.get("lastName")] if x
                )
                addr = guest.get("address") or {}
                country = normalize_country(addr.get("country"))
                city = addr.get("city")

                if not (country or city or email):
                    skipped += 1
                    continue

                with conn.cursor() as cur:
                    if email and "@" in email:
                        cur.execute(
                            """
                            INSERT INTO guests (email_normalized, name, phone, country_code, city)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (email_normalized) WHERE email_normalized IS NOT NULL
                            DO UPDATE SET
                                name = COALESCE(guests.name, EXCLUDED.name),
                                phone = COALESCE(guests.phone, EXCLUDED.phone),
                                country_code = COALESCE(EXCLUDED.country_code, guests.country_code),
                                city = COALESCE(EXCLUDED.city, guests.city)
                            RETURNING id
                            """,
                            (email, name, phone, country, city),
                        )
                        guest_uuid = str(cur.fetchone()[0])
                        cur.execute(
                            "UPDATE reservations SET guest_id = %s WHERE guesty_source_id = %s",
                            (guest_uuid, gid),
                        )
                    else:
                        # No email — link reservation to a name-keyed guest
                        cur.execute(
                            "SELECT guest_id FROM reservations WHERE guesty_source_id = %s LIMIT 1",
                            (gid,),
                        )
                        existing_guest = cur.fetchone()
                        if existing_guest and existing_guest[0]:
                            # Update existing guest with country/city if missing
                            cur.execute(
                                """
                                UPDATE guests SET
                                    country_code = COALESCE(country_code, %s),
                                    city = COALESCE(city, %s),
                                    phone = COALESCE(phone, %s),
                                    name = COALESCE(name, %s)
                                WHERE id = %s
                                """,
                                (country, city, phone, name, existing_guest[0]),
                            )
                        elif name:
                            # Create a name-only guest
                            cur.execute(
                                """
                                INSERT INTO guests (name, phone, country_code, city)
                                VALUES (%s, %s, %s, %s) RETURNING id
                                """,
                                (name, phone, country, city),
                            )
                            guest_uuid = str(cur.fetchone()[0])
                            cur.execute(
                                "UPDATE reservations SET guest_id = %s WHERE guesty_source_id = %s",
                                (guest_uuid, gid),
                            )
                enriched += 1

            except Exception as e:
                errors += 1
                conn.rollback()
                if errors < 5:
                    log.warning(f"  Error on {gid}: {e}")

            if (i + 1) % 50 == 0:
                conn.commit()
                log.info(f"  Processed {i+1}/{len(guesty_reservation_ids)}…")

        conn.commit()
        log.info(f"  → enriched={enriched} skipped={skipped} errors={errors}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
