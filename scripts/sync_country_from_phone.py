"""Phone → ISO country code fallback enrichment.

Many Guesty/OTA guests come without address country, but with a phone number.
The ITU country code prefix (e.g. +351 → PT, +44 → GB) gives us country with
high confidence — we use Google's libphonenumber via the `phonenumbers` package.

Updates guests.country_code only when currently NULL; existing values are kept
(even if they disagree with the phone — country_of_residence often differs).

Usage: .venv/bin/python scripts/sync_country_from_phone.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import phonenumbers
from phonenumbers import geocoder, region_code_for_number

from common.db import connect
from common.logging_utils import setup_logging


def parse_country(phone: str) -> str | None:
    if not phone:
        return None
    p = phone.strip()
    if not p or len(p) < 8:
        return None
    # Add + prefix if missing (libphonenumber needs it for international parsing)
    if not p.startswith("+"):
        p = "+" + p.lstrip("0")
    try:
        num = phonenumbers.parse(p, None)
    except phonenumbers.NumberParseException:
        return None
    if not phonenumbers.is_valid_number(num):
        return None
    cc = region_code_for_number(num)
    return cc if cc else None


def main() -> int:
    log = setup_logging("sync_country_from_phone")
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, phone FROM guests
                WHERE phone IS NOT NULL AND country_code IS NULL
                  AND phone ~ '^[+0-9]' AND length(phone) >= 8
            """)
            rows = cur.fetchall()
        log.info(f"Candidates: {len(rows)}")

        updated = invalid = 0
        sample_by_cc: dict[str, int] = {}
        with conn.cursor() as cur:
            for gid, phone in rows:
                cc = parse_country(phone)
                if not cc:
                    invalid += 1
                    continue
                cur.execute(
                    "UPDATE guests SET country_code = %s WHERE id = %s AND country_code IS NULL",
                    (cc, gid),
                )
                updated += 1
                sample_by_cc[cc] = sample_by_cc.get(cc, 0) + 1
        conn.commit()
        log.info(f"  → updated={updated} invalid={invalid}")
        log.info("  Top countries detected:")
        for cc, n in sorted(sample_by_cc.items(), key=lambda x: -x[1])[:15]:
            log.info(f"    {cc}: {n}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
