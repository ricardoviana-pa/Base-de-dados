"""Import Guesty reservations from a cached JSON dump (the 1060-row pull).

This is a lighter alternative to sync_guesty.py — uses a local JSON file
instead of hitting the API. Useful when:
  • Rate-limited
  • For a one-shot full import to bootstrap the DB
  • For testing without burning API quota

Source: ~/Downloads/guesty/guesty_data/reservations.json (1,060 reservations)

Status caveat: the dump only has CONFIRMED reservations (cancellations
were filtered out at export time). For cancellations, you need the
live API call with all-statuses filter (in sync_guesty.py).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.db import connect, get_entity_id
from common.logging_utils import setup_logging

# Reuse mapper logic from sync_guesty
from sync_guesty import (
    STATUS_MAP, CHANNEL_MAP, map_status, map_channel,
    process_reservation, to_dt, to_decimal,
)

DUMP_PATH = Path.home() / "Downloads" / "guesty" / "guesty_data" / "reservations.json"


def main() -> int:
    log = setup_logging("import_guesty_json_dump")
    if not DUMP_PATH.exists():
        log.error(f"Dump not found: {DUMP_PATH}")
        return 1

    with DUMP_PATH.open() as f:
        reservations = json.load(f)
    log.info(f"Loaded {len(reservations)} reservations from {DUMP_PATH}")

    conn = connect()
    try:
        entity_id = get_entity_id(conn, "RTV")
        with conn.cursor() as cur:
            cur.execute("SELECT guesty_listing_id, property_id FROM guesty_listing_map WHERE property_id IS NOT NULL")
            listing_map = dict(cur.fetchall())
        log.info(f"Listing map: {len(listing_map)} listings → properties")

        inserted = matched = updated = cancellation = unmapped = errors = 0
        for i, r in enumerate(reservations):
            try:
                ok = process_reservation(r, conn, listing_map, entity_id, log)
                if ok == "matched":             matched += 1
                elif ok == "inserted":           inserted += 1
                elif ok == "updated":            updated += 1
                elif ok == "cancellation":       cancellation += 1
                elif ok == "skipped_unmapped":   unmapped += 1
            except Exception as e:
                errors += 1
                conn.rollback()
                if errors <= 3:
                    log.warning(f"  Error on {r.get('_id')}: {e}")
            if (i+1) % 100 == 0:
                conn.commit()
                log.info(f"  Processed {i+1}/{len(reservations)}…")
        conn.commit()

        log.info(f"  → fetched={len(reservations)} new={inserted} matched={matched} "
                 f"updated={updated} cancellations={cancellation} unmapped={unmapped} errors={errors}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
