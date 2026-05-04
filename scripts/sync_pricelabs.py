"""PriceLabs Open API sync (Sprint 5).

Pulls:
  GET  /v1/listings              → pricelabs_listing_map (header per listing)
  POST /v1/listing_prices        → pricelabs_daily_prices (forward 365d curve)

PriceLabs API key in env: PRICELABS_API_KEY
Endpoint base:           https://api.pricelabs.co/v1

Listings are matched to our `properties.id` by fuzzy name (PropertyResolver).
Listings without a match still land in the map (property_id NULL) so we can
review and assign manually.

Forward curve only fetches push_enabled listings (others return
LISTING_TOGGLE_OFF). Each call covers 365 days in one shot.

Usage:
  .venv/bin/python scripts/sync_pricelabs.py [listings|prices|all]
"""
from __future__ import annotations

import html
import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import requests
from dotenv import load_dotenv

from common.db import connect
from common.logging_utils import setup_logging
from common.property_match import PropertyResolver, _norm

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

API_BASE = "https://api.pricelabs.co/v1"


class PriceLabsClient:
    def __init__(self, log):
        self.log = log
        self.key = os.environ["PRICELABS_API_KEY"]
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": self.key})

    def get(self, path: str, params: dict | None = None) -> dict:
        url = f"{API_BASE}{path}"
        for attempt in range(4):
            r = self.session.get(url, params=params or {}, timeout=30)
            if r.status_code == 429:
                wait = 5 * (2 ** attempt)
                self.log.warning(f"  429 — waiting {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            time.sleep(0.1)
            return r.json()
        raise RuntimeError(f"GET {path} retried out")

    def post(self, path: str, body: dict) -> list | dict:
        url = f"{API_BASE}{path}"
        for attempt in range(4):
            r = self.session.post(url, json=body, timeout=60,
                                   headers={"Content-Type": "application/json"})
            if r.status_code == 429:
                wait = 5 * (2 ** attempt)
                self.log.warning(f"  429 — waiting {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            time.sleep(0.15)
            return r.json()
        raise RuntimeError(f"POST {path} retried out")


def sync_listings(client: PriceLabsClient, conn, log):
    """Pull all listings, fuzzy-match to property_id, upsert into pricelabs_listing_map."""
    data = client.get("/listings")
    listings = data.get("listings", [])
    log.info(f"Pulled {len(listings)} listings from PriceLabs")

    resolver = build_resolver(conn)

    matched = unmatched = 0
    with conn.cursor() as cur:
        for l in listings:
            raw_name = l.get("name") or ""
            name = html.unescape(raw_name)  # decode &amp; &#39; etc
            pid = resolver.resolve(name)
            if not pid and " -- " in name:
                # PriceLabs uses "T2-Code -- Long Name" pattern; try just the long part
                pid = resolver.resolve(name.split(" -- ", 1)[1])
            if not pid and "-" in name.split(" ", 1)[0]:
                # try just the suffix after the first hyphen-token
                pid = resolver.resolve(name.split("-", 1)[1])
            if pid:
                matched += 1
            else:
                unmatched += 1

            last_pushed = l.get("last_date_pushed")
            cur.execute(
                """
                INSERT INTO pricelabs_listing_map (
                    pricelabs_id, property_id, listing_name, pms, group_name,
                    city_name, bedrooms, base_price, min_price, max_price,
                    push_enabled, is_hidden, last_pushed_at, raw_response
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                ON CONFLICT (pricelabs_id) DO UPDATE SET
                    property_id   = COALESCE(EXCLUDED.property_id, pricelabs_listing_map.property_id),
                    listing_name  = EXCLUDED.listing_name,
                    pms           = EXCLUDED.pms,
                    group_name    = EXCLUDED.group_name,
                    city_name     = EXCLUDED.city_name,
                    bedrooms      = EXCLUDED.bedrooms,
                    base_price    = EXCLUDED.base_price,
                    min_price     = EXCLUDED.min_price,
                    max_price     = EXCLUDED.max_price,
                    push_enabled  = EXCLUDED.push_enabled,
                    is_hidden     = EXCLUDED.is_hidden,
                    last_pushed_at= EXCLUDED.last_pushed_at,
                    raw_response  = EXCLUDED.raw_response,
                    imported_at   = NOW()
                """,
                (
                    l["id"], pid, name, l.get("pms"), l.get("group"),
                    l.get("city_name"), l.get("no_of_bedrooms"),
                    l.get("base"), l.get("min"), l.get("max"),
                    bool(l.get("push_enabled")), bool(l.get("isHidden")),
                    last_pushed, json.dumps(l, default=str),
                ),
            )
    conn.commit()
    log.info(f"  → matched={matched} unmatched={unmatched}")
    return matched, unmatched


def build_resolver(conn) -> PropertyResolver:
    """Build a name resolver including canonical, display, building, listing names."""
    r = PropertyResolver()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, canonical_name, display_name, building,
                   doc_unico_id, rental_ready_id, guesty_id
            FROM properties
        """)
        for row in cur.fetchall():
            pid = str(row[0])
            for variant in row[1:]:
                if variant:
                    r._add(str(variant), pid)
        # Also add Guesty listing-map names since PriceLabs `name` is often the
        # Guesty/RR listing display name, not our canonical_name.
        cur.execute("""
            SELECT property_id, guesty_title FROM guesty_listing_map
            WHERE property_id IS NOT NULL AND guesty_title IS NOT NULL
        """)
        for pid, lname in cur.fetchall():
            r._add(str(lname), str(pid))
    return r


def sync_daily_prices(client: PriceLabsClient, conn, log):
    """For each push_enabled listing, pull forward 365d prices and upsert."""
    today = date.today()
    end = today + timedelta(days=365)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT pricelabs_id, property_id, listing_name, pms
            FROM pricelabs_listing_map
            WHERE push_enabled = TRUE
            ORDER BY listing_name
        """)
        listings = cur.fetchall()
    log.info(f"Pulling forward 365d for {len(listings)} push-enabled listings")

    snapshot = today
    rows_total = errors = listings_done = 0

    for plid, pid, name, pms in listings:
        try:
            payload = {
                "listings": [{
                    "id": plid,
                    "pms": pms,
                    "dateFrom": today.isoformat(),
                    "dateTo": end.isoformat(),
                }]
            }
            resp = client.post("/listing_prices", payload)
            if not isinstance(resp, list) or not resp:
                log.warning(f"  {name}: empty response")
                continue
            item = resp[0]
            if item.get("error"):
                log.warning(f"  {name}: {item.get('error_status')} — {item.get('error')}")
                continue
            days = item.get("data") or []
            with conn.cursor() as cur:
                for d in days:
                    user_p = d.get("user_price")
                    if user_p == -1:
                        user_p = None
                    def _date(v):
                        if v in (None, "", "-1", -1):
                            return None
                        return v
                    bd = _date(d.get("booked_date"))
                    bd_stly = _date(d.get("booked_date_STLY"))
                    cur.execute(
                        """
                        INSERT INTO pricelabs_daily_prices (
                            pricelabs_id, property_id, target_date, snapshot_date,
                            price, user_price, uncustomized_price, min_stay,
                            booking_status, booking_status_stly, adr, adr_stly,
                            booked_date, booked_date_stly, demand_color, demand_desc, raw
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                        ON CONFLICT (pricelabs_id, target_date, snapshot_date) DO UPDATE SET
                            price = EXCLUDED.price,
                            user_price = EXCLUDED.user_price,
                            uncustomized_price = EXCLUDED.uncustomized_price,
                            min_stay = EXCLUDED.min_stay,
                            booking_status = EXCLUDED.booking_status,
                            booking_status_stly = EXCLUDED.booking_status_stly,
                            adr = EXCLUDED.adr,
                            adr_stly = EXCLUDED.adr_stly,
                            booked_date = EXCLUDED.booked_date,
                            booked_date_stly = EXCLUDED.booked_date_stly,
                            demand_color = EXCLUDED.demand_color,
                            demand_desc = EXCLUDED.demand_desc,
                            raw = EXCLUDED.raw
                        """,
                        (
                            plid, pid, d["date"], snapshot,
                            d.get("price"), user_p, d.get("uncustomized_price"),
                            d.get("min_stay"),
                            d.get("booking_status"), d.get("booking_status_STLY"),
                            d.get("ADR"), d.get("ADR_STLY"),
                            bd, bd_stly,
                            d.get("demand_color"), d.get("demand_desc"),
                            json.dumps(d, default=str),
                        ),
                    )
            rows_total += len(days)
            listings_done += 1
            if listings_done % 10 == 0:
                conn.commit()
                log.info(f"  {listings_done}/{len(listings)} listings done, {rows_total} day-rows")
        except Exception as e:
            errors += 1
            conn.rollback()
            log.warning(f"  {name}: {e}")

    conn.commit()
    log.info(f"  → listings={listings_done} day_rows={rows_total} errors={errors}")
    return listings_done, rows_total, errors


def main():
    log = setup_logging("sync_pricelabs")
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    conn = connect()
    client = PriceLabsClient(log)

    try:
        if mode in ("listings", "all"):
            sync_listings(client, conn, log)
        if mode in ("prices", "all"):
            sync_daily_prices(client, conn, log)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
