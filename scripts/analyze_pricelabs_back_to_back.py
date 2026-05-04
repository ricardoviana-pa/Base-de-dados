"""PriceLabs back-to-back analysis (Sprint 5 Phase 2).

Compares realised pricing (from reservations) against:
  - PriceLabs current base price (from pricelabs_listing_map)
  - PriceLabs daily price recommendations (from pricelabs_daily_prices)
  - Same-time-last-year (STLY) ADR + booking_status from PriceLabs
  - Comp-set / market ADR (PriceLabs ADR field per day)

Produces a structured report (printed + Excel export) covering:
  1. April 2026 vs April 2025 deep dive (the symptomatic month)
  2. Property-level variance: realised ADR vs current base price
  3. ADR vs market ADR (comp set proxy) per property × week
  4. Booking pace: STLY booked vs current booked
  5. Top under-priced + over-priced properties
  6. Demand-vs-price coherence: high-demand days at low price = leaving money

Usage: .venv/bin/python scripts/analyze_pricelabs_back_to_back.py
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.db import connect
from common.logging_utils import setup_logging


def section(title: str):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def fmt_money(v):
    if v is None:
        return "—"
    return f"€{v:,.0f}"


def fmt_pct(v):
    if v is None:
        return "—"
    return f"{v:+.1f}%"


def april_yoy(cur):
    section("1. APRIL 2026 vs APRIL 2025 — the symptomatic month")
    cur.execute("""
        SELECT
          v.current_tier,
          COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM v.checkin_date)=2025
                              AND v.status='CONFIRMED'
                              AND EXTRACT(MONTH FROM v.checkin_date)=4) AS r25,
          ROUND(AVG(v.gross_total/NULLIF(v.nights,0)) FILTER (WHERE EXTRACT(YEAR FROM v.checkin_date)=2025
                              AND v.status='CONFIRMED'
                              AND EXTRACT(MONTH FROM v.checkin_date)=4), 0) AS adr25,
          SUM(v.gross_total) FILTER (WHERE EXTRACT(YEAR FROM v.checkin_date)=2025
                              AND v.status='CONFIRMED'
                              AND EXTRACT(MONTH FROM v.checkin_date)=4) AS gross25,
          COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM v.checkin_date)=2026
                              AND v.status='CONFIRMED'
                              AND EXTRACT(MONTH FROM v.checkin_date)=4) AS r26,
          ROUND(AVG(v.gross_total/NULLIF(v.nights,0)) FILTER (WHERE EXTRACT(YEAR FROM v.checkin_date)=2026
                              AND v.status='CONFIRMED'
                              AND EXTRACT(MONTH FROM v.checkin_date)=4), 0) AS adr26,
          SUM(v.gross_total) FILTER (WHERE EXTRACT(YEAR FROM v.checkin_date)=2026
                              AND v.status='CONFIRMED'
                              AND EXTRACT(MONTH FROM v.checkin_date)=4) AS gross26
        FROM v_reservation_current v
        GROUP BY 1 ORDER BY 1
    """)
    print(f"\n{'Tier':<14}{'r25':>6}{'ADR25':>10}{'Gross25':>14}{'r26':>6}{'ADR26':>10}{'Gross26':>14}{'ΔADR%':>9}{'Δr%':>9}")
    for row in cur.fetchall():
        tier, r25, a25, g25, r26, a26, g26 = row
        d_adr = ((a26 - a25) / a25 * 100) if (a25 and a26) else None
        d_r = ((r26 - r25) / r25 * 100) if r25 else None
        print(f"{(tier or 'NULL'):<14}{r25 or 0:>6}{fmt_money(a25):>10}{fmt_money(g25):>14}"
              f"{r26 or 0:>6}{fmt_money(a26):>10}{fmt_money(g26):>14}"
              f"{fmt_pct(d_adr):>9}{fmt_pct(d_r):>9}")

    print("\nBy channel:")
    cur.execute("""
        SELECT
          v.channel,
          COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM v.checkin_date)=2025
                              AND v.status='CONFIRMED'
                              AND EXTRACT(MONTH FROM v.checkin_date)=4) AS r25,
          ROUND(AVG(v.gross_total/NULLIF(v.nights,0)) FILTER (WHERE EXTRACT(YEAR FROM v.checkin_date)=2025
                              AND v.status='CONFIRMED'
                              AND EXTRACT(MONTH FROM v.checkin_date)=4), 0) AS adr25,
          COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM v.checkin_date)=2026
                              AND v.status='CONFIRMED'
                              AND EXTRACT(MONTH FROM v.checkin_date)=4) AS r26,
          ROUND(AVG(v.gross_total/NULLIF(v.nights,0)) FILTER (WHERE EXTRACT(YEAR FROM v.checkin_date)=2026
                              AND v.status='CONFIRMED'
                              AND EXTRACT(MONTH FROM v.checkin_date)=4), 0) AS adr26
        FROM v_reservation_current v
        WHERE v.channel IS NOT NULL
        GROUP BY 1
        HAVING COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM v.checkin_date) IN (2025,2026)
                                  AND EXTRACT(MONTH FROM v.checkin_date)=4) > 0
        ORDER BY r26 DESC NULLS LAST
    """)
    print(f"\n{'Channel':<12}{'r25':>6}{'ADR25':>10}{'r26':>6}{'ADR26':>10}{'ΔADR%':>9}{'Δr%':>9}")
    for ch, r25, a25, r26, a26 in cur.fetchall():
        d_adr = ((a26 - a25) / a25 * 100) if (a25 and a26) else None
        d_r = ((r26 - (r25 or 0)) / r25 * 100) if r25 else None
        print(f"{ch:<12}{r25 or 0:>6}{fmt_money(a25):>10}{r26 or 0:>6}{fmt_money(a26):>10}"
              f"{fmt_pct(d_adr):>9}{fmt_pct(d_r):>9}")


def realised_vs_base(cur):
    section("2. PROPERTY VARIANCE — realised ADR vs PriceLabs base price (current)")
    cur.execute("""
        WITH realised_2025 AS (
            SELECT v.property_id,
                   ROUND(AVG(v.gross_total/NULLIF(v.nights,0)),0) AS adr_2025,
                   COUNT(*) AS n_2025
            FROM v_reservation_current v
            WHERE EXTRACT(YEAR FROM v.checkin_date)=2025 AND v.status='CONFIRMED'
            GROUP BY 1
        ),
        realised_2026 AS (
            SELECT v.property_id,
                   ROUND(AVG(v.gross_total/NULLIF(v.nights,0)),0) AS adr_2026,
                   COUNT(*) AS n_2026
            FROM v_reservation_current v
            WHERE EXTRACT(YEAR FROM v.checkin_date)=2026 AND v.status='CONFIRMED'
            GROUP BY 1
        )
        SELECT
          p.canonical_name,
          plm.base_price AS pl_base,
          plm.min_price AS pl_min,
          plm.max_price AS pl_max,
          r25.adr_2025, r25.n_2025,
          r26.adr_2026, r26.n_2026,
          ROUND((r25.adr_2025 - plm.base_price) / NULLIF(plm.base_price,0) * 100, 0) AS gap_2025_pct,
          ROUND((r26.adr_2026 - plm.base_price) / NULLIF(plm.base_price,0) * 100, 0) AS gap_2026_pct
        FROM pricelabs_listing_map plm
        JOIN properties p ON p.id = plm.property_id
        LEFT JOIN realised_2025 r25 ON r25.property_id = plm.property_id
        LEFT JOIN realised_2026 r26 ON r26.property_id = plm.property_id
        WHERE plm.push_enabled = TRUE AND plm.property_id IS NOT NULL
        ORDER BY gap_2026_pct ASC NULLS LAST
    """)
    rows = cur.fetchall()

    print(f"\n→ {len(rows)} push-enabled properties matched to DB\n")
    print(f"{'Property':<48}{'Base':>6}{'ADR25':>7}{'gap25':>7}{'ADR26':>7}{'gap26':>7}")

    for row in rows:
        name, base, _mn, _mx, a25, n25, a26, n26, g25, g26 = row
        if not (a25 or a26):
            continue
        print(f"{(name or '?')[:48]:<48}{int(base or 0):>6}{int(a25 or 0):>7}"
              f"{(str(int(g25))+'%' if g25 is not None else '—'):>7}"
              f"{int(a26 or 0):>7}"
              f"{(str(int(g26))+'%' if g26 is not None else '—'):>7}")


def under_over_priced(cur):
    section("3. UNDER-PRICED PROPERTIES — realised >> PriceLabs base = could push higher")
    cur.execute("""
        SELECT p.canonical_name, plm.base_price,
               ROUND(AVG(v.gross_total/NULLIF(v.nights,0)),0) AS adr,
               COUNT(*) AS resvs,
               ROUND((AVG(v.gross_total/NULLIF(v.nights,0)) - plm.base_price)
                     / NULLIF(plm.base_price,0) * 100, 0) AS gap_pct
        FROM v_reservation_current v
        JOIN pricelabs_listing_map plm ON plm.property_id = v.property_id
        JOIN properties p ON p.id = v.property_id
        WHERE EXTRACT(YEAR FROM v.checkin_date) IN (2025, 2026)
          AND v.status='CONFIRMED' AND plm.push_enabled = TRUE
        GROUP BY 1,2
        HAVING COUNT(*) >= 5
        ORDER BY gap_pct DESC NULLS LAST
        LIMIT 10
    """)
    print(f"\n{'Property':<48}{'PLBase':>8}{'ADR':>7}{'Resv':>5}{'Gap%':>7}")
    for name, base, adr, n, gap in cur.fetchall():
        print(f"{(name or '?')[:48]:<48}{int(base or 0):>8}{int(adr or 0):>7}{n:>5}"
              f"{(str(int(gap))+'%' if gap is not None else '—'):>7}")

    section("4. OVER-PRICED PROPERTIES — realised << PriceLabs base = base too high or overrides hurting")
    cur.execute("""
        SELECT p.canonical_name, plm.base_price,
               ROUND(AVG(v.gross_total/NULLIF(v.nights,0)),0) AS adr,
               COUNT(*) AS resvs,
               ROUND((AVG(v.gross_total/NULLIF(v.nights,0)) - plm.base_price)
                     / NULLIF(plm.base_price,0) * 100, 0) AS gap_pct
        FROM v_reservation_current v
        JOIN pricelabs_listing_map plm ON plm.property_id = v.property_id
        JOIN properties p ON p.id = v.property_id
        WHERE EXTRACT(YEAR FROM v.checkin_date) IN (2025, 2026)
          AND v.status='CONFIRMED' AND plm.push_enabled = TRUE
        GROUP BY 1,2
        HAVING COUNT(*) >= 5
        ORDER BY gap_pct ASC NULLS LAST
        LIMIT 10
    """)
    print(f"\n{'Property':<48}{'PLBase':>8}{'ADR':>7}{'Resv':>5}{'Gap%':>7}")
    for name, base, adr, n, gap in cur.fetchall():
        print(f"{(name or '?')[:48]:<48}{int(base or 0):>8}{int(adr or 0):>7}{n:>5}"
              f"{(str(int(gap))+'%' if gap is not None else '—'):>7}")


def forward_pace(cur):
    section("5. FORWARD BOOKING PACE — STLY vs current (PriceLabs daily data)")
    cur.execute("""
        SELECT
          DATE_TRUNC('month', target_date)::date AS month,
          COUNT(*) AS days,
          COUNT(*) FILTER (WHERE booking_status='Booked') AS booked_now,
          COUNT(*) FILTER (WHERE booking_status_stly='Booked') AS booked_stly,
          ROUND(AVG(price) FILTER (WHERE booking_status='Available'),0) AS avg_price_avail,
          ROUND(AVG(adr) FILTER (WHERE adr IS NOT NULL),0) AS avg_market_adr,
          ROUND(AVG(adr_stly) FILTER (WHERE adr_stly IS NOT NULL),0) AS avg_market_adr_stly
        FROM pricelabs_daily_prices
        WHERE target_date >= CURRENT_DATE
        GROUP BY 1 ORDER BY 1
        LIMIT 12
    """)
    print(f"\n{'Month':<10}{'Days':>6}{'Booked':>8}{'STLY-bk':>9}{'Δpace':>9}{'AvailP':>8}{'MktADR':>8}{'STLY-MA':>9}")
    for m, d, b, bs, ap, ma, mas in cur.fetchall():
        delta_pace = (b - bs) / bs * 100 if bs else None
        print(f"{m.strftime('%Y-%m'):<10}{d:>6}{b:>8}{bs:>9}"
              f"{(str(int(delta_pace))+'%' if delta_pace is not None else '—'):>9}"
              f"{int(ap or 0):>8}{int(ma or 0):>8}{int(mas or 0):>9}")


def demand_coherence(cur):
    section("6. DEMAND-vs-PRICE COHERENCE — high-demand days at low price (money on table)")
    cur.execute("""
        SELECT
          demand_desc,
          COUNT(*) AS days,
          COUNT(*) FILTER (WHERE booking_status='Available') AS available,
          ROUND(AVG(price) FILTER (WHERE booking_status='Available'),0) AS avg_price_avail,
          ROUND(AVG(uncustomized_price) FILTER (WHERE booking_status='Available'),0) AS avg_uncust,
          ROUND(AVG(adr) FILTER (WHERE adr IS NOT NULL),0) AS market_adr
        FROM pricelabs_daily_prices
        WHERE target_date >= CURRENT_DATE AND target_date < CURRENT_DATE + 90
        GROUP BY 1 ORDER BY market_adr DESC NULLS LAST
    """)
    print(f"\nNext 90 days (where price > 0 = ours, uncust = PriceLabs algo, market = comp set):")
    print(f"{'Demand':<14}{'Days':>6}{'Avail':>7}{'Ours':>7}{'PLAlgo':>8}{'Market':>8}")
    for desc, d, a, ours, alg, mkt in cur.fetchall():
        print(f"{(desc or 'NULL'):<14}{d:>6}{a:>7}{int(ours or 0):>7}{int(alg or 0):>8}{int(mkt or 0):>8}")

    section("7. MANUAL OVERRIDES — where user_price is set, are we leaving money?")
    cur.execute("""
        SELECT
          CASE WHEN user_price IS NULL THEN 'no override'
               WHEN user_price < uncustomized_price THEN 'manual DOWN'
               WHEN user_price > uncustomized_price THEN 'manual UP'
               ELSE 'equal'
          END AS override_kind,
          COUNT(*) AS days,
          ROUND(AVG(price - uncustomized_price),1) AS avg_delta,
          ROUND(AVG(uncustomized_price - price) FILTER (WHERE user_price < uncustomized_price),1)
            AS avg_money_left_when_down
        FROM pricelabs_daily_prices
        WHERE target_date >= CURRENT_DATE AND target_date < CURRENT_DATE + 180
          AND uncustomized_price IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """)
    print(f"\n{'Override':<15}{'Days':>6}{'AvgΔ€':>8}{'€left/day if DOWN':>20}")
    for k, d, dlt, lost in cur.fetchall():
        print(f"{k:<15}{d:>6}{(str(dlt) if dlt is not None else '—'):>8}"
              f"{(str(lost) if lost is not None else '—'):>20}")


def margin_per_property(cur):
    section("8. MARGIN PER PROPERTY (2024-2025 averaged) — owner share + direct costs")
    cur.execute("""
        SELECT p.canonical_name,
               COUNT(*) AS resvs,
               ROUND(SUM(rm.gross_total),0) AS gross,
               ROUND(SUM(rm.pa_revenue_gross),0) AS pa_rev,
               ROUND(SUM(rm.direct_cleaning_cost + rm.direct_laundry_cost),0) AS direct,
               ROUND(SUM(rm.margin_direct),0) AS margin,
               ROUND(AVG(rm.margin_direct_pct)*100,1) AS margin_pct
        FROM v_reservation_margin rm
        JOIN properties p ON p.id = rm.property_id
        WHERE EXTRACT(YEAR FROM rm.checkin_date) IN (2024, 2025)
        GROUP BY 1 HAVING COUNT(*) >= 8
        ORDER BY margin DESC LIMIT 10
    """)
    print(f"\nTop 10 by absolute margin:")
    print(f"{'Property':<48}{'Resv':>5}{'Gross':>8}{'PA Rev':>8}{'Direct':>8}{'Margin':>8}{'Mgn%':>7}")
    for n, r, g, pa, d, m, p in cur.fetchall():
        print(f"{(n or '?')[:48]:<48}{r:>5}{int(g or 0):>8}{int(pa or 0):>8}"
              f"{int(d or 0):>8}{int(m or 0):>8}"
              f"{(str(p)+'%' if p is not None else '—'):>7}")

    cur.execute("""
        SELECT p.canonical_name,
               COUNT(*) AS resvs,
               ROUND(SUM(rm.gross_total),0) AS gross,
               ROUND(SUM(rm.margin_direct),0) AS margin,
               ROUND(AVG(rm.margin_direct_pct)*100,1) AS margin_pct
        FROM v_reservation_margin rm
        JOIN properties p ON p.id = rm.property_id
        WHERE EXTRACT(YEAR FROM rm.checkin_date) IN (2024, 2025)
        GROUP BY 1 HAVING COUNT(*) >= 8
        ORDER BY margin_pct ASC NULLS LAST LIMIT 10
    """)
    print(f"\nWorst 10 by margin %:")
    print(f"{'Property':<48}{'Resv':>5}{'Gross':>8}{'Margin':>8}{'Mgn%':>7}")
    for n, r, g, m, p in cur.fetchall():
        print(f"{(n or '?')[:48]:<48}{r:>5}{int(g or 0):>8}{int(m or 0):>8}"
              f"{(str(p)+'%' if p is not None else '—'):>7}")


def main():
    log = setup_logging("analyze_pricelabs_back_to_back")
    conn = connect()
    try:
        with conn.cursor() as cur:
            april_yoy(cur)
            realised_vs_base(cur)
            under_over_priced(cur)
            forward_pace(cur)
            demand_coherence(cur)
            margin_per_property(cur)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
