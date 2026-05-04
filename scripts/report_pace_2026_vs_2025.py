"""Booking pace 2026 vs 2025 — focused report.

Compares "what's on the books today (2026-05-04) for each future month"
against "what was on the books on 2025-05-04 for the same month of 2025".
This is the only fair YoY comparison for a forward-looking window.

Sections:
  1. Realised YTD (Jan 1 → today)
  2. Pace by month (OTB now vs OTB-STLY = same-day-last-year)
  3. Pace by tier × month
  4. Pace by channel × month
  5. Year-end forecast: realised + on-the-books extrapolation
  6. PriceLabs forward pace cross-check (booking_status STLY from API)
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
    print("=" * 90)
    print(title)
    print("=" * 90)


def fmt_pct(v):
    return f"{v:+.1f}%" if v is not None else "—"


def realised_ytd(cur, today):
    section(f"1. REALISED YTD — Jan 1 → {today.isoformat()}  (CONFIRMED, by checkin_date)")
    # use day-of-year for symmetric comparison
    cur.execute("""
        SELECT
          EXTRACT(YEAR FROM checkin_date)::int AS y,
          COUNT(*) AS r,
          SUM(nights) AS n,
          SUM(gross_total) AS g,
          SUM(pa_revenue_gross) AS pa,
          ROUND(AVG(gross_total/NULLIF(nights,0)),0) AS adr
        FROM v_reservation_current
        WHERE status='CONFIRMED'
          AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
          AND EXTRACT(DOY FROM checkin_date) <= EXTRACT(DOY FROM %s::date)
        GROUP BY 1 ORDER BY 1
    """, (today,))
    rows = cur.fetchall()
    print(f"\n{'Year':<6}{'Resv':>7}{'Nights':>8}{'Gross':>13}{'PA Rev':>12}{'ADR':>7}")
    for y, r, n, g, pa, adr in rows:
        print(f"{y:<6}{r:>7}{n:>8}€{int(g or 0):>11,}€{int(pa or 0):>10,}€{int(adr or 0):>5}")
    if len(rows) == 2:
        (y25, r25, n25, g25, pa25, a25), (y26, r26, n26, g26, pa26, a26) = rows
        print(f"\n{'Δ%':<6}{fmt_pct((r26-r25)/r25*100):>7}"
              f"{fmt_pct((n26-n25)/n25*100):>8}"
              f"{fmt_pct((float(g26)-float(g25))/float(g25)*100):>13}"
              f"{fmt_pct((float(pa26)-float(pa25))/float(pa25)*100):>12}"
              f"{fmt_pct((float(a26)-float(a25))/float(a25)*100):>7}")


def pace_by_month(cur, today):
    section(f"2. PACE BY CHECKIN-MONTH — OTB (booked ≤ {today.isoformat()}) vs OTB-STLY")
    print("\nFor each checkin-month, count reservations booked before this DOY:")
    print(f"  2026 column = booked on/before {today.isoformat()}, checkin in 2026-MM")
    print(f"  2025 column = booked on/before 2025-{today.month:02d}-{today.day:02d}, checkin in 2025-MM")
    print(f"  Δr% / ΔG% = pace gap")
    print()

    cur.execute("""
        WITH otb AS (
          SELECT
            EXTRACT(YEAR FROM checkin_date)::int AS y,
            EXTRACT(MONTH FROM checkin_date)::int AS m,
            COUNT(*) AS r,
            SUM(gross_total) AS g,
            ROUND(AVG(gross_total/NULLIF(nights,0)),0) AS adr
          FROM v_reservation_current
          WHERE status='CONFIRMED'
            AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
            AND booked_at IS NOT NULL
            AND booked_at::date <= make_date(EXTRACT(YEAR FROM checkin_date)::int,
                                              EXTRACT(MONTH FROM %s::date)::int,
                                              EXTRACT(DAY FROM %s::date)::int)
          GROUP BY 1, 2
        )
        SELECT m,
          MAX(r) FILTER (WHERE y=2025) AS r25, MAX(g) FILTER (WHERE y=2025) AS g25, MAX(adr) FILTER (WHERE y=2025) AS a25,
          MAX(r) FILTER (WHERE y=2026) AS r26, MAX(g) FILTER (WHERE y=2026) AS g26, MAX(adr) FILTER (WHERE y=2026) AS a26
        FROM otb GROUP BY 1 ORDER BY 1
    """, (today, today))
    months = ["", "Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    print(f"{'Month':<6}{'r25':>5}{'g25':>10}{'a25':>6}{'r26':>5}{'g26':>10}{'a26':>6}{'Δr%':>7}{'ΔG%':>7}{'ΔADR%':>7}")
    rows = cur.fetchall()
    tot_r25 = tot_r26 = 0
    tot_g25 = tot_g26 = 0.0
    for m, r25, g25, a25, r26, g26, a26 in rows:
        r25 = r25 or 0; r26 = r26 or 0
        g25 = float(g25 or 0); g26 = float(g26 or 0)
        dr = (r26-r25)/r25*100 if r25 else None
        dg = (g26-g25)/g25*100 if g25 else None
        da = (float(a26 or 0)-float(a25 or 0))/float(a25 or 1)*100 if a25 else None
        tot_r25 += r25; tot_r26 += r26; tot_g25 += g25; tot_g26 += g26
        print(f"{months[m]:<6}{r25:>5}€{int(g25):>8,}€{int(a25 or 0):>4}"
              f"{r26:>5}€{int(g26):>8,}€{int(a26 or 0):>4}"
              f"{fmt_pct(dr):>7}{fmt_pct(dg):>7}{fmt_pct(da):>7}")
    print("-" * 90)
    dr_t = (tot_r26-tot_r25)/tot_r25*100 if tot_r25 else None
    dg_t = (tot_g26-tot_g25)/tot_g25*100 if tot_g25 else None
    print(f"{'TOTAL':<6}{tot_r25:>5}€{int(tot_g25):>8,}{'':>6}{tot_r26:>5}€{int(tot_g26):>8,}{'':>6}{fmt_pct(dr_t):>7}{fmt_pct(dg_t):>7}")


def pace_by_tier(cur, today):
    section("3. PACE BY TIER — same OTB methodology")
    cur.execute("""
        WITH otb AS (
          SELECT
            EXTRACT(YEAR FROM checkin_date)::int AS y,
            current_tier,
            COUNT(*) AS r, SUM(gross_total) AS g
          FROM v_reservation_current
          WHERE status='CONFIRMED'
            AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
            AND booked_at IS NOT NULL
            AND booked_at::date <= make_date(EXTRACT(YEAR FROM checkin_date)::int,
                                              EXTRACT(MONTH FROM %s::date)::int,
                                              EXTRACT(DAY FROM %s::date)::int)
          GROUP BY 1, 2
        )
        SELECT current_tier,
          MAX(r) FILTER (WHERE y=2025) AS r25, MAX(g) FILTER (WHERE y=2025) AS g25,
          MAX(r) FILTER (WHERE y=2026) AS r26, MAX(g) FILTER (WHERE y=2026) AS g26
        FROM otb GROUP BY 1 ORDER BY 1
    """, (today, today))
    print(f"\n{'Tier':<14}{'r25':>5}{'Gross25':>11}{'r26':>5}{'Gross26':>11}{'Δr%':>7}{'ΔG%':>7}")
    for tier, r25, g25, r26, g26 in cur.fetchall():
        r25 = r25 or 0; r26 = r26 or 0
        g25 = float(g25 or 0); g26 = float(g26 or 0)
        dr = (r26-r25)/r25*100 if r25 else None
        dg = (g26-g25)/g25*100 if g25 else None
        print(f"{(tier or 'NULL'):<14}{r25:>5}€{int(g25):>9,}{r26:>5}€{int(g26):>9,}{fmt_pct(dr):>7}{fmt_pct(dg):>7}")


def pace_by_channel(cur, today):
    section("4. PACE BY CHANNEL")
    cur.execute("""
        WITH otb AS (
          SELECT
            EXTRACT(YEAR FROM checkin_date)::int AS y,
            channel,
            COUNT(*) AS r, SUM(gross_total) AS g
          FROM v_reservation_current
          WHERE status='CONFIRMED' AND channel IS NOT NULL
            AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
            AND booked_at IS NOT NULL
            AND booked_at::date <= make_date(EXTRACT(YEAR FROM checkin_date)::int,
                                              EXTRACT(MONTH FROM %s::date)::int,
                                              EXTRACT(DAY FROM %s::date)::int)
          GROUP BY 1, 2
        )
        SELECT channel,
          MAX(r) FILTER (WHERE y=2025) AS r25, MAX(g) FILTER (WHERE y=2025) AS g25,
          MAX(r) FILTER (WHERE y=2026) AS r26, MAX(g) FILTER (WHERE y=2026) AS g26
        FROM otb GROUP BY 1 HAVING MAX(r) FILTER (WHERE y=2026) > 0 OR MAX(r) FILTER (WHERE y=2025) > 0
        ORDER BY GREATEST(MAX(r) FILTER (WHERE y=2025), MAX(r) FILTER (WHERE y=2026)) DESC
    """, (today, today))
    print(f"\n{'Channel':<12}{'r25':>5}{'Gross25':>11}{'r26':>5}{'Gross26':>11}{'Δr%':>7}{'ΔG%':>7}")
    for ch, r25, g25, r26, g26 in cur.fetchall():
        r25 = r25 or 0; r26 = r26 or 0
        g25 = float(g25 or 0); g26 = float(g26 or 0)
        dr = (r26-r25)/r25*100 if r25 else None
        dg = (g26-g25)/g25*100 if g25 else None
        print(f"{ch:<12}{r25:>5}€{int(g25):>9,}{r26:>5}€{int(g26):>9,}{fmt_pct(dr):>7}{fmt_pct(dg):>7}")


def yearend_forecast(cur, today):
    section(f"5. YEAR-END FORECAST — realised YTD + OTB future + STLY pace projection")
    # 2025 actuals (full year — snapshot of what happened)
    cur.execute("""
        SELECT COUNT(*), SUM(gross_total)
        FROM v_reservation_current
        WHERE status='CONFIRMED' AND EXTRACT(YEAR FROM checkin_date)=2025
    """)
    r25_full, g25_full = cur.fetchone()

    # 2026 realised so far (checkin already happened)
    cur.execute("""
        SELECT COUNT(*), SUM(gross_total)
        FROM v_reservation_current
        WHERE status='CONFIRMED' AND EXTRACT(YEAR FROM checkin_date)=2026
          AND checkin_date <= %s
    """, (today,))
    r26_real, g26_real = cur.fetchone()

    # 2026 OTB future (checkin > today, already booked)
    cur.execute("""
        SELECT COUNT(*), SUM(gross_total)
        FROM v_reservation_current
        WHERE status='CONFIRMED' AND EXTRACT(YEAR FROM checkin_date)=2026
          AND checkin_date > %s
    """, (today,))
    r26_otb, g26_otb = cur.fetchone()

    # 2025 same-day OTB (booked on or before 2025-05-04, checkin > that date in 2025)
    cur.execute("""
        SELECT COUNT(*), SUM(gross_total)
        FROM v_reservation_current
        WHERE status='CONFIRMED' AND EXTRACT(YEAR FROM checkin_date)=2025
          AND checkin_date > make_date(2025, EXTRACT(MONTH FROM %s::date)::int, EXTRACT(DAY FROM %s::date)::int)
          AND booked_at::date <= make_date(2025, EXTRACT(MONTH FROM %s::date)::int, EXTRACT(DAY FROM %s::date)::int)
    """, (today, today, today, today))
    r25_otb_stly, g25_otb_stly = cur.fetchone()

    # 2025 late bookings (booked after 4-May-2025 AND checkin > 4-May-2025) — what we still need to capture
    cur.execute("""
        SELECT COUNT(*), SUM(gross_total)
        FROM v_reservation_current
        WHERE status='CONFIRMED' AND EXTRACT(YEAR FROM checkin_date)=2025
          AND checkin_date > make_date(2025, EXTRACT(MONTH FROM %s::date)::int, EXTRACT(DAY FROM %s::date)::int)
          AND booked_at::date > make_date(2025, EXTRACT(MONTH FROM %s::date)::int, EXTRACT(DAY FROM %s::date)::int)
    """, (today, today, today, today))
    r25_late, g25_late = cur.fetchone()

    print(f"\n2025 actuals (full year):                {r25_full or 0:>5} reservas  €{int(g25_full or 0):>9,}")
    print(f"2026 realised (Jan→{today.isoformat()}):           {r26_real or 0:>5} reservas  €{int(g26_real or 0):>9,}")
    print(f"2026 OTB future (booked, not arrived):   {r26_otb or 0:>5} reservas  €{int(g26_otb or 0):>9,}")
    print(f"  (2025 STLY same-day OTB future):       {r25_otb_stly or 0:>5} reservas  €{int(g25_otb_stly or 0):>9,}  ← what we had at the same date in 2025")
    print(f"  (2025 late bookings after that date):  {r25_late or 0:>5} reservas  €{int(g25_late or 0):>9,}  ← what came in after 4-May-2025")
    print()
    # Forecast: realised 2026 YTD + (OTB now * (full_2025_total / OTB_2025_STLY))
    # OR realised 2026 YTD + OTB now + (estimate of late bookings yet to come)
    # Use ratio of late_to_otb_2025 as multiplier
    if r25_otb_stly and r25_late:
        late_ratio_r = (r25_late / r25_otb_stly)
        late_ratio_g = (float(g25_late) / float(g25_otb_stly))
        proj_late_r = int((r26_otb or 0) * late_ratio_r)
        proj_late_g = float(g26_otb or 0) * late_ratio_g
        forecast_r = (r26_real or 0) + (r26_otb or 0) + proj_late_r
        forecast_g = float(g26_real or 0) + float(g26_otb or 0) + proj_late_g
        print(f"Late-booking ratio observed in 2025: {late_ratio_r:.2f}x reservas, {late_ratio_g:.2f}x gross")
        print(f"  → Implied 2026 late bookings:        {proj_late_r:>5} reservas  €{int(proj_late_g):>9,}")
        print()
        print(f"  YEAR-END FORECAST 2026:                {forecast_r:>5} reservas  €{int(forecast_g):>9,}")
        if r25_full and g25_full:
            print(f"  vs 2025 actuals:                        {r25_full:>5} reservas  €{int(g25_full):>9,}")
            dr = (forecast_r - r25_full)/r25_full*100
            dg = (forecast_g - float(g25_full))/float(g25_full)*100
            print(f"  YE Δ:                                   {fmt_pct(dr):>5}            {fmt_pct(dg):>10}")


def pricelabs_pace(cur):
    section("6. PRICELABS FORWARD PACE — booking_status STLY (independent cross-check)")
    cur.execute("""
        SELECT
          DATE_TRUNC('month', target_date)::date AS m,
          COUNT(*) FILTER (WHERE booking_status='Booked') AS now_bk,
          COUNT(*) FILTER (WHERE booking_status_stly='Booked') AS stly_bk,
          ROUND(AVG(price) FILTER (WHERE price > 0),0) AS avg_p
        FROM pricelabs_daily_prices
        WHERE target_date >= CURRENT_DATE
          AND target_date < (CURRENT_DATE + INTERVAL '8 months')
        GROUP BY 1 ORDER BY 1
    """)
    print(f"\nFrom PriceLabs API (64 push-enabled listings, day-nights basis):")
    print(f"{'Month':<10}{'NowBkd':>9}{'STLYBkd':>10}{'Δ days':>9}{'Avg€':>7}{'Loss€':>11}")
    total_loss = 0
    for m, nb, sb, ap in cur.fetchall():
        gap = sb - nb
        loss = gap * (ap or 0)
        total_loss += loss
        d = (nb-sb)/sb*100 if sb else None
        print(f"{m.strftime('%Y-%m'):<10}{nb:>9}{sb:>10}{gap:>9}{int(ap or 0):>7}€{int(loss):>9,}")
    print("-" * 60)
    print(f"{'TOTAL':<28}{'':>10}{'':>9}{'':>7}€{int(total_loss):>9,}")


def main():
    log = setup_logging("report_pace_2026_vs_2025")
    today = date.today()
    conn = connect()
    try:
        with conn.cursor() as cur:
            realised_ytd(cur, today)
            pace_by_month(cur, today)
            pace_by_tier(cur, today)
            pace_by_channel(cur, today)
            yearend_forecast(cur, today)
            pricelabs_pace(cur)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
