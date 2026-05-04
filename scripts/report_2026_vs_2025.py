"""Full 2026 vs 2025 YoY report.

Sections:
  1. Year-to-date (Jan 1 – today) headline: revenue, reservations, ADR, nights
  2. Monthly trajectory: month-by-month YoY for actual months
  3. By tier (STANDARD / PREMIUM / LUXURY)
  4. By channel (Airbnb / Booking / Direct / etc.)
  5. By region
  6. Lead time + booking window
  7. Cancellations
  8. Forward outlook (rest of 2026 vs full 2025)
  9. Margin per property top movers
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
    print("=" * 82)
    print(title)
    print("=" * 82)


def fmt_money(v):
    return f"€{int(v):,}" if v else "—"


def fmt_pct(v):
    return f"{v:+.1f}%" if v is not None else "—"


def ytd_headline(cur, today):
    section(f"1. YEAR-TO-DATE — Jan 1 → {today.isoformat()} (CONFIRMED)")
    cur.execute("""
        WITH yr AS (
          SELECT
            EXTRACT(YEAR FROM checkin_date)::int AS y,
            COUNT(*) AS reservations,
            SUM(nights) AS nights,
            SUM(gross_total) AS gross,
            SUM(pa_revenue_gross) AS pa_rev,
            ROUND(AVG(gross_total/NULLIF(nights,0)),0) AS adr,
            ROUND(AVG(lead_time_days),0) AS lead_time
          FROM v_reservation_current
          WHERE status='CONFIRMED'
            AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
            AND checkin_date <= %s
            AND checkin_date >= make_date(EXTRACT(YEAR FROM checkin_date)::int, 1, 1)
            AND (EXTRACT(MONTH FROM checkin_date) < EXTRACT(MONTH FROM %s::date)
                 OR (EXTRACT(MONTH FROM checkin_date) = EXTRACT(MONTH FROM %s::date)
                     AND EXTRACT(DAY FROM checkin_date) <= EXTRACT(DAY FROM %s::date)))
          GROUP BY 1
        )
        SELECT * FROM yr ORDER BY y
    """, (today, today, today, today))
    rows = cur.fetchall()
    print(f"\n{'Year':<6}{'Resv':>7}{'Nights':>8}{'Gross':>12}{'PA Rev':>12}{'ADR':>7}{'LeadT':>7}")
    for y, r, n, g, pa, adr, lt in rows:
        print(f"{y:<6}{r:>7}{n:>8}{int(g or 0):>12,}{int(pa or 0):>12,}{int(adr or 0):>7}{int(lt or 0):>7}")
    if len(rows) == 2:
        y25, r25, n25, g25, pa25, a25, lt25 = rows[0]
        y26, r26, n26, g26, pa26, a26, lt26 = rows[1]
        print()
        print(f"{'Δ':<6}"
              f"{fmt_pct((r26-r25)/r25*100):>7}"
              f"{fmt_pct((n26-n25)/n25*100):>8}"
              f"{fmt_pct((float(g26)-float(g25))/float(g25)*100):>12}"
              f"{fmt_pct((float(pa26)-float(pa25))/float(pa25)*100):>12}"
              f"{fmt_pct((float(a26)-float(a25))/float(a25)*100):>7}"
              f"{fmt_pct((float(lt26)-float(lt25))/float(lt25)*100):>7}")


def monthly_trajectory(cur):
    section("2. MONTHLY TRAJECTORY — every month YoY")
    cur.execute("""
        WITH m AS (
          SELECT
            EXTRACT(MONTH FROM checkin_date)::int AS mo,
            EXTRACT(YEAR FROM checkin_date)::int AS y,
            COUNT(*) AS reservations,
            SUM(gross_total) AS gross,
            ROUND(AVG(gross_total/NULLIF(nights,0)),0) AS adr
          FROM v_reservation_current
          WHERE status='CONFIRMED'
            AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
          GROUP BY 1, 2
        )
        SELECT
          mo,
          MAX(reservations) FILTER (WHERE y=2025) AS r25,
          MAX(reservations) FILTER (WHERE y=2026) AS r26,
          MAX(gross) FILTER (WHERE y=2025) AS g25,
          MAX(gross) FILTER (WHERE y=2026) AS g26,
          MAX(adr) FILTER (WHERE y=2025) AS a25,
          MAX(adr) FILTER (WHERE y=2026) AS a26
        FROM m GROUP BY 1 ORDER BY 1
    """)
    print(f"\n{'Month':<6}{'r25':>5}{'r26':>5}{'Δr%':>7}{'Gross25':>10}{'Gross26':>10}{'ΔG%':>7}{'ADR25':>7}{'ADR26':>7}{'ΔADR%':>8}")
    months = ["", "Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    for row in cur.fetchall():
        mo, r25, r26, g25, g26, a25, a26 = row
        dr = (r26-r25)/r25*100 if (r25 and r26) else None
        dg = (float(g26)-float(g25))/float(g25)*100 if (g25 and g26) else None
        da = (float(a26)-float(a25))/float(a25)*100 if (a25 and a26) else None
        print(f"{months[mo]:<6}{r25 or 0:>5}{r26 or 0:>5}{fmt_pct(dr):>7}"
              f"{int(g25 or 0):>10,}{int(g26 or 0):>10,}{fmt_pct(dg):>7}"
              f"{int(a25 or 0):>7}{int(a26 or 0):>7}{fmt_pct(da):>8}")


def by_tier(cur, today):
    section("3. BY TIER — YTD same-period")
    cur.execute("""
        SELECT
          current_tier,
          EXTRACT(YEAR FROM checkin_date)::int AS y,
          COUNT(*) AS r, SUM(gross_total) AS g,
          ROUND(AVG(gross_total/NULLIF(nights,0)),0) AS adr
        FROM v_reservation_current
        WHERE status='CONFIRMED'
          AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
          AND EXTRACT(DOY FROM checkin_date) <= EXTRACT(DOY FROM %s::date)
        GROUP BY 1, 2 ORDER BY 1, 2
    """, (today,))
    rows = cur.fetchall()
    by_tier = {}
    for tier, y, r, g, adr in rows:
        by_tier.setdefault(tier or "NULL", {})[y] = (r, g, adr)
    print(f"\n{'Tier':<14}{'r25':>5}{'r26':>5}{'Δr%':>7}{'Gross25':>10}{'Gross26':>10}{'ΔG%':>7}{'ADR25':>7}{'ADR26':>7}{'ΔADR%':>8}")
    for tier, d in sorted(by_tier.items()):
        r25, g25, a25 = d.get(2025, (0, 0, 0))
        r26, g26, a26 = d.get(2026, (0, 0, 0))
        dr = (r26-r25)/r25*100 if r25 else None
        dg = (float(g26)-float(g25))/float(g25)*100 if g25 else None
        da = (float(a26)-float(a25))/float(a25)*100 if a25 else None
        print(f"{tier:<14}{r25:>5}{r26:>5}{fmt_pct(dr):>7}"
              f"{int(g25):>10,}{int(g26):>10,}{fmt_pct(dg):>7}"
              f"{int(a25 or 0):>7}{int(a26 or 0):>7}{fmt_pct(da):>8}")


def by_channel(cur, today):
    section("4. BY CHANNEL — YTD same-period")
    cur.execute("""
        SELECT
          channel,
          EXTRACT(YEAR FROM checkin_date)::int AS y,
          COUNT(*) AS r, SUM(gross_total) AS g,
          ROUND(AVG(gross_total/NULLIF(nights,0)),0) AS adr
        FROM v_reservation_current
        WHERE status='CONFIRMED' AND channel IS NOT NULL
          AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
          AND EXTRACT(DOY FROM checkin_date) <= EXTRACT(DOY FROM %s::date)
        GROUP BY 1, 2 ORDER BY 1, 2
    """, (today,))
    rows = cur.fetchall()
    by_ch = {}
    for ch, y, r, g, adr in rows:
        by_ch.setdefault(ch, {})[y] = (r, g, adr)
    print(f"\n{'Channel':<12}{'r25':>5}{'r26':>5}{'Δr%':>7}{'Gross25':>10}{'Gross26':>10}{'ΔG%':>7}{'ADR25':>7}{'ADR26':>7}{'ΔADR%':>8}")
    for ch, d in sorted(by_ch.items(), key=lambda x: -(x[1].get(2026, (0,0,0))[0] or x[1].get(2025, (0,0,0))[0])):
        r25, g25, a25 = d.get(2025, (0, 0, 0))
        r26, g26, a26 = d.get(2026, (0, 0, 0))
        dr = (r26-r25)/r25*100 if r25 else None
        dg = (float(g26)-float(g25))/float(g25)*100 if g25 else None
        da = (float(a26)-float(a25))/float(a25)*100 if a25 else None
        print(f"{ch:<12}{r25:>5}{r26:>5}{fmt_pct(dr):>7}"
              f"{int(g25):>10,}{int(g26):>10,}{fmt_pct(dg):>7}"
              f"{int(a25 or 0):>7}{int(a26 or 0):>7}{fmt_pct(da):>8}")


def by_region(cur, today):
    section("5. BY REGION — YTD same-period")
    cur.execute("""
        SELECT
          region,
          EXTRACT(YEAR FROM checkin_date)::int AS y,
          COUNT(*) AS r, SUM(gross_total) AS g
        FROM v_reservation_current
        WHERE status='CONFIRMED' AND region IS NOT NULL
          AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
          AND EXTRACT(DOY FROM checkin_date) <= EXTRACT(DOY FROM %s::date)
        GROUP BY 1, 2 ORDER BY 1, 2
    """, (today,))
    rows = cur.fetchall()
    by_r = {}
    for r, y, n, g in rows:
        by_r.setdefault(r, {})[y] = (n, g)
    print(f"\n{'Region':<22}{'r25':>5}{'r26':>5}{'Δr%':>7}{'Gross25':>10}{'Gross26':>10}{'ΔG%':>7}")
    for r, d in sorted(by_r.items(), key=lambda x: -float(x[1].get(2026, (0,0))[1] or 0)):
        r25, g25 = d.get(2025, (0, 0))
        r26, g26 = d.get(2026, (0, 0))
        dr = (r26-r25)/r25*100 if r25 else None
        dg = (float(g26)-float(g25))/float(g25)*100 if g25 else None
        print(f"{r[:22]:<22}{r25:>5}{r26:>5}{fmt_pct(dr):>7}"
              f"{int(g25):>10,}{int(g26):>10,}{fmt_pct(dg):>7}")


def lead_time(cur, today):
    section("6. LEAD TIME / BOOKING WINDOW — YTD same-period")
    cur.execute("""
        SELECT
          EXTRACT(YEAR FROM checkin_date)::int AS y,
          ROUND(AVG(lead_time_days),0) AS avg_lt,
          ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lead_time_days)::numeric,0) AS median_lt,
          ROUND(AVG(nights),1) AS avg_los
        FROM v_reservation_current
        WHERE status='CONFIRMED'
          AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
          AND EXTRACT(DOY FROM checkin_date) <= EXTRACT(DOY FROM %s::date)
        GROUP BY 1 ORDER BY 1
    """, (today,))
    rows = cur.fetchall()
    print(f"\n{'Year':<6}{'Avg LT':>9}{'Med LT':>9}{'Avg LOS':>10}")
    for y, alt, mlt, los in rows:
        print(f"{y:<6}{int(alt or 0):>9}{int(mlt or 0):>9}{los or 0:>10}")
    if len(rows) == 2:
        y25, alt25, mlt25, los25 = rows[0]
        y26, alt26, mlt26, los26 = rows[1]
        print(f"\nAvg LT  {y25}→{y26}: {fmt_pct((alt26-alt25)/alt25*100)}")
        print(f"Med LT  {y25}→{y26}: {fmt_pct((mlt26-mlt25)/mlt25*100)}")
        print(f"Avg LOS {y25}→{y26}: {fmt_pct((float(los26)-float(los25))/float(los25)*100)}")


def cancellations(cur, today):
    section("7. CANCELLATIONS — YTD same-period")
    cur.execute("""
        SELECT
          EXTRACT(YEAR FROM checkin_date)::int AS y,
          status,
          COUNT(*) AS n
        FROM v_reservation_current
        WHERE EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
          AND EXTRACT(DOY FROM checkin_date) <= EXTRACT(DOY FROM %s::date)
        GROUP BY 1, 2 ORDER BY 1, 2
    """, (today,))
    rows = cur.fetchall()
    by_y = {}
    for y, s, n in rows:
        by_y.setdefault(y, {})[s] = n
    print(f"\n{'Year':<6}{'Confirmed':>10}{'Cancelled':>11}{'Cancel %':>10}")
    for y, d in sorted(by_y.items()):
        cnf = d.get('CONFIRMED', 0)
        cnc = d.get('CANCELLED', 0)
        rate = cnc/(cnf+cnc)*100 if (cnf+cnc) else 0
        print(f"{y:<6}{cnf:>10}{cnc:>11}{rate:>9.1f}%")


def forward_outlook(cur, today):
    section(f"8. FORWARD OUTLOOK — {today.isoformat()} → 2026-12-31 (on the books)")
    cur.execute("""
        SELECT
          EXTRACT(YEAR FROM checkin_date)::int AS y,
          COUNT(*) AS r, SUM(gross_total) AS g,
          ROUND(AVG(gross_total/NULLIF(nights,0)),0) AS adr
        FROM v_reservation_current
        WHERE status='CONFIRMED'
          AND ((EXTRACT(YEAR FROM checkin_date)=2026 AND checkin_date > %s)
               OR (EXTRACT(YEAR FROM checkin_date)=2025
                   AND checkin_date > make_date(2025, EXTRACT(MONTH FROM %s::date)::int,
                                                EXTRACT(DAY FROM %s::date)::int)))
        GROUP BY 1 ORDER BY 1
    """, (today, today, today))
    rows = cur.fetchall()
    print(f"\nFrom {today.isoformat()} for 2026 and STLY same-day window for 2025:")
    print(f"{'Year':<6}{'OTB':>7}{'Gross':>12}{'ADR':>7}")
    for y, r, g, adr in rows:
        print(f"{y:<6}{r:>7}{int(g or 0):>12,}{int(adr or 0):>7}")
    if len(rows) == 2:
        y25, r25, g25, a25 = rows[0]
        y26, r26, g26, a26 = rows[1]
        print(f"\nOTB Δ: {fmt_pct((r26-r25)/r25*100)} | Gross Δ: {fmt_pct((float(g26)-float(g25))/float(g25)*100)} | ADR Δ: {fmt_pct((float(a26)-float(a25))/float(a25)*100)}")


def margin_movers(cur, today):
    section("9. MARGIN PER PROPERTY — top movers YoY (≥5 reservas em ambos anos)")
    cur.execute("""
        WITH y AS (
          SELECT property_id,
                 EXTRACT(YEAR FROM checkin_date)::int AS y,
                 COUNT(*) AS r,
                 SUM(gross_total) AS g,
                 SUM(margin_direct) AS m
          FROM v_reservation_margin
          WHERE EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
            AND EXTRACT(DOY FROM checkin_date) <= EXTRACT(DOY FROM %s::date)
          GROUP BY 1, 2
        )
        SELECT p.canonical_name,
               MAX(r) FILTER (WHERE y=2025) AS r25,
               MAX(g) FILTER (WHERE y=2025) AS g25,
               MAX(m) FILTER (WHERE y=2025) AS m25,
               MAX(r) FILTER (WHERE y=2026) AS r26,
               MAX(g) FILTER (WHERE y=2026) AS g26,
               MAX(m) FILTER (WHERE y=2026) AS m26
        FROM y JOIN properties p ON p.id = y.property_id
        GROUP BY 1
        HAVING MAX(r) FILTER (WHERE y=2025) >= 5 AND MAX(r) FILTER (WHERE y=2026) >= 5
    """, (today,))
    rows = cur.fetchall()

    # Compute Δ margin
    enriched = []
    for n, r25, g25, m25, r26, g26, m26 in rows:
        dm = float(m26 or 0) - float(m25 or 0)
        enriched.append((n, r25, m25, r26, m26, dm))

    print("\nTop 10 GAINERS (margin €):")
    print(f"{'Property':<46}{'r25':>5}{'M25':>9}{'r26':>5}{'M26':>9}{'ΔM€':>9}")
    for n, r25, m25, r26, m26, dm in sorted(enriched, key=lambda x: -x[5])[:10]:
        print(f"{(n or '?')[:46]:<46}{r25 or 0:>5}{int(m25 or 0):>9,}{r26 or 0:>5}{int(m26 or 0):>9,}{int(dm):>9,}")
    print("\nTop 10 LOSERS (margin €):")
    print(f"{'Property':<46}{'r25':>5}{'M25':>9}{'r26':>5}{'M26':>9}{'ΔM€':>9}")
    for n, r25, m25, r26, m26, dm in sorted(enriched, key=lambda x: x[5])[:10]:
        print(f"{(n or '?')[:46]:<46}{r25 or 0:>5}{int(m25 or 0):>9,}{r26 or 0:>5}{int(m26 or 0):>9,}{int(dm):>9,}")


def main():
    log = setup_logging("report_2026_vs_2025")
    today = date.today()
    conn = connect()
    try:
        with conn.cursor() as cur:
            ytd_headline(cur, today)
            monthly_trajectory(cur)
            by_tier(cur, today)
            by_channel(cur, today)
            by_region(cur, today)
            lead_time(cur, today)
            cancellations(cur, today)
            forward_outlook(cur, today)
            margin_movers(cur, today)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
