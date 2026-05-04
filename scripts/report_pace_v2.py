"""Pace Report v2 — clean YoY view (EBEN excluded, no tiers).

Focused on:
  1. Total revenue + reservations evolution YoY
  2. BOB (book-on-books) vs STLY by checkin-month
  3. Summer (Jul-Aug) deep dive — sou OK para o verão?
  4. Year evolution: monthly trajectory
  5. By channel
  6. Year-end forecast
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.db import connect
from common.logging_utils import setup_logging

# Properties to exclude from analysis (e.g. EBEN LODGE saiu da gestão)
EXCLUDED_PROP_IDS = (
    "76a3c435-dd0e-4785-b64a-6a7d5bf1f1e9",  # EBEN LODGE
    "796975ae-8da3-496f-931c-a6196a89f05f",  # T5-Eben Lodge
)

EXCLUDE_SQL = "AND property_id NOT IN ({})".format(
    ",".join(f"'{p}'" for p in EXCLUDED_PROP_IDS)
)


def section(title: str):
    print()
    print("=" * 90)
    print(title)
    print("=" * 90)


def fmt_pct(v):
    return f"{v:+.1f}%" if v is not None else "—"


def realised_ytd(cur, today):
    section(f"1. REALISED YTD — Jan 1 → {today.isoformat()}  (excl. EBEN)")
    cur.execute(f"""
        SELECT
          EXTRACT(YEAR FROM checkin_date)::int AS y,
          COUNT(*) AS r,
          SUM(nights) AS n,
          SUM(gross_total) AS g,
          SUM(pa_revenue_gross) AS pa,
          ROUND(AVG(gross_total/NULLIF(nights,0)),0) AS adr,
          ROUND(AVG(lead_time_days),0) AS lt
        FROM v_reservation_current
        WHERE status='CONFIRMED' {EXCLUDE_SQL}
          AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
          AND EXTRACT(DOY FROM checkin_date) <= EXTRACT(DOY FROM %s::date)
        GROUP BY 1 ORDER BY 1
    """, (today,))
    rows = cur.fetchall()
    print(f"\n{'Year':<6}{'Resv':>7}{'Nights':>8}{'Gross':>13}{'PA Rev':>12}{'ADR':>7}{'LeadT':>7}")
    for y, r, n, g, pa, adr, lt in rows:
        print(f"{y:<6}{r:>7}{n:>8} €{int(g or 0):>10,} €{int(pa or 0):>9,}  €{int(adr or 0):>4}{int(lt or 0):>7}")
    if len(rows) == 2:
        (y25, r25, n25, g25, pa25, a25, lt25), (y26, r26, n26, g26, pa26, a26, lt26) = rows
        print(f"\n{'Δ':<6}{fmt_pct((r26-r25)/r25*100):>7}"
              f"{fmt_pct((n26-n25)/n25*100):>8}"
              f"{fmt_pct((float(g26)-float(g25))/float(g25)*100):>13}"
              f"{fmt_pct((float(pa26)-float(pa25))/float(pa25)*100):>12}"
              f"{fmt_pct((float(a26)-float(a25))/float(a25)*100):>7}"
              f"{fmt_pct((float(lt26)-float(lt25))/float(lt25)*100):>7}")


def bob_vs_stly(cur, today):
    section(f"2. BOB vs STLY — booked-on-books at {today.isoformat()} for each month  (excl. EBEN)")
    print("\nBOB = reservations confirmed with booked_at ≤ today (this year and same date last year),")
    print("with check-in falling in each month.\n")
    cur.execute(f"""
        WITH bob AS (
          SELECT
            EXTRACT(YEAR FROM checkin_date)::int AS y,
            EXTRACT(MONTH FROM checkin_date)::int AS m,
            COUNT(*) AS r,
            SUM(nights) AS n,
            SUM(gross_total) AS g
          FROM v_reservation_current
          WHERE status='CONFIRMED' {EXCLUDE_SQL}
            AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
            AND booked_at IS NOT NULL
            AND booked_at::date <= make_date(EXTRACT(YEAR FROM checkin_date)::int,
                                              EXTRACT(MONTH FROM %s::date)::int,
                                              EXTRACT(DAY FROM %s::date)::int)
          GROUP BY 1, 2
        )
        SELECT m,
          MAX(r) FILTER (WHERE y=2025) AS r25, MAX(n) FILTER (WHERE y=2025) AS n25, MAX(g) FILTER (WHERE y=2025) AS g25,
          MAX(r) FILTER (WHERE y=2026) AS r26, MAX(n) FILTER (WHERE y=2026) AS n26, MAX(g) FILTER (WHERE y=2026) AS g26
        FROM bob GROUP BY 1 ORDER BY 1
    """, (today, today))
    months = ["", "Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    print(f"{'Month':<6}{'Resv25':>7}{'Resv26':>7}{'Δr%':>7}{'Gross25':>11}{'Gross26':>11}{'ΔG%':>7}{'Status':>14}")
    rows = cur.fetchall()
    tot_r25 = tot_r26 = 0
    tot_g25 = tot_g26 = 0.0
    summer_g25 = summer_g26 = 0.0
    summer_r25 = summer_r26 = 0
    for m, r25, n25, g25, r26, n26, g26 in rows:
        r25 = r25 or 0; r26 = r26 or 0
        g25 = float(g25 or 0); g26 = float(g26 or 0)
        dr = (r26-r25)/r25*100 if r25 else None
        dg = (g26-g25)/g25*100 if g25 else None
        # Status indicator
        if dg is None:
            status = "—"
        elif dg >= 10:
            status = "✓ AHEAD"
        elif dg >= -5:
            status = "≈ FLAT"
        else:
            status = "⚠ BEHIND"
        tot_r25 += r25; tot_r26 += r26; tot_g25 += g25; tot_g26 += g26
        if m in (7, 8):
            summer_r25 += r25; summer_r26 += r26
            summer_g25 += g25; summer_g26 += g26
        print(f"{months[m]:<6}{r25:>7}{r26:>7}{fmt_pct(dr):>7}"
              f" €{int(g25):>9,} €{int(g26):>9,}{fmt_pct(dg):>7}{status:>14}")
    print("-" * 90)
    dr_t = (tot_r26-tot_r25)/tot_r25*100 if tot_r25 else None
    dg_t = (tot_g26-tot_g25)/tot_g25*100 if tot_g25 else None
    print(f"{'TOTAL':<6}{tot_r25:>7}{tot_r26:>7}{fmt_pct(dr_t):>7} €{int(tot_g25):>9,} €{int(tot_g26):>9,}{fmt_pct(dg_t):>7}")
    return summer_r25, summer_r26, summer_g25, summer_g26


def summer_deepdive(cur, today, summer):
    section("3. SUMMER DEEP DIVE — Julho + Agosto 2026 vs 2025  (excl. EBEN)")
    sr25, sr26, sg25, sg26 = summer
    dr = (sr26-sr25)/sr25*100 if sr25 else None
    dg = (sg26-sg25)/sg25*100 if sg25 else None
    print(f"\nBOB (no mesmo ponto do ano) para Jul + Ago combinados:")
    print(f"  2025-05-04 had booked: {sr25:>4} reservas  €{int(sg25):>9,}")
    print(f"  2026-05-04 has booked: {sr26:>4} reservas  €{int(sg26):>9,}")
    print(f"  Δ pace summer:         {fmt_pct(dr)}        {fmt_pct(dg)}")

    # 2025 final summer (full)
    cur.execute(f"""
        SELECT COUNT(*), SUM(gross_total), ROUND(AVG(gross_total/NULLIF(nights,0)),0)
        FROM v_reservation_current
        WHERE status='CONFIRMED' {EXCLUDE_SQL}
          AND EXTRACT(YEAR FROM checkin_date)=2025
          AND EXTRACT(MONTH FROM checkin_date) IN (7, 8)
    """)
    r25_final, g25_final, a25_final = cur.fetchone()

    # 2025 — what came in AFTER same date (late bookings)
    cur.execute(f"""
        SELECT COUNT(*), SUM(gross_total)
        FROM v_reservation_current
        WHERE status='CONFIRMED' {EXCLUDE_SQL}
          AND EXTRACT(YEAR FROM checkin_date)=2025
          AND EXTRACT(MONTH FROM checkin_date) IN (7, 8)
          AND booked_at::date > make_date(2025, EXTRACT(MONTH FROM %s::date)::int, EXTRACT(DAY FROM %s::date)::int)
    """, (today, today))
    r25_late, g25_late = cur.fetchone()

    print(f"\n2025 final (verão completo):     {r25_final:>4} reservas  €{int(g25_final or 0):>9,}  ADR €{int(a25_final or 0)}")
    print(f"2025 late bookings (entradas após 4-Mai-25): {r25_late:>4} reservas  €{int(g25_late or 0):>9,}")
    print(f"  → Para fechar 2025 contámos com {r25_late} reservas adicionais e €{int(g25_late or 0):,} de gross.")

    # Project 2026 summer end-state
    if sr25 and r25_late:
        late_mult_g = float(g25_late) / sg25  # how much more than what was on books
        late_mult_r = r25_late / sr25
        proj_r = sr26 + int(sr26 * late_mult_r)
        proj_g = sg26 + sg26 * late_mult_g
        print(f"\nMultiplicador 2025: cada €1 OTB virou €{1+late_mult_g:.2f} no fim, cada reserva trouxe {1+late_mult_r:.2f}.")
        print(f"\n🎯 PROJEÇÃO VERÃO 2026 (se padrão se repete):")
        print(f"   {proj_r:>4} reservas  €{int(proj_g):>9,}  ADR €{int(proj_g/(proj_r*5)) if proj_r else 0}*  (*assumindo LOS 5 noites)")
        print(f"   vs 2025 final: {r25_final:>4} reservas  €{int(g25_final or 0):>9,}")
        if r25_final and g25_final:
            print(f"   Δ:             {fmt_pct((proj_r-r25_final)/r25_final*100):>5}                       {fmt_pct((proj_g-float(g25_final))/float(g25_final)*100):>10}")


def by_channel(cur, today):
    section("4. PACE POR CANAL — BOB at same DOY  (excl. EBEN)")
    cur.execute(f"""
        WITH bob AS (
          SELECT
            EXTRACT(YEAR FROM checkin_date)::int AS y,
            channel,
            COUNT(*) AS r, SUM(gross_total) AS g
          FROM v_reservation_current
          WHERE status='CONFIRMED' {EXCLUDE_SQL} AND channel IS NOT NULL
            AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
            AND booked_at IS NOT NULL
            AND booked_at::date <= make_date(EXTRACT(YEAR FROM checkin_date)::int,
                                              EXTRACT(MONTH FROM %s::date)::int,
                                              EXTRACT(DAY FROM %s::date)::int)
          GROUP BY 1, 2
        )
        SELECT channel,
          MAX(r) FILTER (WHERE y=2025), MAX(g) FILTER (WHERE y=2025),
          MAX(r) FILTER (WHERE y=2026), MAX(g) FILTER (WHERE y=2026)
        FROM bob GROUP BY 1
        HAVING COALESCE(MAX(r) FILTER (WHERE y=2025),0) + COALESCE(MAX(r) FILTER (WHERE y=2026),0) > 0
        ORDER BY GREATEST(COALESCE(MAX(r) FILTER (WHERE y=2025),0), COALESCE(MAX(r) FILTER (WHERE y=2026),0)) DESC
    """, (today, today))
    print(f"\n{'Channel':<12}{'r25':>6}{'Gross25':>11}{'r26':>6}{'Gross26':>11}{'Δr%':>7}{'ΔG%':>7}")
    for ch, r25, g25, r26, g26 in cur.fetchall():
        r25 = r25 or 0; r26 = r26 or 0
        g25 = float(g25 or 0); g26 = float(g26 or 0)
        dr = (r26-r25)/r25*100 if r25 else None
        dg = (g26-g25)/g25*100 if g25 else None
        print(f"{ch:<12}{r25:>6} €{int(g25):>9,}{r26:>6} €{int(g26):>9,}{fmt_pct(dr):>7}{fmt_pct(dg):>7}")


def yearend_forecast(cur, today):
    section(f"5. YEAR-END FORECAST 2026  (excl. EBEN)")
    cur.execute(f"""
        SELECT COUNT(*), SUM(gross_total)
        FROM v_reservation_current
        WHERE status='CONFIRMED' {EXCLUDE_SQL} AND EXTRACT(YEAR FROM checkin_date)=2025
    """)
    r25_full, g25_full = cur.fetchone()

    cur.execute(f"""
        SELECT COUNT(*), SUM(gross_total)
        FROM v_reservation_current
        WHERE status='CONFIRMED' {EXCLUDE_SQL} AND EXTRACT(YEAR FROM checkin_date)=2026
          AND checkin_date <= %s
    """, (today,))
    r26_real, g26_real = cur.fetchone()

    cur.execute(f"""
        SELECT COUNT(*), SUM(gross_total)
        FROM v_reservation_current
        WHERE status='CONFIRMED' {EXCLUDE_SQL} AND EXTRACT(YEAR FROM checkin_date)=2026
          AND checkin_date > %s
    """, (today,))
    r26_otb, g26_otb = cur.fetchone()

    cur.execute(f"""
        SELECT COUNT(*), SUM(gross_total)
        FROM v_reservation_current
        WHERE status='CONFIRMED' {EXCLUDE_SQL} AND EXTRACT(YEAR FROM checkin_date)=2025
          AND checkin_date > make_date(2025, EXTRACT(MONTH FROM %s::date)::int, EXTRACT(DAY FROM %s::date)::int)
          AND booked_at::date <= make_date(2025, EXTRACT(MONTH FROM %s::date)::int, EXTRACT(DAY FROM %s::date)::int)
    """, (today, today, today, today))
    r25_otb_stly, g25_otb_stly = cur.fetchone()

    cur.execute(f"""
        SELECT COUNT(*), SUM(gross_total)
        FROM v_reservation_current
        WHERE status='CONFIRMED' {EXCLUDE_SQL} AND EXTRACT(YEAR FROM checkin_date)=2025
          AND checkin_date > make_date(2025, EXTRACT(MONTH FROM %s::date)::int, EXTRACT(DAY FROM %s::date)::int)
          AND booked_at::date > make_date(2025, EXTRACT(MONTH FROM %s::date)::int, EXTRACT(DAY FROM %s::date)::int)
    """, (today, today, today, today))
    r25_late, g25_late = cur.fetchone()

    print(f"\n{'2025 final (full year)':<40}{r25_full or 0:>7} reservas  €{int(g25_full or 0):>10,}")
    print(f"{'2026 realised (Jan→today)':<40}{r26_real or 0:>7} reservas  €{int(g26_real or 0):>10,}")
    print(f"{'2026 OTB future (booked, not arrived)':<40}{r26_otb or 0:>7} reservas  €{int(g26_otb or 0):>10,}")
    print(f"{'  vs 2025 OTB at same date':<40}{r25_otb_stly or 0:>7} reservas  €{int(g25_otb_stly or 0):>10,}  ← BOB STLY")
    print(f"{'  2025 late bookings (after that date)':<40}{r25_late or 0:>7} reservas  €{int(g25_late or 0):>10,}")

    if r25_otb_stly and r25_late:
        late_mult_g = float(g25_late) / float(g25_otb_stly)
        late_mult_r = r25_late / r25_otb_stly
        proj_late_r = int((r26_otb or 0) * late_mult_r)
        proj_late_g = float(g26_otb or 0) * late_mult_g
        forecast_r = (r26_real or 0) + (r26_otb or 0) + proj_late_r
        forecast_g = float(g26_real or 0) + float(g26_otb or 0) + proj_late_g
        print(f"\n  Multiplicador 2025: {late_mult_r:.2f}x reservas, {late_mult_g:.2f}x revenue")
        print(f"  → Late-booking projection 2026:        {proj_late_r:>4} reservas  €{int(proj_late_g):>9,}")
        print(f"\n  🎯 YE FORECAST 2026:                    {forecast_r:>4} reservas  €{int(forecast_g):>9,}")
        if r25_full and g25_full:
            dr = (forecast_r - r25_full)/r25_full*100
            dg = (forecast_g - float(g25_full))/float(g25_full)*100
            print(f"     vs 2025 actuals:                     {r25_full:>4} reservas  €{int(g25_full):>9,}")
            print(f"     YE Δ:                                {fmt_pct(dr):>5}              {fmt_pct(dg):>10}")


def evolution(cur, today):
    section("6. EVOLUÇÃO MENSAL DO ANO — todos os meses YoY (excl. EBEN)")
    print("\n(Com `r25/g25/...` = realised TOTAL final do mês 2025; r26/g26 = OTB hoje para mês 2026)")
    cur.execute(f"""
        SELECT
          EXTRACT(MONTH FROM checkin_date)::int AS m,
          EXTRACT(YEAR FROM checkin_date)::int AS y,
          COUNT(*) AS r,
          SUM(gross_total) AS g
        FROM v_reservation_current
        WHERE status='CONFIRMED' {EXCLUDE_SQL}
          AND EXTRACT(YEAR FROM checkin_date) IN (2025, 2026)
        GROUP BY 1, 2
    """)
    by_m = {}
    for m, y, r, g in cur.fetchall():
        by_m.setdefault(m, {})[y] = (r, g)
    months = ["", "Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    print(f"\n{'Month':<6}{'r25 final':>11}{'g25 final':>13}{'r26 BOB':>10}{'g26 BOB':>13}{'Coverage':>11}")
    print("-" * 75)
    for m in range(1, 13):
        d = by_m.get(m, {})
        r25, g25 = d.get(2025, (0, 0))
        r26, g26 = d.get(2026, (0, 0))
        cov = float(g26 or 0) / float(g25) * 100 if g25 else None
        cov_str = f"{cov:.0f}%" if cov is not None else "—"
        print(f"{months[m]:<6}{r25 or 0:>11}€{int(g25 or 0):>11,}{r26 or 0:>10}€{int(g26 or 0):>11,}{cov_str:>11}")


def main():
    log = setup_logging("report_pace_v2")
    today = date.today()
    conn = connect()
    try:
        with conn.cursor() as cur:
            realised_ytd(cur, today)
            summer = bob_vs_stly(cur, today)
            summer_deepdive(cur, today, summer)
            by_channel(cur, today)
            yearend_forecast(cur, today)
            evolution(cur, today)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
