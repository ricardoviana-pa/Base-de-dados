"""Post-import enrichment — fills computed/derived columns that the per-source
import scripts can't easily fill inline. Idempotent: safe to run any time after
a fresh import.

What it does:
  1. Sets reservation_states.{net_stay, liquido_split, pa_revenue_gross, owner_share}
     for any rows where they are still NULL. Migration 014's trigger handles
     new inserts, but legacy rows or manual inserts may still need this.
  2. Improves cleanings.service_type by matching cleaning_date to reservation
     checkout dates (those become CO_L).
  3. Links cleanings to reservations on (property_id, cleaning_date == checkout_date).
  4. Synthesises properties.tipologia from bedrooms (T<N>) when blank.
  5. Defaults properties.current_tier to STANDARD when blank.
  6. Fills cleanings.{catalog_id, cost_net, cost_gross} via catalog lookup on
     (current_tier, tipologia, service_type), picking the cheapest variant.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.db import connect
from common.logging_utils import setup_logging


SQL_BLOCKS = [
    ("Reclassify properties.status from reservation activity", """
        WITH activity AS (
            SELECT
                p.id AS property_id,
                MAX(EXTRACT(YEAR FROM rs.checkin_date)) FILTER (WHERE rs.status IN ('CONFIRMED','COMPLETED'))::INT
                    AS last_year,
                COUNT(*) FILTER (WHERE rs.status IN ('CONFIRMED','COMPLETED')) AS n_resv,
                MAX(rs.checkout_date) FILTER (WHERE rs.status IN ('CONFIRMED','COMPLETED')) AS last_checkout
            FROM properties p
            LEFT JOIN reservations r ON r.property_id = p.id
            LEFT JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
            GROUP BY p.id
        )
        UPDATE properties p SET
            status = (CASE
                -- Moimenta da Beira (cost_centers 0052-0062 marked 'saiu')
                WHEN p.canonical_name ILIKE '%moimenta%'
                  OR p.canonical_name ILIKE '%-MB'
                  OR p.canonical_name ILIKE '%_Moimenta%' THEN 'OFFBOARDED'
                -- Active iff has 2026 confirmed reservations
                WHEN a.last_year >= 2026 THEN 'ACTIVE'
                -- Had 2025 reservations but no 2026 ones — paused (temporary)
                WHEN a.last_year = 2025 THEN 'PAUSED'
                -- Only legacy ≤2024 reservations — left the portfolio
                WHEN a.last_year IS NOT NULL AND a.last_year <= 2024 THEN 'OFFBOARDED'
                -- 0 confirmed reservations but in current Guesty/RR catalog — coming soon
                WHEN a.n_resv = 0 AND (p.guesty_id IS NOT NULL OR p.rental_ready_id IS NOT NULL)
                    THEN 'ONBOARDING'
                ELSE 'OFFBOARDED'
            END)::property_status,
            offboarded_at = CASE
                WHEN (CASE
                    WHEN p.canonical_name ILIKE '%moimenta%' OR p.canonical_name ILIKE '%-MB' OR p.canonical_name ILIKE '%_Moimenta%' THEN 'OFFBOARDED'
                    WHEN a.last_year >= 2026 THEN 'ACTIVE'
                    WHEN a.last_year = 2025 THEN 'PAUSED'
                    WHEN a.last_year IS NOT NULL AND a.last_year <= 2024 THEN 'OFFBOARDED'
                    WHEN a.n_resv = 0 AND (p.guesty_id IS NOT NULL OR p.rental_ready_id IS NOT NULL) THEN 'ONBOARDING'
                    ELSE 'OFFBOARDED'
                END) = 'OFFBOARDED' THEN COALESCE(p.offboarded_at, a.last_checkout, CURRENT_DATE)
                ELSE NULL
            END
        FROM activity a
        WHERE a.property_id = p.id
    """),

    ("Backfill reservation_states financial split", """
        UPDATE reservation_states SET
          net_stay = gross_total - COALESCE(vat_stay, 0) - COALESCE(cleaning_fee_gross, 0),
          liquido_split = (gross_total - COALESCE(vat_stay, 0) - COALESCE(cleaning_fee_gross, 0))
                          - COALESCE(channel_commission, 0),
          pa_revenue_gross = ((gross_total - COALESCE(vat_stay, 0) - COALESCE(cleaning_fee_gross, 0))
                              - COALESCE(channel_commission, 0))
                             * COALESCE(pa_commission_rate, 0.40),
          owner_share = ((gross_total - COALESCE(vat_stay, 0) - COALESCE(cleaning_fee_gross, 0))
                         - COALESCE(channel_commission, 0))
                        * (1 - COALESCE(pa_commission_rate, 0.40))
        WHERE pa_revenue_gross IS NULL AND gross_total IS NOT NULL
    """),

    ("Set cleanings.service_type=CO_L on checkout-day matches", """
        UPDATE cleanings c SET service_type = 'CO_L'
        FROM reservations r
        JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
        WHERE r.property_id = c.property_id
          AND rs.checkout_date = c.cleaning_date
    """),

    ("Link cleanings to reservations", """
        UPDATE cleanings c SET reservation_id = r.id
        FROM reservations r
        JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
        WHERE r.property_id = c.property_id
          AND rs.checkout_date = c.cleaning_date
          AND c.reservation_id IS NULL
    """),

    ("Synthesise properties.tipologia from bedrooms (T<N>)", """
        UPDATE properties SET tipologia = 'T' || COALESCE(bedrooms, 0)::text
        WHERE tipologia IS NULL AND bedrooms IS NOT NULL
    """),

    ("Default properties.current_tier=STANDARD where blank", """
        UPDATE properties SET current_tier = 'STANDARD'
        WHERE current_tier IS NULL AND tipologia IS NOT NULL
    """),

    ("Fill cleanings.{catalog_id,cost_net,cost_gross} via catalog lookup", """
        WITH ranked AS (
            SELECT c.id AS cleaning_id, csc.id AS catalog_id,
                   csc.cost_net, csc.cost_with_vat_6,
                   ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY csc.cost_net ASC) AS rn
            FROM cleanings c
            JOIN properties p ON p.id = c.property_id
            JOIN cleaning_service_catalog csc
              ON csc.tier = p.current_tier
             AND csc.service_type = c.service_type
             AND csc.effective_to IS NULL
             AND (csc.tipologia = p.tipologia
                  OR csc.tipologia ILIKE p.tipologia || ' -%'
                  OR csc.tipologia ILIKE p.tipologia || ' %')
            WHERE c.cost_gross IS NULL
              AND p.current_tier IS NOT NULL
              AND p.tipologia IS NOT NULL
        )
        UPDATE cleanings c SET
            catalog_id = ranked.catalog_id,
            cost_net = ranked.cost_net,
            cost_gross = ranked.cost_with_vat_6
        FROM ranked
        WHERE c.id = ranked.cleaning_id AND ranked.rn = 1
    """),
]


def main() -> int:
    log = setup_logging("post_import_enrich")
    conn = connect()
    try:
        with conn.cursor() as cur:
            for label, sql in SQL_BLOCKS:
                cur.execute(sql)
                log.info(f"  {label}: {cur.rowcount} rows affected")
        conn.commit()

        # Final stats
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  (SELECT COUNT(*) FROM reservation_states WHERE pa_revenue_gross IS NULL),
                  (SELECT COUNT(*) FROM cleanings WHERE cost_gross IS NULL),
                  (SELECT COUNT(*) FROM cleanings WHERE reservation_id IS NULL),
                  (SELECT COUNT(*) FROM properties WHERE tipologia IS NULL),
                  (SELECT COUNT(*) FROM properties WHERE current_tier IS NULL)
            """)
            r = cur.fetchone()
            log.info("=" * 60)
            log.info(f"Residual NULLs after enrichment:")
            log.info(f"  reservation_states.pa_revenue_gross : {r[0]}")
            log.info(f"  cleanings.cost_gross                : {r[1]}")
            log.info(f"  cleanings.reservation_id (unlinked) : {r[2]}")
            log.info(f"  properties.tipologia                : {r[3]}")
            log.info(f"  properties.current_tier             : {r[4]}")
        return 0
    except Exception as exc:
        conn.rollback()
        log.exception(f"Enrichment failed: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
