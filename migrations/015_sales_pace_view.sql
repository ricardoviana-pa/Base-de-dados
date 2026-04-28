-- ============================================================================
-- Migration 015: v_sales_pace_ytd — booking pace YTD vs prior YTD
-- ============================================================================
-- "Quanto vendemos este ano" vs "quanto vendemos no mesmo período do ano passado".
-- This is the metric that answers "are we selling faster this year?" — independent
-- of which year the check-in falls in. It's a complement to v_otb_vs_stly which
-- answers "how saturated is the current year's revenue book?".

CREATE OR REPLACE VIEW v_sales_pace_ytd AS
WITH today_param AS (SELECT CURRENT_DATE AS today)
SELECT
  -- Sales booked between Jan 1 and today, in the current calendar year
  (SELECT COALESCE(SUM(rs.gross_total), 0)
   FROM reservations r
   JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
   WHERE rs.status IN ('CONFIRMED', 'COMPLETED')
     AND r.booked_at >= DATE_TRUNC('year', (SELECT today FROM today_param))
     AND r.booked_at <= (SELECT today FROM today_param)
  ) AS sales_ytd_current_year,

  (SELECT COUNT(*)
   FROM reservations r
   JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
   WHERE rs.status IN ('CONFIRMED', 'COMPLETED')
     AND r.booked_at >= DATE_TRUNC('year', (SELECT today FROM today_param))
     AND r.booked_at <= (SELECT today FROM today_param)
  ) AS reservations_ytd_current_year,

  -- Same calendar window in the previous year
  (SELECT COALESCE(SUM(rs.gross_total), 0)
   FROM reservations r
   JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
   WHERE rs.status IN ('CONFIRMED', 'COMPLETED')
     AND r.booked_at >= DATE_TRUNC('year', (SELECT today FROM today_param)) - INTERVAL '1 year'
     AND r.booked_at <= (SELECT today FROM today_param) - INTERVAL '1 year'
  ) AS sales_ytd_prior_year,

  (SELECT COUNT(*)
   FROM reservations r
   JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
   WHERE rs.status IN ('CONFIRMED', 'COMPLETED')
     AND r.booked_at >= DATE_TRUNC('year', (SELECT today FROM today_param)) - INTERVAL '1 year'
     AND r.booked_at <= (SELECT today FROM today_param) - INTERVAL '1 year'
  ) AS reservations_ytd_prior_year,

  (SELECT today FROM today_param) AS as_of_date;

COMMENT ON VIEW v_sales_pace_ytd IS
  'Booking pace YTD: gross sold between Jan 1 and today this year vs. same calendar window last year. Independent of check-in year — measures sales velocity, not OTB saturation.';
