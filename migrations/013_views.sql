-- ============================================================================
-- Migration 013: Analytical Views
-- ============================================================================
-- These views encapsulate business logic. Apps consume views, never raw tables.

-- ============================================================================
-- v_reservation_current — Estado atual + property + owner enriched
-- ============================================================================

CREATE OR REPLACE VIEW v_reservation_current AS
SELECT
  r.id,
  r.entity_id,
  r.source_system,
  r.source_id,
  r.channel,
  r.booked_at,
  r.property_id,
  p.canonical_name AS property_name,
  p.region,
  p.current_tier,
  p.tipologia,
  p.cost_center_id,
  o.id AS owner_id,
  o.legal_name AS owner_name,
  rs.status,
  rs.checkin_date,
  rs.checkout_date,
  rs.nights,
  rs.adults,
  rs.children,
  rs.babies,
  rs.pax,
  rs.gross_total,
  rs.cleaning_fee_gross,
  rs.cleaning_fee_net,
  rs.channel_commission,
  rs.channel_commission_pct,
  rs.pa_commission_rate,
  rs.net_stay,
  rs.liquido_split,
  rs.pa_revenue_gross,
  rs.owner_share,
  rs.vat_stay,
  rs.vat_cleaning,
  EXTRACT(EPOCH FROM (rs.checkin_date::timestamp - r.booked_at)) / 86400 AS lead_time_days
FROM reservations r
INNER JOIN reservation_states rs
  ON rs.reservation_id = r.id AND rs.effective_to IS NULL
LEFT JOIN properties p ON r.property_id = p.id
LEFT JOIN owners o ON p.owner_id = o.id;

COMMENT ON VIEW v_reservation_current IS 'Reserva no estado atual, com property+owner. Use isto, nunca reservation_states diretamente para queries normais.';

-- ============================================================================
-- v_reservation_margin — Margem em 3 níveis
-- ============================================================================

CREATE OR REPLACE VIEW v_reservation_margin AS
SELECT
  rc.id AS reservation_id,
  rc.entity_id,
  rc.property_id,
  rc.property_name,
  rc.checkin_date,
  rc.gross_total,
  rc.pa_revenue_gross,

  -- Custos diretos atribuíveis
  COALESCE(c.total_cleaning, 0) AS direct_cleaning_cost,
  COALESCE(l.total_laundry, 0) AS direct_laundry_cost,

  -- Margem nível 1: PA revenue menos custos diretos
  rc.pa_revenue_gross
    - COALESCE(c.total_cleaning, 0)
    - COALESCE(l.total_laundry, 0)
    AS margin_direct,

  -- Margem % nível 1
  CASE WHEN rc.gross_total > 0 THEN
    (rc.pa_revenue_gross - COALESCE(c.total_cleaning, 0) - COALESCE(l.total_laundry, 0)) / rc.gross_total
  ELSE NULL END AS margin_direct_pct

FROM v_reservation_current rc
LEFT JOIN (
  SELECT reservation_id, SUM(cost_gross) AS total_cleaning
  FROM cleanings WHERE reservation_id IS NOT NULL
  GROUP BY reservation_id
) c ON c.reservation_id = rc.id
LEFT JOIN (
  SELECT reservation_id, SUM(total_paid) AS total_laundry
  FROM laundry WHERE reservation_id IS NOT NULL
  GROUP BY reservation_id
) l ON l.reservation_id = rc.id;

COMMENT ON VIEW v_reservation_margin IS 'Margem por reserva. Nível 1: revenue PA menos cleanings+laundry diretas.';

-- ============================================================================
-- v_otb_vs_stly — OTB vs STLY (Same Time Last Year)
-- ============================================================================

CREATE OR REPLACE VIEW v_otb_vs_stly AS
WITH today_param AS (SELECT CURRENT_DATE AS today)
SELECT
  -- OTB ano corrente
  (SELECT COALESCE(SUM(rs.gross_total), 0)
   FROM reservations r
   JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
   WHERE rs.status = 'CONFIRMED'
     AND r.booked_at <= (SELECT today FROM today_param)
     AND EXTRACT(YEAR FROM rs.checkin_date) = EXTRACT(YEAR FROM (SELECT today FROM today_param))
  ) AS otb_revenue_current_year,

  (SELECT COUNT(*)
   FROM reservations r
   JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
   WHERE rs.status = 'CONFIRMED'
     AND r.booked_at <= (SELECT today FROM today_param)
     AND EXTRACT(YEAR FROM rs.checkin_date) = EXTRACT(YEAR FROM (SELECT today FROM today_param))
  ) AS otb_reservations_current_year,

  -- OTB STLY (mesma data calendário ano passado, ano fiscal anterior)
  (SELECT COALESCE(SUM(rs.gross_total), 0)
   FROM reservations r
   JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
   WHERE rs.status = 'CONFIRMED'
     AND r.booked_at <= ((SELECT today FROM today_param) - INTERVAL '1 year')
     AND EXTRACT(YEAR FROM rs.checkin_date) = EXTRACT(YEAR FROM ((SELECT today FROM today_param) - INTERVAL '1 year'))
  ) AS otb_revenue_stly,

  (SELECT COUNT(*)
   FROM reservations r
   JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
   WHERE rs.status = 'CONFIRMED'
     AND r.booked_at <= ((SELECT today FROM today_param) - INTERVAL '1 year')
     AND EXTRACT(YEAR FROM rs.checkin_date) = EXTRACT(YEAR FROM ((SELECT today FROM today_param) - INTERVAL '1 year'))
  ) AS otb_reservations_stly,

  -- BOB (apenas check-ins futuros)
  (SELECT COALESCE(SUM(rs.gross_total), 0)
   FROM reservations r
   JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
   WHERE rs.status = 'CONFIRMED' AND rs.checkin_date > (SELECT today FROM today_param)
  ) AS bob_revenue,

  (SELECT today FROM today_param) AS as_of_date;

COMMENT ON VIEW v_otb_vs_stly AS 'OTB ano corrente vs STLY + BOB. Resolve os 4 valores diferentes do dashboard antigo.';

-- ============================================================================
-- v_property_performance_monthly — KPIs mensais por property
-- ============================================================================

CREATE OR REPLACE VIEW v_property_performance_monthly AS
SELECT
  p.id AS property_id,
  p.canonical_name,
  p.region,
  p.current_tier,
  EXTRACT(YEAR FROM rs.checkin_date)::INT AS year,
  EXTRACT(MONTH FROM rs.checkin_date)::INT AS month,
  COUNT(*) AS reservations_count,
  SUM(rs.nights) AS total_nights,
  SUM(rs.gross_total) AS gross_revenue,
  SUM(rs.pa_revenue_gross) AS pa_revenue,
  SUM(rs.owner_share) AS owner_share,
  CASE WHEN SUM(rs.nights) > 0 THEN SUM(rs.gross_total) / SUM(rs.nights) ELSE NULL END AS adr,
  AVG(rs.nights) AS avg_los
FROM properties p
JOIN reservations r ON r.property_id = p.id
JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
WHERE rs.status IN ('CONFIRMED', 'COMPLETED')
GROUP BY p.id, p.canonical_name, p.region, p.current_tier,
         EXTRACT(YEAR FROM rs.checkin_date), EXTRACT(MONTH FROM rs.checkin_date);

COMMENT ON VIEW v_property_performance_monthly IS 'Performance mensal por property. Base do property ranking.';

-- ============================================================================
-- v_company_pnl_monthly — P&L mensal da empresa (real)
-- ============================================================================

CREATE OR REPLACE VIEW v_company_pnl_monthly AS
SELECT
  pm.entity_id,
  pm.year,
  pm.month,
  pa.management_category,
  pa.account_class,
  SUM(pm.movement_credit - pm.movement_debit) FILTER (WHERE pa.account_type = 'REVENUE') AS revenue,
  SUM(pm.movement_debit - pm.movement_credit) FILTER (WHERE pa.account_type = 'EXPENSE') AS expense,
  SUM(pm.balance) AS balance
FROM monthly_pnl pm
JOIN primavera_accounts pa ON pa.account_code = pm.account_code
GROUP BY pm.entity_id, pm.year, pm.month, pa.management_category, pa.account_class;

COMMENT ON VIEW v_company_pnl_monthly IS 'P&L mensal agregado por categoria de gestão. Reconciliável com balancete EB Consultores.';

-- ============================================================================
-- v_budget_vs_actual_monthly — Real vs Budget
-- ============================================================================

CREATE OR REPLACE VIEW v_budget_vs_actual_monthly AS
SELECT
  b.entity_id,
  b.fiscal_year AS year,
  blp.month,
  blp.property_id,
  blp.placeholder_label,
  blp.property_label,
  blp.revenue_amount AS budget_revenue,
  COALESCE(actual.revenue, 0) AS actual_revenue,
  COALESCE(actual.revenue, 0) - COALESCE(blp.revenue_amount, 0) AS variance_revenue,
  CASE WHEN blp.revenue_amount > 0
       THEN (COALESCE(actual.revenue, 0) - blp.revenue_amount) / blp.revenue_amount
       ELSE NULL END AS variance_pct,
  blp.margin_amount AS budget_margin,
  blp.margin_pct AS budget_margin_pct
FROM budgets b
JOIN budget_lines_property blp ON blp.budget_id = b.id
LEFT JOIN (
  SELECT
    property_id,
    EXTRACT(YEAR FROM rs.checkin_date)::INT AS year,
    EXTRACT(MONTH FROM rs.checkin_date)::INT AS month,
    SUM(rs.gross_total) AS revenue
  FROM reservations r
  JOIN reservation_states rs ON rs.reservation_id = r.id AND rs.effective_to IS NULL
  WHERE rs.status IN ('CONFIRMED', 'COMPLETED')
  GROUP BY property_id, EXTRACT(YEAR FROM rs.checkin_date), EXTRACT(MONTH FROM rs.checkin_date)
) actual ON actual.property_id = blp.property_id
         AND actual.year = b.fiscal_year
         AND actual.month = blp.month
WHERE b.status = 'APPROVED';

COMMENT ON VIEW v_budget_vs_actual_monthly IS 'Real vs Budget mês a mês. Usa apenas budget APPROVED.';
