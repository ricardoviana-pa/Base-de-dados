-- ============================================================================
-- Migration 011: Daily Snapshots (the YoY-honest feature)
-- ============================================================================

-- ============================================================================
-- DAILY_PROPERTY_SNAPSHOTS
-- ============================================================================

CREATE TABLE daily_property_snapshots (
  id BIGSERIAL PRIMARY KEY,
  snapshot_date DATE NOT NULL,
  property_id UUID NOT NULL REFERENCES properties(id),
  entity_id UUID NOT NULL REFERENCES entities(id),

  -- Forward-looking metrics (calculados ao tirar snapshot)
  occupancy_30d NUMERIC(5,4),
  occupancy_60d NUMERIC(5,4),
  occupancy_90d NUMERIC(5,4),
  occupancy_365d NUMERIC(5,4),

  adr_30d NUMERIC(8,2),
  adr_60d NUMERIC(8,2),
  adr_90d NUMERIC(8,2),

  revpar_30d NUMERIC(8,2),
  revpar_60d NUMERIC(8,2),
  revpar_90d NUMERIC(8,2),

  -- OTB do ano fiscal corrente
  otb_revenue_current_year NUMERIC(10,2),
  otb_nights_current_year INT,
  otb_reservations_current_year INT,

  -- BOB (apenas check-ins futuros)
  bob_revenue NUMERIC(10,2),
  bob_nights INT,
  bob_reservations INT,

  -- Pace (novas reservas)
  new_reservations_7d INT,
  new_revenue_7d NUMERIC(10,2),
  new_reservations_30d INT,
  new_revenue_30d NUMERIC(10,2),

  -- Cancellations
  cancellations_7d INT,
  cancellations_30d INT,

  -- Pricing context
  current_base_price NUMERIC(8,2),
  current_min_stay SMALLINT,
  current_cleaning_fee NUMERIC(8,2),

  -- Status no momento do snapshot
  property_status property_status,
  property_tier property_tier,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE(snapshot_date, property_id)
);
COMMENT ON TABLE daily_property_snapshots IS 'Snapshot diário do estado de cada property. Job às 02:00. Permite YoY honesto.';

CREATE INDEX idx_dps_date ON daily_property_snapshots(snapshot_date);
CREATE INDEX idx_dps_property_date ON daily_property_snapshots(property_id, snapshot_date DESC);

-- ============================================================================
-- DAILY_COMPANY_SNAPSHOTS
-- ============================================================================

CREATE TABLE daily_company_snapshots (
  id BIGSERIAL PRIMARY KEY,
  snapshot_date DATE NOT NULL,
  entity_id UUID NOT NULL REFERENCES entities(id),

  -- Portfolio
  active_properties INT,
  paused_properties INT,
  onboarding_properties INT,
  total_inventory_nights_30d INT,
  total_inventory_nights_90d INT,

  -- Revenue
  otb_revenue_ytd NUMERIC(12,2),
  otb_revenue_current_year NUMERIC(12,2),
  bob_revenue NUMERIC(12,2),
  realized_revenue_ytd NUMERIC(12,2),

  -- Pace
  new_reservations_24h INT,
  new_revenue_24h NUMERIC(10,2),
  cancellations_24h INT,
  cancellation_revenue_24h NUMERIC(10,2),

  -- Margem aproximada
  estimated_pa_revenue_ytd NUMERIC(12,2),
  estimated_margin_ytd NUMERIC(12,2),

  -- Pipeline
  open_pipeline_deals INT,
  open_pipeline_expected_revenue NUMERIC(12,2),

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE(snapshot_date, entity_id)
);
COMMENT ON TABLE daily_company_snapshots IS 'Snapshot diário ao nível empresa. Base do CEO daily pulse.';

CREATE INDEX idx_dcs_date ON daily_company_snapshots(snapshot_date);
