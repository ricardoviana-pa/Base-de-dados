-- ============================================================================
-- Migration 007: Pricing Decisions + PriceLabs Snapshots
-- ============================================================================

-- ============================================================================
-- PRICING_DECISIONS (cérebro do Revenue AI)
-- ============================================================================

CREATE TABLE pricing_decisions (
  id BIGSERIAL PRIMARY KEY,
  property_id UUID NOT NULL REFERENCES properties(id),
  entity_id UUID NOT NULL REFERENCES entities(id),

  decision_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  decision_type pricing_decision_type NOT NULL,

  -- Range de aplicação
  date_range_from DATE,
  date_range_to DATE,

  -- Antes/depois
  previous_value NUMERIC(10,2),
  new_value NUMERIC(10,2),

  -- Quem e porquê
  triggered_by TEXT NOT NULL,  -- 'USER:ricardo', 'AI_RM_RULE', 'PRICELABS_AUTO', 'AI_RM_CLAUDE'
  rule_name TEXT,
  reasoning TEXT,

  -- Contexto no momento (snapshot serializado)
  context_snapshot JSONB,

  -- Outcome (preenchido depois para learning)
  outcome_measured_at TIMESTAMPTZ,
  reservations_after_decision INT,
  revenue_after_decision NUMERIC(10,2),
  outcome_classification TEXT CHECK (
    outcome_classification IN ('POSITIVE', 'NEUTRAL', 'NEGATIVE', 'UNCLEAR') OR outcome_classification IS NULL
  ),
  outcome_notes TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE pricing_decisions IS 'Memória de decisões de pricing. Cada alteração de preço/min stay/cleaning fee é registada com contexto e outcome. Base de aprendizagem do Revenue AI.';

CREATE INDEX idx_pd_property ON pricing_decisions(property_id, decision_at);
CREATE INDEX idx_pd_type ON pricing_decisions(decision_type);
CREATE INDEX idx_pd_outcome ON pricing_decisions(outcome_classification) WHERE outcome_classification IS NOT NULL;
CREATE INDEX idx_pd_decision_at ON pricing_decisions(decision_at);

-- ============================================================================
-- PRICELABS_SNAPSHOTS
-- ============================================================================

CREATE TABLE pricelabs_snapshots (
  id BIGSERIAL PRIMARY KEY,
  property_id UUID NOT NULL REFERENCES properties(id),

  snapshot_date DATE NOT NULL,

  base_price NUMERIC(8,2),
  min_price NUMERIC(8,2),
  max_price NUMERIC(8,2),
  min_stay SMALLINT,
  cleaning_fee NUMERIC(8,2),

  demand_score NUMERIC(5,2),
  occupancy_score NUMERIC(5,2),

  comp_set_data JSONB,
  raw_response JSONB,

  imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE(property_id, snapshot_date)
);
COMMENT ON TABLE pricelabs_snapshots IS 'Snapshot diário do estado PriceLabs por property. Alimenta context_snapshot do pricing_decisions.';

CREATE INDEX idx_pls_date ON pricelabs_snapshots(snapshot_date);
CREATE INDEX idx_pls_property_date ON pricelabs_snapshots(property_id, snapshot_date DESC);
