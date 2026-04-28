-- ============================================================================
-- Migration 009: Properties Pipeline (Pipedrive integration)
-- ============================================================================
-- Schema preparado para receber export do Pipedrive. Campos genéricos hoje;
-- afinaremos quando recebermos o export real do Cowork.

-- ============================================================================
-- PIPELINE_STAGES (catálogo)
-- ============================================================================

CREATE TABLE pipeline_stages (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  pipedrive_stage_id TEXT UNIQUE,

  name TEXT NOT NULL,
  display_order SMALLINT,
  pipeline_name TEXT,  -- if Pipedrive has multiple pipelines

  is_won_stage BOOLEAN NOT NULL DEFAULT FALSE,
  is_lost_stage BOOLEAN NOT NULL DEFAULT FALSE,

  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ps_active ON pipeline_stages(active);

-- ============================================================================
-- LEAD_SOURCES (catálogo)
-- ============================================================================

CREATE TABLE lead_sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  code TEXT UNIQUE NOT NULL,
  display_name TEXT NOT NULL,
  category TEXT,  -- 'INBOUND', 'OUTBOUND', 'REFERRAL', 'PARTNERSHIP'
  active BOOLEAN NOT NULL DEFAULT TRUE
);

-- ============================================================================
-- LOSS_REASONS (catálogo)
-- ============================================================================

CREATE TABLE loss_reasons (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  code TEXT UNIQUE NOT NULL,
  display_name TEXT NOT NULL,
  category TEXT,  -- 'PRICE', 'COMPETITION', 'TIMING', 'FIT', 'OTHER'
  active BOOLEAN NOT NULL DEFAULT TRUE
);

-- ============================================================================
-- PROPERTIES_PIPELINE
-- ============================================================================

CREATE TABLE properties_pipeline (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id UUID NOT NULL REFERENCES entities(id),

  -- Pipedrive integration
  pipedrive_deal_id TEXT UNIQUE,

  -- Identificação da deal
  title TEXT NOT NULL,
  property_address TEXT,
  property_type TEXT,
  estimated_tipologia TEXT,

  -- Owner contact info
  contact_name TEXT,
  contact_email CITEXT,
  contact_phone TEXT,

  -- Pipeline state
  current_stage_id UUID REFERENCES pipeline_stages(id),
  status pipeline_deal_status NOT NULL DEFAULT 'OPEN',

  -- Lead origin
  lead_source_id UUID REFERENCES lead_sources(id),
  lead_source_other TEXT,  -- free-text se source não é catalogado

  -- Loss tracking
  loss_reason_id UUID REFERENCES loss_reasons(id),
  loss_reason_other TEXT,
  lost_at TIMESTAMPTZ,

  -- Win tracking
  won_at TIMESTAMPTZ,
  property_id UUID REFERENCES properties(id),  -- preenchido quando deal é ganha e property criada

  -- Estimated value (€/year)
  expected_annual_revenue NUMERIC(10,2),
  expected_pa_revenue NUMERIC(10,2),

  -- Dates
  pipedrive_created_at TIMESTAMPTZ,
  pipedrive_updated_at TIMESTAMPTZ,
  expected_close_date DATE,

  raw_pipedrive_data JSONB,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE properties_pipeline IS 'Pipeline de aquisição de propriedades. Sync regular com Pipedrive.';

CREATE INDEX idx_pp_entity ON properties_pipeline(entity_id);
CREATE INDEX idx_pp_status ON properties_pipeline(status);
CREATE INDEX idx_pp_stage ON properties_pipeline(current_stage_id);
CREATE INDEX idx_pp_property ON properties_pipeline(property_id) WHERE property_id IS NOT NULL;

-- ============================================================================
-- PIPELINE_ACTIVITIES (histórico de atividades)
-- ============================================================================

CREATE TABLE pipeline_activities (
  id BIGSERIAL PRIMARY KEY,
  deal_id UUID NOT NULL REFERENCES properties_pipeline(id),

  pipedrive_activity_id TEXT,
  activity_type TEXT,  -- 'CALL', 'MEETING', 'EMAIL', 'TASK', 'NOTE'
  subject TEXT,
  notes TEXT,
  due_date DATE,
  done BOOLEAN NOT NULL DEFAULT FALSE,
  done_at TIMESTAMPTZ,

  assigned_to TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pa_deal ON pipeline_activities(deal_id);
CREATE INDEX idx_pa_due ON pipeline_activities(due_date) WHERE done = FALSE;
