-- ============================================================================
-- Migration 010: Budgets (Annual + Monthly per Property)
-- ============================================================================

-- ============================================================================
-- BUDGETS (header — versões do orçamento)
-- ============================================================================

CREATE TABLE budgets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id UUID NOT NULL REFERENCES entities(id),

  fiscal_year SMALLINT NOT NULL,
  version_name TEXT NOT NULL,  -- 'BUD2026_v1', 'BUD2026_FINAL'
  status budget_status NOT NULL DEFAULT 'DRAFT',

  approved_by TEXT,
  approved_at TIMESTAMPTZ,

  notes TEXT,
  source_file TEXT,  -- 'PTAC_BUD26_EBITDA.xlsx'

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE(entity_id, fiscal_year, version_name)
);
COMMENT ON TABLE budgets IS 'Versões do orçamento anual. Permite ter draft + approved em simultâneo.';

CREATE INDEX idx_budgets_year_status ON budgets(fiscal_year, status);

-- ============================================================================
-- BUDGET_LINES_PROPERTY (orçamento por property × mês × métrica)
-- ============================================================================

CREATE TABLE budget_lines_property (
  id BIGSERIAL PRIMARY KEY,
  budget_id UUID NOT NULL REFERENCES budgets(id),

  -- Property pode ser real ou placeholder
  property_id UUID REFERENCES properties(id),
  pipeline_deal_id UUID REFERENCES properties_pipeline(id),
  placeholder_label TEXT,  -- 'CASA NOVA 01', 'CASA NOVA 02' etc. — para budget de properties hipotéticas
  CONSTRAINT property_or_placeholder CHECK (
    property_id IS NOT NULL OR placeholder_label IS NOT NULL
  ),

  -- Metadata da linha
  cost_center_primavera TEXT,  -- '010182717'
  property_label TEXT,  -- 'T5 - Eben Lodge'

  -- Period
  year SMALLINT NOT NULL,
  month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),

  -- Drivers de receita
  occupancy_pct NUMERIC(5,4),
  num_reservations NUMERIC(6,2),
  avg_daily_rate NUMERIC(8,2),

  -- Métricas financeiras (uma linha por métrica seria muito; usamos colunas)
  revenue_amount NUMERIC(10,2),
  owner_share_amount NUMERIC(10,2),
  platform_fees_amount NUMERIC(10,2),
  marketing_amount NUMERIC(10,2),
  cleaning_amount NUMERIC(10,2),
  maintenance_amount NUMERIC(10,2),
  checkin_amount NUMERIC(10,2),
  onboarding_amount NUMERIC(10,2),
  other_amount NUMERIC(10,2),
  margin_amount NUMERIC(10,2),
  margin_pct NUMERIC(5,4),

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE(budget_id, COALESCE(property_id::text, placeholder_label), year, month)
);
COMMENT ON TABLE budget_lines_property IS 'Linhas do budget por property × mês. Suporta placeholders (CASA NOVA 01) para properties que ainda não existem.';

CREATE INDEX idx_blp_budget ON budget_lines_property(budget_id);
CREATE INDEX idx_blp_property ON budget_lines_property(property_id) WHERE property_id IS NOT NULL;
CREATE INDEX idx_blp_period ON budget_lines_property(year, month);
CREATE INDEX idx_blp_placeholder ON budget_lines_property(placeholder_label) WHERE placeholder_label IS NOT NULL;

-- ============================================================================
-- BUDGET_LINES_COMPANY (orçamento de estrutura — não atribuível a property)
-- ============================================================================

CREATE TABLE budget_lines_company (
  id BIGSERIAL PRIMARY KEY,
  budget_id UUID NOT NULL REFERENCES budgets(id),

  year SMALLINT NOT NULL,
  month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),

  category TEXT NOT NULL,  -- 'HR_SALARIES', 'OFFICE_RENT', 'SOFTWARE', 'LEGAL', etc.
  subcategory TEXT,
  amount NUMERIC(12,2) NOT NULL,
  notes TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE budget_lines_company IS 'Linhas do budget para custos de estrutura (HR, escritório, software).';

CREATE INDEX idx_blc_budget ON budget_lines_company(budget_id);
CREATE INDEX idx_blc_period ON budget_lines_company(year, month);
CREATE INDEX idx_blc_category ON budget_lines_company(category);
