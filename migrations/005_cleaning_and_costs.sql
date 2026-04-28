-- ============================================================================
-- Migration 005: Cleaning Catalog + Direct Costs (Cleanings, Laundry, Property Expenses)
-- ============================================================================

-- ============================================================================
-- CLEANING_SERVICE_CATALOG (matriz da Ops Manager)
-- ============================================================================
-- Reflects PA_Ops_Costs_final.xlsx — matrix of (tier × tipologia × service_type)

CREATE TABLE cleaning_service_catalog (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  tier property_tier NOT NULL,
  tipologia TEXT NOT NULL,  -- 'T0', 'T1 - 1 quarto', 'T2 - 2 quartos c/ varanda', etc.
  service_type cleaning_service_type NOT NULL,

  -- Specs operacionais
  hours NUMERIC(4,2) NOT NULL,
  staff_count SMALLINT NOT NULL,
  has_laundry BOOLEAN NOT NULL DEFAULT FALSE,

  -- Custos
  labor_cost NUMERIC(8,2) NOT NULL,
  cleaning_products_cost NUMERIC(8,2) NOT NULL,
  transport_cost NUMERIC(8,2) NOT NULL,

  -- Total computed
  cost_net NUMERIC(8,2) NOT NULL,         -- s/ IVA
  cost_with_vat_6 NUMERIC(8,2) NOT NULL,  -- c/ IVA 6%
  cost_with_vat_23 NUMERIC(8,2) NOT NULL, -- c/ IVA 23%

  -- Versioning
  effective_from DATE NOT NULL,
  effective_to DATE,  -- NULL = current

  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE(tier, tipologia, service_type, effective_from)
);
COMMENT ON TABLE cleaning_service_catalog IS 'Matriz de custos de limpeza mantida pela Ops Manager. Versionada para captar mudanças de pricing operacional.';

CREATE INDEX idx_cc_tier_tipologia ON cleaning_service_catalog(tier, tipologia);
CREATE INDEX idx_cc_current ON cleaning_service_catalog(tier, tipologia, service_type) WHERE effective_to IS NULL;

-- ============================================================================
-- CONSUMABLES_BASELINE
-- ============================================================================

CREATE TABLE consumables_baseline (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tipologia TEXT NOT NULL,
  bathrooms SMALLINT NOT NULL,

  -- Custo total estimado por reserva
  cost_per_booking_net NUMERIC(8,2) NOT NULL,

  -- Detail breakdown (JSONB for flexibility)
  items JSONB,  -- [{name: 'Guardanapos', cost: 0.648, supplier: 'LusoHigin'}, ...]

  effective_from DATE NOT NULL,
  effective_to DATE,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE consumables_baseline IS 'Custos de consumíveis por tipologia (papel, café, gel, etc). Da Ops Manager.';

CREATE INDEX idx_consumables_current ON consumables_baseline(tipologia) WHERE effective_to IS NULL;

-- ============================================================================
-- LAUNDRY_BASELINE
-- ============================================================================

CREATE TABLE laundry_baseline (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tipologia TEXT NOT NULL,
  bedrooms SMALLINT NOT NULL,

  kg_roupa NUMERIC(5,2) NOT NULL,
  cost_net NUMERIC(8,2) NOT NULL,
  cost_with_vat_6 NUMERIC(8,2) NOT NULL,
  cost_with_vat_23 NUMERIC(8,2) NOT NULL,

  effective_from DATE NOT NULL,
  effective_to DATE,

  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_laundry_current ON laundry_baseline(tipologia) WHERE effective_to IS NULL;

-- ============================================================================
-- CLEANINGS (instâncias reais)
-- ============================================================================

CREATE TABLE cleanings (
  id BIGSERIAL PRIMARY KEY,
  property_id UUID NOT NULL REFERENCES properties(id),
  reservation_id UUID REFERENCES reservations(id),

  service_type cleaning_service_type NOT NULL,
  catalog_id UUID REFERENCES cleaning_service_catalog(id),  -- linha do catálogo aplicada

  cleaning_date DATE NOT NULL,
  team_name TEXT,
  staff_count SMALLINT,
  hours_spent NUMERIC(5,2),

  -- Custos reais (podem divergir do catálogo)
  cost_net NUMERIC(8,2),
  cost_gross NUMERIC(8,2),
  fuel_cost NUMERIC(6,2),

  invoice_ref TEXT,
  paid_at DATE,
  notes TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE cleanings IS 'Limpezas executadas. catalog_id refere a linha do catálogo aplicada para auditoria.';

CREATE INDEX idx_cleanings_property ON cleanings(property_id, cleaning_date);
CREATE INDEX idx_cleanings_reservation ON cleanings(reservation_id) WHERE reservation_id IS NOT NULL;
CREATE INDEX idx_cleanings_date ON cleanings(cleaning_date);

-- ============================================================================
-- LAUNDRY (instâncias reais)
-- ============================================================================

CREATE TABLE laundry (
  id BIGSERIAL PRIMARY KEY,
  property_id UUID NOT NULL REFERENCES properties(id),
  reservation_id UUID REFERENCES reservations(id),

  service_date DATE NOT NULL,
  unit_price NUMERIC(8,2),
  total_paid NUMERIC(8,2) NOT NULL,

  invoice_ref TEXT,
  paid_at DATE,
  notes TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_laundry_property ON laundry(property_id, service_date);
CREATE INDEX idx_laundry_reservation ON laundry(reservation_id) WHERE reservation_id IS NOT NULL;

-- ============================================================================
-- PROPERTY_EXPENSES (custos não diretamente atribuíveis a reserva)
-- ============================================================================

CREATE TABLE property_expenses (
  id BIGSERIAL PRIMARY KEY,
  property_id UUID NOT NULL REFERENCES properties(id),
  cost_center_id UUID REFERENCES cost_centers(id),

  expense_date DATE NOT NULL,
  category TEXT NOT NULL,  -- 'POOL', 'GARDEN', 'ELECTRICITY', 'WATER', 'INTERNET', 'MAINTENANCE', 'FURNITURE', 'INSURANCE', 'PHOTOGRAPHY', 'EQUIPMENT', 'CLEANING_SUPPLIES', 'RENT', 'PACK_GIFT'
  subcategory TEXT,

  supplier_name TEXT,
  amount_gross NUMERIC(10,2) NOT NULL,
  amount_net NUMERIC(10,2),
  vat_amount NUMERIC(10,2),

  invoice_ref TEXT,
  primavera_account_code TEXT,
  paid_at DATE,
  responsible_user TEXT,
  description TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE property_expenses IS 'Despesas atribuíveis a uma property (não a uma reserva). Ex: pool, garden, electricity, manutenção.';

CREATE INDEX idx_pe_property_date ON property_expenses(property_id, expense_date);
CREATE INDEX idx_pe_category ON property_expenses(category);
CREATE INDEX idx_pe_cc ON property_expenses(cost_center_id) WHERE cost_center_id IS NOT NULL;
