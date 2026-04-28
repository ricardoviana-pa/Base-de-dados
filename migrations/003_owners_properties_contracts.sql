-- ============================================================================
-- Migration 003: Catalog (Owners, Properties, Tier History, Contracts)
-- ============================================================================

-- ============================================================================
-- OWNERS
-- ============================================================================

CREATE TABLE owners (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id UUID NOT NULL REFERENCES entities(id),

  legal_name TEXT NOT NULL,
  is_company BOOLEAN NOT NULL DEFAULT FALSE,
  vat_number TEXT,
  primavera_account_code TEXT,  -- Ex: '2782101001' from balancete

  contact_email CITEXT,
  contact_phone TEXT,
  iban TEXT,
  preferred_language CHAR(2) NOT NULL DEFAULT 'pt',

  notes TEXT,
  active BOOLEAN NOT NULL DEFAULT TRUE,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE owners IS 'Proprietários das casas. Liga a contas 2782101xxx do Primavera.';

CREATE INDEX idx_owners_entity ON owners(entity_id);
CREATE INDEX idx_owners_primavera ON owners(primavera_account_code) WHERE primavera_account_code IS NOT NULL;
CREATE INDEX idx_owners_email ON owners(contact_email) WHERE contact_email IS NOT NULL;

-- ============================================================================
-- PROPERTIES
-- ============================================================================

CREATE TABLE properties (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id UUID NOT NULL REFERENCES entities(id),

  -- Identidade
  canonical_name TEXT NOT NULL,
  display_name TEXT NOT NULL,
  building TEXT,

  -- IDs externos para reconciliação histórica
  guesty_id TEXT UNIQUE,
  doc_unico_id TEXT,
  avantio_id TEXT,
  rental_ready_id TEXT,
  cost_center_id UUID REFERENCES cost_centers(id),

  -- Localização
  city TEXT,
  region property_region,
  address TEXT,
  postal_code TEXT,
  latitude NUMERIC(10,6),
  longitude NUMERIC(10,6),

  -- Características físicas
  property_type TEXT,
  tipologia TEXT,  -- 'T0', 'T1', 'T2', 'T3', etc. — usado para custo de limpeza
  bedrooms SMALLINT,
  bathrooms SMALLINT,
  max_guests SMALLINT,
  has_pool BOOLEAN NOT NULL DEFAULT FALSE,
  has_heated_pool BOOLEAN NOT NULL DEFAULT FALSE,
  has_garden BOOLEAN NOT NULL DEFAULT FALSE,
  has_external_area BOOLEAN NOT NULL DEFAULT FALSE,

  -- Tier dinâmico (snapshot do estado atual; verdade está em property_tier_history)
  current_tier property_tier,

  -- Owner
  owner_id UUID REFERENCES owners(id),

  -- Status
  status property_status NOT NULL DEFAULT 'DRAFT',
  onboarded_at DATE,
  offboarded_at DATE,

  -- Soft delete
  deleted_at TIMESTAMPTZ,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE properties IS 'Catálogo de propriedades. Multi-source IDs para reconciliar histórico Doc Único + RR + Guesty.';

CREATE INDEX idx_properties_entity ON properties(entity_id);
CREATE INDEX idx_properties_status ON properties(status) WHERE deleted_at IS NULL;
CREATE INDEX idx_properties_region ON properties(region);
CREATE INDEX idx_properties_owner ON properties(owner_id);
CREATE INDEX idx_properties_cost_center ON properties(cost_center_id);
CREATE INDEX idx_properties_guesty ON properties(guesty_id) WHERE guesty_id IS NOT NULL;

-- ============================================================================
-- PROPERTY_TIER_HISTORY
-- ============================================================================

CREATE TABLE property_tier_history (
  id BIGSERIAL PRIMARY KEY,
  property_id UUID NOT NULL REFERENCES properties(id),

  tier property_tier NOT NULL,
  effective_from DATE NOT NULL,
  effective_to DATE,  -- NULL = ainda em vigor

  reason TEXT,  -- 'INITIAL', 'RENOVATION', 'REPOSITIONING'
  changed_by TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE property_tier_history IS 'Tier muda ao longo do tempo. Reservas históricas são avaliadas com tier em vigor à data do checkin.';

CREATE INDEX idx_tier_property_dates ON property_tier_history(property_id, effective_from, effective_to);

-- ============================================================================
-- OWNER_CONTRACTS
-- ============================================================================

CREATE TABLE owner_contracts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id UUID NOT NULL REFERENCES properties(id),
  owner_id UUID NOT NULL REFERENCES owners(id),

  effective_from DATE NOT NULL,
  effective_to DATE,  -- NULL = current

  -- Commission split
  pa_commission_pct NUMERIC(5,4) NOT NULL,
  owner_commission_pct NUMERIC(5,4) NOT NULL,
  CONSTRAINT pct_sums CHECK (
    ABS(pa_commission_pct + owner_commission_pct - 1.0) < 0.0001
  ),

  -- Owner VAT (alguns owners passam fatura à PA, outros não)
  owner_vat_rate NUMERIC(5,4) NOT NULL DEFAULT 0,

  -- Default costs from contract
  laundry_cost_per_booking NUMERIC(8,2),
  consumable_cost_per_booking NUMERIC(8,2),
  cleaning_fee_default NUMERIC(8,2),

  -- Special clauses
  fixed_monthly_fee NUMERIC(10,2),
  notes TEXT,
  contract_pdf_url TEXT,
  signed_at DATE,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE owner_contracts IS 'Contratos com owners. Doc Único mostra que taxas variaram entre 2020-2024. Cada reserva é avaliada usando contrato em vigor à data do checkin.';

CREATE INDEX idx_contracts_property ON owner_contracts(property_id);
CREATE INDEX idx_contracts_dates ON owner_contracts(property_id, effective_from, effective_to);
