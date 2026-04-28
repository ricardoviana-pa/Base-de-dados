-- ============================================================================
-- Migration 002: Entities + Cost Centers
-- ============================================================================

CREATE TABLE entities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  legal_name TEXT NOT NULL,
  trading_name TEXT,
  vat_number TEXT UNIQUE,
  primavera_company_code TEXT,
  base_currency CHAR(3) NOT NULL DEFAULT 'EUR',
  fiscal_year_start_month SMALLINT NOT NULL DEFAULT 1,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE entities IS 'Entidades legais. Hoje: Ricardo Torres Viana Lda. Futuras: SPVs Algarve, holdings.';

INSERT INTO entities (legal_name, trading_name, primavera_company_code)
VALUES ('Ricardo Torres Viana Unipessoal Lda', 'Portugal Active', 'RTV');

-- ============================================================================
-- COST CENTERS (mapping com Primavera, dual-code)
-- ============================================================================

CREATE TABLE cost_centers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id UUID NOT NULL REFERENCES entities(id),

  -- Two-code system reflecting actual Primavera setup
  code_short TEXT NOT NULL,        -- '0001', '0002', ... (Listagem da Ops)
  code_primavera TEXT,              -- '010182717' (analítica completa, quando existe)

  description TEXT NOT NULL,
  cc_type TEXT NOT NULL CHECK (cc_type IN ('STRUCTURE', 'PROPERTY', 'COWORK', 'PARK', 'OTHER')),
  fiscal_year SMALLINT NOT NULL,
  primavera_type CHAR(1) NOT NULL DEFAULT 'M',  -- M = Movimento

  active BOOLEAN NOT NULL DEFAULT TRUE,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE(entity_id, code_short, fiscal_year)
);
COMMENT ON TABLE cost_centers IS 'Centros de custo. Sincronizar com EB Consultores mensalmente. Dual-code: short (4 digits) + primavera (9 digits).';

CREATE INDEX idx_cc_entity ON cost_centers(entity_id);
CREATE INDEX idx_cc_type ON cost_centers(cc_type) WHERE active = TRUE;
CREATE INDEX idx_cc_primavera ON cost_centers(code_primavera) WHERE code_primavera IS NOT NULL;
