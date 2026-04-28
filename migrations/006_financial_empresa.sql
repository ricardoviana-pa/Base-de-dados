-- ============================================================================
-- Migration 006: Financeiro Empresa (Primavera, monthly_pnl, company_costs)
-- ============================================================================

-- ============================================================================
-- PRIMAVERA_ACCOUNTS (plano de contas)
-- ============================================================================

CREATE TABLE primavera_accounts (
  account_code TEXT PRIMARY KEY,
  account_name TEXT NOT NULL,
  parent_code TEXT REFERENCES primavera_accounts(account_code),

  account_class SMALLINT,  -- 1-8 (PT chart of accounts)
  account_type TEXT CHECK (account_type IN ('ASSET', 'LIABILITY', 'EQUITY', 'REVENUE', 'EXPENSE', 'OTHER')),

  -- Mapeamento para categorias de gestão (preenchido manualmente)
  management_category TEXT,
  is_pa_cost BOOLEAN,

  active BOOLEAN NOT NULL DEFAULT TRUE,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE primavera_accounts IS 'Plano de contas Primavera, com mapeamento para categorias de gestão. Importado da EB Consultores.';

CREATE INDEX idx_pa_class ON primavera_accounts(account_class);
CREATE INDEX idx_pa_category ON primavera_accounts(management_category);

-- ============================================================================
-- MONTHLY_PNL (snapshot agregado mensal)
-- ============================================================================

CREATE TABLE monthly_pnl (
  id BIGSERIAL PRIMARY KEY,
  entity_id UUID NOT NULL REFERENCES entities(id),

  year SMALLINT NOT NULL,
  month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),

  account_code TEXT NOT NULL REFERENCES primavera_accounts(account_code),
  cost_center_id UUID REFERENCES cost_centers(id),

  movement_debit NUMERIC(12,2) NOT NULL DEFAULT 0,
  movement_credit NUMERIC(12,2) NOT NULL DEFAULT 0,
  balance NUMERIC(12,2),

  source TEXT NOT NULL DEFAULT 'eb_consultores',
  imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE(entity_id, year, month, account_code, cost_center_id)
);
COMMENT ON TABLE monthly_pnl IS 'P&L mensal agregado por conta. Importado mensalmente da EB Consultores.';

CREATE INDEX idx_pnl_period ON monthly_pnl(year, month);
CREATE INDEX idx_pnl_account ON monthly_pnl(account_code);
CREATE INDEX idx_pnl_cc ON monthly_pnl(cost_center_id) WHERE cost_center_id IS NOT NULL;

-- ============================================================================
-- COMPANY_COSTS (lançamentos individuais — para análise de detalhe)
-- ============================================================================

CREATE TABLE company_costs (
  id BIGSERIAL PRIMARY KEY,
  entity_id UUID NOT NULL REFERENCES entities(id),

  cost_date DATE NOT NULL,
  category TEXT NOT NULL,  -- 'HR_SALARIES', 'OFFICE_RENT', 'SOFTWARE', 'LEGAL', 'INSURANCE', etc.
  subcategory TEXT,

  supplier_name TEXT,
  description TEXT,

  amount_gross NUMERIC(12,2) NOT NULL,
  amount_net NUMERIC(12,2),
  vat_amount NUMERIC(10,2),

  account_code TEXT REFERENCES primavera_accounts(account_code),
  cost_center_id UUID REFERENCES cost_centers(id),
  invoice_ref TEXT,

  notes TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE company_costs IS 'Custos da empresa não atribuíveis a uma property (HR, escritório, software, etc.). Importado da EB Consultores.';

CREATE INDEX idx_cc_period ON company_costs(cost_date);
CREATE INDEX idx_cc_category ON company_costs(category);
CREATE INDEX idx_cc_account ON company_costs(account_code);
CREATE INDEX idx_cc_costcenter ON company_costs(cost_center_id) WHERE cost_center_id IS NOT NULL;

-- ============================================================================
-- SEED: Categorias de gestão típicas (para mapping de account_codes)
-- ============================================================================
-- Não é tabela mas referência: management_category values esperados são
-- 'OWNER_SHARE', 'CHANNEL_FEES', 'CLEANING_LABOR', 'CLEANING_PRODUCTS', 'LAUNDRY',
-- 'POOL_GARDEN_MAINT', 'HR_SALARIES', 'HR_BENEFITS', 'OFFICE_RENT', 'OFFICE_UTILITIES',
-- 'SOFTWARE', 'MARKETING', 'LEGAL', 'ACCOUNTING', 'INSURANCE', 'TRAVEL',
-- 'BANK_FEES', 'INTEREST', 'TAXES', 'DEPRECIATION', 'OTHER'
