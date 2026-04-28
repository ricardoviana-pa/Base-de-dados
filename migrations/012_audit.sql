-- ============================================================================
-- Migration 012: Audit Log + Triggers
-- ============================================================================

CREATE TABLE audit_log (
  id BIGSERIAL PRIMARY KEY,
  table_name TEXT NOT NULL,
  record_id TEXT NOT NULL,
  operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D')),  -- Insert, Update, Delete

  changed_by TEXT,
  changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  old_values JSONB,
  new_values JSONB,
  changed_fields TEXT[],

  client_ip TEXT,
  user_agent TEXT,
  request_id TEXT
);
COMMENT ON TABLE audit_log IS 'Audit trail automático para tabelas críticas. Populado por triggers.';

CREATE INDEX idx_audit_record ON audit_log(table_name, record_id, changed_at);
CREATE INDEX idx_audit_changed_at ON audit_log(changed_at);
CREATE INDEX idx_audit_changed_by ON audit_log(changed_by);

-- ============================================================================
-- AUDIT TRIGGER FUNCTION
-- ============================================================================

CREATE OR REPLACE FUNCTION audit_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
  old_data JSONB;
  new_data JSONB;
  changed_fields TEXT[];
BEGIN
  IF TG_OP = 'DELETE' THEN
    old_data := to_jsonb(OLD);
    INSERT INTO audit_log (table_name, record_id, operation, old_values, changed_by)
    VALUES (TG_TABLE_NAME, OLD.id::text, 'D', old_data, current_setting('app.current_user', TRUE));
    RETURN OLD;
  ELSIF TG_OP = 'UPDATE' THEN
    old_data := to_jsonb(OLD);
    new_data := to_jsonb(NEW);
    SELECT array_agg(key) INTO changed_fields
    FROM jsonb_each(new_data)
    WHERE new_data -> key IS DISTINCT FROM old_data -> key;

    IF changed_fields IS NOT NULL AND array_length(changed_fields, 1) > 0 THEN
      INSERT INTO audit_log (table_name, record_id, operation, old_values, new_values, changed_fields, changed_by)
      VALUES (TG_TABLE_NAME, NEW.id::text, 'U', old_data, new_data, changed_fields, current_setting('app.current_user', TRUE));
    END IF;
    RETURN NEW;
  ELSIF TG_OP = 'INSERT' THEN
    new_data := to_jsonb(NEW);
    INSERT INTO audit_log (table_name, record_id, operation, new_values, changed_by)
    VALUES (TG_TABLE_NAME, NEW.id::text, 'I', new_data, current_setting('app.current_user', TRUE));
    RETURN NEW;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- ATTACH TRIGGERS TO CRITICAL TABLES
-- ============================================================================

CREATE TRIGGER trg_audit_owner_contracts
  AFTER INSERT OR UPDATE OR DELETE ON owner_contracts
  FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();

CREATE TRIGGER trg_audit_properties
  AFTER INSERT OR UPDATE OR DELETE ON properties
  FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();

CREATE TRIGGER trg_audit_owners
  AFTER INSERT OR UPDATE OR DELETE ON owners
  FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();

CREATE TRIGGER trg_audit_pricing_decisions
  AFTER INSERT OR UPDATE OR DELETE ON pricing_decisions
  FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();

CREATE TRIGGER trg_audit_budgets
  AFTER INSERT OR UPDATE OR DELETE ON budgets
  FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();

CREATE TRIGGER trg_audit_monthly_pnl
  AFTER INSERT OR UPDATE OR DELETE ON monthly_pnl
  FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();

-- ============================================================================
-- updated_at AUTO-UPDATE TRIGGER
-- ============================================================================

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_updated_at_entities BEFORE UPDATE ON entities FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at_owners BEFORE UPDATE ON owners FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at_properties BEFORE UPDATE ON properties FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at_owner_contracts BEFORE UPDATE ON owner_contracts FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at_experiences BEFORE UPDATE ON experiences FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at_budgets BEFORE UPDATE ON budgets FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at_pipeline BEFORE UPDATE ON properties_pipeline FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at_cost_centers BEFORE UPDATE ON cost_centers FOR EACH ROW EXECUTE FUNCTION set_updated_at();
