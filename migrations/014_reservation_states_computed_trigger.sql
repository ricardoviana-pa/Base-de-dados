-- ============================================================================
-- Migration 014: Auto-compute reservation_states financial breakdown
-- ============================================================================
-- net_stay, liquido_split, pa_revenue_gross, owner_share are populated by trigger
-- whenever they are not explicitly provided. Keeps the import scripts simple and
-- prevents drift between sources.

CREATE OR REPLACE FUNCTION reservation_states_compute_split()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.gross_total IS NULL THEN
    RETURN NEW;
  END IF;

  IF NEW.net_stay IS NULL THEN
    NEW.net_stay := NEW.gross_total
                    - COALESCE(NEW.vat_stay, 0)
                    - COALESCE(NEW.cleaning_fee_gross, 0);
  END IF;

  IF NEW.liquido_split IS NULL THEN
    NEW.liquido_split := NEW.net_stay - COALESCE(NEW.channel_commission, 0);
  END IF;

  IF NEW.pa_revenue_gross IS NULL THEN
    NEW.pa_revenue_gross := NEW.liquido_split * COALESCE(NEW.pa_commission_rate, 0.40);
  END IF;

  IF NEW.owner_share IS NULL THEN
    NEW.owner_share := NEW.liquido_split * (1 - COALESCE(NEW.pa_commission_rate, 0.40));
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_reservation_states_split ON reservation_states;
CREATE TRIGGER trg_reservation_states_split
  BEFORE INSERT OR UPDATE ON reservation_states
  FOR EACH ROW EXECUTE FUNCTION reservation_states_compute_split();

COMMENT ON FUNCTION reservation_states_compute_split IS
  'Auto-fills computed financial breakdown (net_stay, liquido_split, pa_revenue_gross, owner_share) when not explicitly provided. See migration 014.';
