-- ============================================================================
-- Migration 017: Master reservations cross-reference columns
-- ============================================================================
-- Sprint 1.10 reset: instead of one row per source × reservation, we now have
-- ONE row per physical reservation with cross-reference IDs for each source.
--
-- `source_system` keeps its meaning: the AUTHORITATIVE source for this row
-- (the era-canonical one — RR for 2023-2025, Guesty for 2026+).
--
-- The new columns let us trace a reservation back to every Excel/PMS export
-- that mentioned it, without duplicating rows.

ALTER TABLE reservations
  ADD COLUMN IF NOT EXISTS rr_source_id        TEXT,
  ADD COLUMN IF NOT EXISTS doc_unico_source_id TEXT,
  ADD COLUMN IF NOT EXISTS guesty_source_id    TEXT;

-- Partial unique indexes: each external ID can appear at most once across the
-- table. (NULLs are allowed and unconstrained — that's how partial UNIQUE works.)
CREATE UNIQUE INDEX IF NOT EXISTS uq_reservations_rr_source_id
  ON reservations(rr_source_id) WHERE rr_source_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_reservations_doc_unico_source_id
  ON reservations(doc_unico_source_id) WHERE doc_unico_source_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_reservations_guesty_source_id
  ON reservations(guesty_source_id) WHERE guesty_source_id IS NOT NULL;

-- Lookup indexes for matching during imports
CREATE INDEX IF NOT EXISTS idx_resv_property_checkin
  ON reservations(property_id, (
    -- We can't index a function of joined data here, but property_id alone is
    -- selective enough and matchers add the date filter on the JOIN.
    entity_id
  ));

COMMENT ON COLUMN reservations.rr_source_id IS
  'Rental Ready reservation ID. Set when this row was sourced from or matched against the RR Excel export.';
COMMENT ON COLUMN reservations.doc_unico_source_id IS
  'Doc Único Booking Number. Set when this row was sourced from or matched against the Doc Único Excel.';
COMMENT ON COLUMN reservations.guesty_source_id IS
  'Guesty internal reservation ID (24-char hex hash). Set from Guesty Excel snapshot or live API.';
