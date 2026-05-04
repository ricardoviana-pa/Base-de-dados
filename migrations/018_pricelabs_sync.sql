-- ============================================================================
-- Migration 018: PriceLabs sync tables
-- ============================================================================
-- Adds:
--   pricelabs_listing_map     1 row per PriceLabs listing, optional FK to property
--   pricelabs_daily_prices    forward-365d daily price curve per listing
-- pricelabs_snapshots from migration 007 stays as the header/snapshot table.

CREATE TABLE IF NOT EXISTS pricelabs_listing_map (
  pricelabs_id  TEXT PRIMARY KEY,           -- e.g. '696532eee44e1a0015bb2460'
  property_id   UUID REFERENCES properties(id),
  listing_name  TEXT NOT NULL,
  pms           TEXT,                        -- 'guesty' | 'rentalready' | 'avantio'
  group_name    TEXT,
  city_name     TEXT,
  bedrooms      SMALLINT,
  base_price    NUMERIC(8,2),
  min_price     NUMERIC(8,2),
  max_price     NUMERIC(8,2),
  push_enabled  BOOLEAN,
  is_hidden     BOOLEAN,
  last_pushed_at TIMESTAMPTZ,
  raw_response  JSONB,
  imported_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE pricelabs_listing_map IS 'PriceLabs listing → DB property cross-ref. Sprint 5 PriceLabs sync.';

CREATE INDEX IF NOT EXISTS idx_plm_property ON pricelabs_listing_map(property_id) WHERE property_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_plm_push ON pricelabs_listing_map(push_enabled) WHERE push_enabled;

CREATE TABLE IF NOT EXISTS pricelabs_daily_prices (
  pricelabs_id   TEXT NOT NULL,
  property_id    UUID REFERENCES properties(id),
  target_date    DATE NOT NULL,
  snapshot_date  DATE NOT NULL DEFAULT CURRENT_DATE,

  price                NUMERIC(8,2),         -- PriceLabs recommended (after rules)
  user_price           NUMERIC(8,2),         -- manual override (-1 / NULL = none)
  uncustomized_price   NUMERIC(8,2),         -- algorithm baseline (no user rules)
  min_stay             SMALLINT,
  booking_status       TEXT,                 -- 'Available' | 'Booked' | 'Blocked' | …
  booking_status_stly  TEXT,                 -- same-time last year
  adr                  NUMERIC(8,2),         -- realised market ADR for this date
  adr_stly             NUMERIC(8,2),
  booked_date          DATE,                 -- when current booking landed
  booked_date_stly     DATE,
  demand_color         TEXT,
  demand_desc          TEXT,                 -- 'Unavailable' | 'Low' | 'Med' | 'High' | …

  raw           JSONB,
  PRIMARY KEY (pricelabs_id, target_date, snapshot_date)
);
COMMENT ON TABLE pricelabs_daily_prices IS 'Forward 365-day price curve per listing. New snapshot_date row each pull = full history.';

CREATE INDEX IF NOT EXISTS idx_plp_property_date ON pricelabs_daily_prices(property_id, target_date) WHERE property_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_plp_target ON pricelabs_daily_prices(target_date);
CREATE INDEX IF NOT EXISTS idx_plp_snapshot ON pricelabs_daily_prices(snapshot_date);
