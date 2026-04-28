-- ============================================================================
-- Migration 014: Guesty API Sync — Supporting tables + extensions
-- ============================================================================
-- Sprint 4: Live sync Guesty via Edge Function (cron diário)

-- ============================================================================
-- EXTENSIONS (pg_cron + pg_net já incluídas no Supabase Pro)
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pg_cron WITH SCHEMA pg_catalog;
CREATE EXTENSION IF NOT EXISTS pg_net WITH SCHEMA extensions;

-- ============================================================================
-- GUESTY LISTING MAP (listing_id Guesty → property_id Supabase)
-- ============================================================================

CREATE TABLE IF NOT EXISTS guesty_listing_map (
  guesty_listing_id TEXT PRIMARY KEY,
  property_id UUID NOT NULL REFERENCES properties(id),
  guesty_title TEXT,
  guesty_active BOOLEAN NOT NULL DEFAULT TRUE,
  synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE guesty_listing_map IS 'Mapeamento entre listing IDs do Guesty e properties no Supabase. Populada pelo sync.';

CREATE INDEX IF NOT EXISTS idx_glm_property ON guesty_listing_map(property_id);

-- ============================================================================
-- SYNC LOG (observabilidade de cada execução)
-- ============================================================================

CREATE TABLE IF NOT EXISTS sync_log (
  id BIGSERIAL PRIMARY KEY,
  source_system TEXT NOT NULL DEFAULT 'guesty',
  sync_type TEXT NOT NULL,  -- 'incremental', 'full_backfill', 'listings_only'
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'running',  -- 'running', 'success', 'partial_failure', 'error'

  -- Contagens
  reservations_fetched INT NOT NULL DEFAULT 0,
  reservations_upserted INT NOT NULL DEFAULT 0,
  states_created INT NOT NULL DEFAULT 0,
  cancellations_detected INT NOT NULL DEFAULT 0,
  listings_synced INT NOT NULL DEFAULT 0,
  skipped_unmapped INT NOT NULL DEFAULT 0,

  -- Erro
  error_message TEXT,
  metadata JSONB,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE sync_log IS 'Log de execuções do sync Guesty. Uma linha por execução.';

CREATE INDEX IF NOT EXISTS idx_sync_log_source ON sync_log(source_system, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sync_log_status ON sync_log(status) WHERE status != 'success';

-- ============================================================================
-- CHANNEL ENUM UPDATE (adicionar canais que podem vir do Guesty)
-- ============================================================================
-- Verificar se 'EXPEDIA' já existe antes de adicionar
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_enum WHERE enumlabel = 'EXPEDIA' AND enumtypid = 'booking_channel'::regtype) THEN
    ALTER TYPE booking_channel ADD VALUE IF NOT EXISTS 'EXPEDIA';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_enum WHERE enumlabel = 'TRIPADVISOR' AND enumtypid = 'booking_channel'::regtype) THEN
    ALTER TYPE booking_channel ADD VALUE IF NOT EXISTS 'TRIPADVISOR';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_enum WHERE enumlabel = 'GOOGLE' AND enumtypid = 'booking_channel'::regtype) THEN
    ALTER TYPE booking_channel ADD VALUE IF NOT EXISTS 'GOOGLE';
  END IF;
END $$;
