-- ============================================================================
-- Migration 004: Reservations (event-sourced)
-- ============================================================================
-- Three tables: reservations (header) + reservation_states (append-only) + reservation_events (log)

-- ============================================================================
-- GUESTS (catálogo de hóspedes)
-- ============================================================================

CREATE TABLE guests (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  email_normalized CITEXT,
  name TEXT,
  phone TEXT,
  country_code CHAR(2),
  city TEXT,
  date_of_birth DATE,

  -- Computed (atualizados por trigger ao criar reserva)
  first_booking_at TIMESTAMPTZ,
  last_booking_at TIMESTAMPTZ,
  total_bookings INT NOT NULL DEFAULT 0,
  total_revenue NUMERIC(10,2) NOT NULL DEFAULT 0,
  total_nights INT NOT NULL DEFAULT 0,
  is_vip BOOLEAN NOT NULL DEFAULT FALSE,

  marketing_consent BOOLEAN NOT NULL DEFAULT FALSE,
  notes TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE guests IS 'Hóspedes únicos. Identidade por email normalizado.';

CREATE UNIQUE INDEX idx_guests_email ON guests(email_normalized) WHERE email_normalized IS NOT NULL;

-- ============================================================================
-- RESERVATIONS (header — imutável)
-- ============================================================================

CREATE TABLE reservations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id UUID NOT NULL REFERENCES entities(id),

  source_system TEXT NOT NULL,  -- 'doc_unico', 'rental_ready', 'guesty'
  source_id TEXT NOT NULL,

  property_id UUID NOT NULL REFERENCES properties(id),
  guest_id UUID REFERENCES guests(id),
  channel booking_channel NOT NULL,

  booked_at TIMESTAMPTZ NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE(source_system, source_id)
);
COMMENT ON TABLE reservations IS 'Header imutável da reserva. Estado mutável vive em reservation_states.';

CREATE INDEX idx_reservations_entity ON reservations(entity_id);
CREATE INDEX idx_reservations_property ON reservations(property_id);
CREATE INDEX idx_reservations_guest ON reservations(guest_id);
CREATE INDEX idx_reservations_booked ON reservations(booked_at);
CREATE INDEX idx_reservations_channel ON reservations(channel);

-- ============================================================================
-- RESERVATION_STATES (append-only state history)
-- ============================================================================

CREATE TABLE reservation_states (
  id BIGSERIAL PRIMARY KEY,
  reservation_id UUID NOT NULL REFERENCES reservations(id),

  status reservation_status NOT NULL,

  -- Datas
  checkin_date DATE NOT NULL,
  checkout_date DATE NOT NULL,
  nights SMALLINT GENERATED ALWAYS AS (checkout_date - checkin_date) STORED,

  -- Hóspedes
  adults SMALLINT,
  children SMALLINT,
  babies SMALLINT,
  pax SMALLINT GENERATED ALWAYS AS (COALESCE(adults,0) + COALESCE(children,0)) STORED,

  -- Financeiro detalhado
  gross_total NUMERIC(10,2) NOT NULL,
  vat_stay NUMERIC(10,2) NOT NULL DEFAULT 0,
  vat_cleaning NUMERIC(10,2) NOT NULL DEFAULT 0,
  cleaning_fee_gross NUMERIC(10,2) NOT NULL DEFAULT 0,
  cleaning_fee_net NUMERIC(10,2) NOT NULL DEFAULT 0,
  channel_commission NUMERIC(10,2) NOT NULL DEFAULT 0,
  channel_commission_pct NUMERIC(5,4),

  -- Computed financial breakdown
  net_stay NUMERIC(10,2),
  liquido_split NUMERIC(10,2),
  pa_revenue_gross NUMERIC(10,2),
  owner_share NUMERIC(10,2),

  -- Snapshot do contrato em vigor (denormalizado intencionalmente)
  pa_commission_rate NUMERIC(5,4) NOT NULL,

  -- Vida útil deste estado
  effective_from TIMESTAMPTZ NOT NULL,
  effective_to TIMESTAMPTZ,  -- NULL = current state

  -- Source
  source_system TEXT NOT NULL,
  raw_payload JSONB,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE reservation_states IS 'Estado da reserva em cada momento. Append-only. Linha com effective_to=NULL é o estado atual.';

CREATE INDEX idx_states_reservation ON reservation_states(reservation_id);
CREATE INDEX idx_states_current ON reservation_states(reservation_id) WHERE effective_to IS NULL;
CREATE INDEX idx_states_checkin_current ON reservation_states(checkin_date) WHERE effective_to IS NULL;
CREATE INDEX idx_states_checkout_current ON reservation_states(checkout_date) WHERE effective_to IS NULL;
CREATE INDEX idx_states_status_current ON reservation_states(status) WHERE effective_to IS NULL;

-- Constraint: only one current state per reservation
CREATE UNIQUE INDEX idx_states_one_current ON reservation_states(reservation_id) WHERE effective_to IS NULL;

-- ============================================================================
-- RESERVATION_EVENTS (event log)
-- ============================================================================

CREATE TABLE reservation_events (
  id BIGSERIAL PRIMARY KEY,
  reservation_id UUID NOT NULL REFERENCES reservations(id),

  event_type reservation_event_type NOT NULL,
  event_at TIMESTAMPTZ NOT NULL,

  details JSONB,  -- contexto: {old_price, new_price, reason, ...}

  source_system TEXT,
  triggered_by TEXT,  -- 'SYSTEM_AUTO', 'USER:ricardo', 'PMS_WEBHOOK', 'AI_RM'

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE reservation_events IS 'Log de eventos da reserva. Permite análise temporal de pace, modificações, cancelamentos.';

CREATE INDEX idx_events_reservation ON reservation_events(reservation_id, event_at);
CREATE INDEX idx_events_type ON reservation_events(event_type, event_at);
CREATE INDEX idx_events_at ON reservation_events(event_at);

-- ============================================================================
-- CHANNELS (catálogo)
-- ============================================================================

CREATE TABLE channels (
  code booking_channel PRIMARY KEY,
  display_name TEXT NOT NULL,
  typical_commission_pct NUMERIC(5,4),
  notes TEXT
);

INSERT INTO channels (code, display_name, typical_commission_pct, notes) VALUES
  ('AIRBNB', 'Airbnb', 0.15, 'Host fee 3%; service fee charged to guest'),
  ('BOOKING', 'Booking.com', 0.15, 'Standard 15%; varies by visibility'),
  ('DIRECT', 'Direto (website)', 0.00, 'Sem comissão de plataforma'),
  ('VRBO', 'VRBO', 0.08, 'VRBO commission'),
  ('HOMEAWAY', 'HomeAway', 0.08, 'Legacy — agora VRBO'),
  ('OLIVERS_TRAVEL', 'Olivers Travel', 0.20, 'Comissão alta, segmento luxury'),
  ('HOLIDU', 'Holidu', 0.15, NULL),
  ('GETYOURGUIDE', 'GetYourGuide', 0.20, 'Apenas atividades, raramente alojamento'),
  ('MANUAL', 'Reserva manual', 0.00, 'Inserida manualmente'),
  ('OTHER', 'Outro', NULL, NULL);
