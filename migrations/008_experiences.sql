-- ============================================================================
-- Migration 008: Experiences and Activity Bookings
-- ============================================================================

CREATE TABLE experiences (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  code TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  category TEXT NOT NULL CHECK (category IN (
    'ADVENTURE', 'GASTRONOMY', 'WELLNESS', 'TRANSPORT', 'EVENTS', 'CULTURE', 'OTHER'
  )),

  default_price NUMERIC(8,2),
  default_supplier_cost NUMERIC(8,2),

  active BOOLEAN NOT NULL DEFAULT TRUE,
  notes TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE experiences IS 'Catálogo de atividades e serviços extra. Passeios, eventos, private chef, etc.';

CREATE INDEX idx_experiences_active ON experiences(active);
CREATE INDEX idx_experiences_category ON experiences(category);

-- Seed common experiences from Doc Único
INSERT INTO experiences (code, name, category) VALUES
  ('HORSE_RIDING', 'Passeios a cavalo', 'ADVENTURE'),
  ('CANYONING', 'Canyoning', 'ADVENTURE'),
  ('CAN_AM_TOUR', 'Can-AM Tour', 'ADVENTURE'),
  ('SAILING', 'Passeio de Vela', 'ADVENTURE'),
  ('BIKE_TOUR', 'Passeio de Bicicleta', 'ADVENTURE'),
  ('HIKING_DIVING_DINNER', 'Caminhada, Mergulho e Jantar', 'ADVENTURE'),
  ('PRIVATE_CHEF', 'Private Chef', 'GASTRONOMY'),
  ('TRANSFER', 'Transfer', 'TRANSPORT'),
  ('MASSAGE', 'Massagem', 'WELLNESS'),
  ('YOGA', 'Yoga', 'WELLNESS'),
  ('GROCERY_SHOPPING', 'Serviço de compras', 'OTHER'),
  ('EVENT', 'Evento (genérico)', 'EVENTS'),
  ('TURISTIC_TOUR', 'Passeios Turísticos', 'CULTURE');

-- ============================================================================
-- EXPERIENCE_BOOKINGS
-- ============================================================================

CREATE TABLE experience_bookings (
  id BIGSERIAL PRIMARY KEY,
  entity_id UUID NOT NULL REFERENCES entities(id),
  experience_id UUID NOT NULL REFERENCES experiences(id),
  reservation_id UUID REFERENCES reservations(id),  -- linkage à estadia se aplicável

  booking_date DATE NOT NULL,
  service_date DATE,
  pax SMALLINT,
  channel booking_channel,

  total_value NUMERIC(8,2),
  supplier_cost NUMERIC(8,2),
  pa_revenue NUMERIC(8,2),

  guest_name TEXT,
  status TEXT,
  notes TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE experience_bookings IS 'Reservas de atividades. Pode estar ligada a uma reservation (guest da casa) ou ser standalone.';

CREATE INDEX idx_eb_experience ON experience_bookings(experience_id);
CREATE INDEX idx_eb_reservation ON experience_bookings(reservation_id) WHERE reservation_id IS NOT NULL;
CREATE INDEX idx_eb_service_date ON experience_bookings(service_date);
CREATE INDEX idx_eb_booking_date ON experience_bookings(booking_date);
