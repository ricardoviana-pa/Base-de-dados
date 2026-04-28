-- ============================================================================
-- Migration 001: Extensions and Enums
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "citext";

CREATE TYPE property_status AS ENUM (
  'DRAFT', 'ONBOARDING', 'ACTIVE', 'PAUSED', 'OFFBOARDED'
);

CREATE TYPE property_tier AS ENUM ('STANDARD', 'PREMIUM', 'LUXURY');

CREATE TYPE property_region AS ENUM (
  'ALTO_MINHO', 'PORTO', 'ALGARVE', 'DOURO', 'OTHER'
);

CREATE TYPE booking_channel AS ENUM (
  'AIRBNB', 'BOOKING', 'DIRECT', 'VRBO', 'HOMEAWAY',
  'OLIVERS_TRAVEL', 'HOLIDU', 'GETYOURGUIDE', 'MANUAL', 'OTHER'
);

CREATE TYPE reservation_status AS ENUM (
  'PENDING', 'CONFIRMED', 'CANCELLED', 'COMPLETED', 'NO_SHOW'
);

CREATE TYPE reservation_event_type AS ENUM (
  'BOOKED', 'PRICE_MODIFIED', 'DATES_MODIFIED', 'GUESTS_MODIFIED',
  'CANCELLED', 'CHECKED_IN', 'CHECKED_OUT',
  'PAYMENT_RECEIVED', 'PAYMENT_FAILED', 'NOTE_ADDED'
);

CREATE TYPE cleaning_service_type AS ENUM (
  'CO', 'CO_L', 'OUT_IN_MINUS', 'OUT_IN_PLUS',
  'PERM', 'PERM_TC', 'REFRESH', 'BEDS',
  'OBRA', 'DEEP_CLEAN', 'INSPECTION'
);

CREATE TYPE pricing_decision_type AS ENUM (
  'BASE_PRICE_CHANGE', 'MIN_STAY_CHANGE', 'CLEANING_FEE_CHANGE',
  'GAP_FILL_DISCOUNT', 'EVENT_SURGE',
  'LAST_MINUTE_DISCOUNT', 'WEEKEND_PREMIUM'
);

CREATE TYPE budget_status AS ENUM (
  'DRAFT', 'APPROVED', 'SUPERSEDED', 'ARCHIVED'
);

CREATE TYPE pipeline_deal_status AS ENUM (
  'OPEN', 'WON', 'LOST', 'ARCHIVED'
);
