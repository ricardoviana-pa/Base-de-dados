// ============================================================================
// Guesty → Supabase Schema Mapper
// ============================================================================

import { STATUS_MAP, CHANNEL_MAP } from "./types.ts";
import type { GuestyReservation } from "./types.ts";

const VAT_RATE = 0.06; // 6% IVA alojamento local Portugal

/** Mapped reservation header (for reservations table) */
export interface MappedReservationHeader {
  source_system: "guesty";
  source_id: string;
  channel: string;
  booked_at: string;
  listing_id: string; // Guesty listing ID for property lookup
  guest_name: string;
  guest_email: string | null;
  confirmation_code: string;
}

/** Mapped reservation state (for reservation_states table) */
export interface MappedReservationState {
  status: string;
  checkin_date: string;
  checkout_date: string;
  adults: number | null;
  children: number | null;
  babies: number | null;
  gross_total: number;
  vat_stay: number;
  vat_cleaning: number;
  cleaning_fee_gross: number;
  cleaning_fee_net: number;
  channel_commission: number;
  channel_commission_pct: number | null;
  net_stay: number | null;
  pa_commission_rate: number;
  source_system: "guesty";
  raw_payload: Record<string, unknown>;
}

export interface MappedReservation {
  header: MappedReservationHeader;
  state: MappedReservationState;
  is_cancelled: boolean;
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function mapStatus(guestyStatus: string): string {
  const mapped = STATUS_MAP[guestyStatus.toLowerCase()];
  if (!mapped) {
    console.warn(`Unknown Guesty status: "${guestyStatus}", defaulting to PENDING`);
    return "PENDING";
  }
  return mapped;
}

function mapChannel(source: string): string {
  if (!source) return "OTHER";
  const mapped = CHANNEL_MAP[source.toLowerCase()];
  if (!mapped) {
    console.warn(`Unknown Guesty source/channel: "${source}", defaulting to OTHER`);
    return "OTHER";
  }
  return mapped;
}

function extractDate(isoString: string): string {
  // "2026-04-28T15:00:00.000Z" → "2026-04-28"
  return isoString.substring(0, 10);
}

function parseCommissionPct(formula: string | undefined): number | null {
  if (!formula) return null;
  // Typical: "net_income*.4" → 0.40
  const match = formula.match(/\*\s*([\d.]+)/);
  if (match) return parseFloat(match[1]);
  return null;
}

function calculateVat(grossAmount: number): number {
  // gross = net * (1 + VAT_RATE) → VAT = gross - gross / (1 + VAT_RATE)
  if (grossAmount <= 0) return 0;
  return Math.round((grossAmount - grossAmount / (1 + VAT_RATE)) * 100) / 100;
}

// ─── Main mapper ────────────────────────────────────────────────────────────

export function mapReservation(r: GuestyReservation): MappedReservation {
  const money = r.money || {} as GuestyReservation["money"];
  const status = mapStatus(r.status);
  const channel = mapChannel(r.source);

  // Financial calculations
  const fareAccom = money.fareAccommodation || 0;
  const fareCleaning = money.fareCleaning || 0;
  const grossTotal = fareAccom + fareCleaning;
  const vatStay = calculateVat(fareAccom);
  const vatCleaning = calculateVat(fareCleaning);
  const cleaningNet = Math.round((fareCleaning / (1 + VAT_RATE)) * 100) / 100;
  const channelComm = money.hostServiceFee || money.commission || 0;
  const commPct = parseCommissionPct(money.commissionFormula);

  // Net stay: accommodation net of VAT minus channel commission
  const netStay = money.hostPayout ?? null;

  // PA commission rate: from the commission formula or default
  // commission formula like "net_income*.4" means PA gets 40%
  const paRate = commPct ?? 0.20; // default 20% if unknown

  // Guest count parsing
  let adults: number | null = null;
  let children: number | null = null;
  if (typeof r.guestsCount === "number") {
    adults = r.guestsCount;
  }

  const header: MappedReservationHeader = {
    source_system: "guesty",
    source_id: r._id,
    channel,
    booked_at: r.createdAt,
    listing_id: r.listingId,
    guest_name: r.guest?.fullName || "Unknown",
    guest_email: r.guest?.email || null,
    confirmation_code: r.confirmationCode || "",
  };

  const state: MappedReservationState = {
    status,
    checkin_date: extractDate(r.checkIn),
    checkout_date: extractDate(r.checkOut),
    adults,
    children,
    babies: null,
    gross_total: Math.round(grossTotal * 100) / 100,
    vat_stay: vatStay,
    vat_cleaning: vatCleaning,
    cleaning_fee_gross: Math.round(fareCleaning * 100) / 100,
    cleaning_fee_net: cleaningNet,
    channel_commission: Math.round(channelComm * 100) / 100,
    channel_commission_pct: commPct,
    net_stay: netStay !== null ? Math.round(netStay * 100) / 100 : null,
    pa_commission_rate: paRate,
    source_system: "guesty",
    raw_payload: {
      guesty_id: r._id,
      confirmation_code: r.confirmationCode,
      guesty_status: r.status,
      platform: r.integration?.platform,
      money_summary: {
        totalPaid: money.totalPaid,
        hostPayout: money.hostPayout,
        fareAccommodation: money.fareAccommodation,
        fareCleaning: money.fareCleaning,
        hostServiceFee: money.hostServiceFee,
        commission: money.commission,
        netIncome: money.netIncome,
        currency: money.currency,
      },
    },
  };

  return {
    header,
    state,
    is_cancelled: status === "CANCELLED",
  };
}
