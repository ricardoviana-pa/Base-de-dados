// ============================================================================
// Guesty Sync — Type Definitions
// ============================================================================

/** Body do request HTTP à Edge Function */
export interface SyncRequest {
  mode: "incremental" | "full_backfill" | "listings_only";
  since?: string; // ISO 8601 — override do updatedAt filter
  dry_run?: boolean;
}

/** Resposta da API OAuth2 */
export interface GuestyTokenResponse {
  token_type: string;
  expires_in: number;
  access_token: string;
  scope: string;
}

/** Reserva da Guesty API (campos que usamos) */
export interface GuestyReservation {
  _id: string;
  status: string;
  checkIn: string;
  checkOut: string;
  source: string;
  nightsCount: number;
  guestsCount: number;
  listingId: string;
  guestId: string;
  createdAt: string;
  confirmationCode: string;
  listing: {
    _id: string;
    title: string;
  };
  guest: {
    _id: string;
    fullName: string;
    email?: string;
    phone?: string;
  };
  money: GuestyMoney;
  integration?: {
    platform: string;
  };
}

export interface GuestyMoney {
  totalPaid: number;
  hostPayout: number;
  hostOriginalPayout: number;
  fareAccommodation: number;
  fareCleaning: number;
  commission: number;
  commissionFormula: string;
  hostServiceFee: number;
  subTotalPrice: number;
  netIncome: number;
  netIncomeFormula: string;
  currency: string;
  invoiceItems?: GuestyInvoiceItem[];
}

export interface GuestyInvoiceItem {
  normalType: string;
  amount: number;
  title: string;
  currency: string;
  type?: string;
  name?: string;
}

/** Listing da Guesty API */
export interface GuestyListing {
  _id: string;
  title: string;
  nickname: string;
  active: boolean;
  address?: {
    city?: string;
    country?: string;
  };
}

/** Resultado paginado da Guesty API */
export interface GuestyPaginatedResponse<T> {
  results: T[];
  count: number;
  limit: number;
  skip: number;
  fields?: string;
  title?: string;
}

/** Status mapping Guesty → nosso enum */
export const STATUS_MAP: Record<string, string> = {
  confirmed: "CONFIRMED",
  reserved: "CONFIRMED",
  checked_in: "CONFIRMED",
  checked_out: "COMPLETED",
  canceled: "CANCELLED",
  cancelled: "CANCELLED",
  inquiry: "PENDING",
  declined: "CANCELLED",
  expired: "CANCELLED",
  closed: "COMPLETED",
};

/** Channel mapping Guesty source → nosso enum */
export const CHANNEL_MAP: Record<string, string> = {
  airbnb2: "AIRBNB",
  airbnb: "AIRBNB",
  "booking.com": "BOOKING",
  bookingcom: "BOOKING",
  vrbo: "VRBO",
  homeaway: "HOMEAWAY",
  expedia: "EXPEDIA",
  tripadvisor: "TRIPADVISOR",
  google: "GOOGLE",
  holidu: "HOLIDU",
  direct: "DIRECT",
  manual: "MANUAL",
  website: "DIRECT",
};

/** Resultado do sync para logging */
export interface SyncResult {
  sync_type: string;
  started_at: string;
  finished_at: string;
  status: "success" | "partial_failure" | "error";
  reservations_fetched: number;
  reservations_upserted: number;
  states_created: number;
  cancellations_detected: number;
  listings_synced: number;
  skipped_unmapped: number;
  error_message?: string;
  metadata?: Record<string, unknown>;
}
