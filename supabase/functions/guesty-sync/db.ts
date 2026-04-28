// ============================================================================
// Database Operations — Upsert reservations + states
// ============================================================================

import { createClient, SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2";
import type { MappedReservation, MappedReservationHeader, MappedReservationState } from "./mapper.ts";
import type { SyncResult } from "./types.ts";

let supabase: SupabaseClient;

export function getSupabase(): SupabaseClient {
  if (!supabase) {
    const url = Deno.env.get("SUPABASE_URL")!;
    const key = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    supabase = createClient(url, key, {
      auth: { autoRefreshToken: false, persistSession: false },
    });
  }
  return supabase;
}

// ─── Listing map ────────────────────────────────────────────────────────────

interface ListingMapRow {
  guesty_listing_id: string;
  property_id: string;
}

let listingMapCache: Map<string, string> | null = null;

export async function loadListingMap(): Promise<Map<string, string>> {
  if (listingMapCache) return listingMapCache;

  const sb = getSupabase();
  const { data, error } = await sb
    .from("guesty_listing_map")
    .select("guesty_listing_id, property_id");

  if (error) throw new Error(`Failed to load listing map: ${error.message}`);

  listingMapCache = new Map<string, string>();
  for (const row of (data as ListingMapRow[]) || []) {
    listingMapCache.set(row.guesty_listing_id, row.property_id);
  }
  console.log(`Listing map loaded: ${listingMapCache.size} mappings`);
  return listingMapCache;
}

export function clearListingMapCache(): void {
  listingMapCache = null;
}

// ─── Sync listings ──────────────────────────────────────────────────────────

interface GuestyListingForDb {
  _id: string;
  title: string;
  active: boolean;
}

/**
 * Sync Guesty listings into guesty_listing_map.
 * Auto-matches by title similarity against properties table.
 */
export async function syncListings(listings: GuestyListingForDb[]): Promise<number> {
  const sb = getSupabase();
  let synced = 0;

  // Load existing properties for matching
  const { data: properties, error: propErr } = await sb
    .from("properties")
    .select("id, name");

  if (propErr) throw new Error(`Failed to load properties: ${propErr.message}`);

  const propMap = new Map<string, string>();
  for (const p of properties || []) {
    // Normalize for matching: lowercase, remove common suffixes
    const normalized = (p.name as string).toLowerCase().trim();
    propMap.set(normalized, p.id as string);
  }

  for (const listing of listings) {
    // Try exact match first, then fuzzy
    const listingNorm = listing.title.toLowerCase().trim();
    let propertyId = propMap.get(listingNorm);

    if (!propertyId) {
      // Try partial match: find property whose name is contained in listing title or vice versa
      for (const [propName, propId] of propMap.entries()) {
        if (listingNorm.includes(propName) || propName.includes(listingNorm)) {
          propertyId = propId;
          break;
        }
      }
    }

    if (!propertyId) {
      // Try matching first significant words
      const listingWords = listingNorm.split(/[\s\-–—]+/).filter((w) => w.length > 3);
      for (const [propName, propId] of propMap.entries()) {
        const propWords = propName.split(/[\s\-–—]+/).filter((w) => w.length > 3);
        const common = listingWords.filter((w) => propWords.includes(w));
        if (common.length >= 2) {
          propertyId = propId;
          break;
        }
      }
    }

    if (propertyId) {
      const { error } = await sb.from("guesty_listing_map").upsert(
        {
          guesty_listing_id: listing._id,
          property_id: propertyId,
          guesty_title: listing.title,
          guesty_active: listing.active,
          synced_at: new Date().toISOString(),
        },
        { onConflict: "guesty_listing_id" },
      );
      if (error) {
        console.error(`Failed to upsert listing ${listing._id}: ${error.message}`);
      } else {
        synced++;
      }
    } else {
      console.warn(`No property match for listing: "${listing.title}" (${listing._id})`);
    }
  }

  clearListingMapCache();
  return synced;
}

// ─── Entity ID ──────────────────────────────────────────────────────────────

let entityIdCache: string | null = null;

async function getEntityId(): Promise<string> {
  if (entityIdCache) return entityIdCache;
  const sb = getSupabase();
  const { data, error } = await sb.from("entities").select("id").limit(1).single();
  if (error || !data) throw new Error(`Failed to get entity: ${error?.message}`);
  entityIdCache = data.id as string;
  return entityIdCache;
}

// ─── Upsert reservation + state ─────────────────────────────────────────────

interface UpsertResult {
  upserted: boolean;
  stateCreated: boolean;
  isCancellation: boolean;
  skipped: boolean;
  error?: string;
}

export async function upsertReservation(mapped: MappedReservation): Promise<UpsertResult> {
  const sb = getSupabase();
  const { header, state } = mapped;
  const result: UpsertResult = {
    upserted: false,
    stateCreated: false,
    isCancellation: mapped.is_cancelled,
    skipped: false,
  };

  try {
    // 1. Resolve property_id from listing map
    const listingMap = await loadListingMap();
    const propertyId = listingMap.get(header.listing_id);
    if (!propertyId) {
      result.skipped = true;
      result.error = `No property mapping for listing ${header.listing_id}`;
      return result;
    }

    const entityId = await getEntityId();

    // 2. Upsert reservation header
    const { data: resData, error: resErr } = await sb
      .from("reservations")
      .upsert(
        {
          source_system: header.source_system,
          source_id: header.source_id,
          entity_id: entityId,
          property_id: propertyId,
          channel: header.channel,
          booked_at: header.booked_at,
        },
        { onConflict: "source_system,source_id" },
      )
      .select("id")
      .single();

    if (resErr) {
      result.error = `Reservation upsert failed: ${resErr.message}`;
      return result;
    }
    result.upserted = true;
    const reservationId = resData.id;

    // 3. Check current state — only append if different
    const { data: currentState } = await sb
      .from("reservation_states")
      .select("id, status, checkin_date, checkout_date, gross_total, cleaning_fee_gross")
      .eq("reservation_id", reservationId)
      .is("effective_to", null)
      .single();

    const hasChanged =
      !currentState ||
      currentState.status !== state.status ||
      currentState.checkin_date !== state.checkin_date ||
      currentState.checkout_date !== state.checkout_date ||
      Math.abs((currentState.gross_total as number) - state.gross_total) > 0.01 ||
      Math.abs((currentState.cleaning_fee_gross as number) - state.cleaning_fee_gross) > 0.01;

    if (!hasChanged) {
      // No change, skip state append
      return result;
    }

    // 4. Close current state
    if (currentState) {
      const { error: closeErr } = await sb
        .from("reservation_states")
        .update({ effective_to: new Date().toISOString() })
        .eq("id", currentState.id);

      if (closeErr) {
        console.error(`Failed to close state ${currentState.id}: ${closeErr.message}`);
      }
    }

    // 5. Append new state
    const { error: stateErr } = await sb.from("reservation_states").insert({
      reservation_id: reservationId,
      status: state.status,
      checkin_date: state.checkin_date,
      checkout_date: state.checkout_date,
      adults: state.adults,
      children: state.children,
      babies: state.babies,
      gross_total: state.gross_total,
      vat_stay: state.vat_stay,
      vat_cleaning: state.vat_cleaning,
      cleaning_fee_gross: state.cleaning_fee_gross,
      cleaning_fee_net: state.cleaning_fee_net,
      channel_commission: state.channel_commission,
      channel_commission_pct: state.channel_commission_pct,
      net_stay: state.net_stay,
      pa_commission_rate: state.pa_commission_rate,
      source_system: state.source_system,
      raw_payload: state.raw_payload,
      effective_from: new Date().toISOString(),
      effective_to: null,
    });

    if (stateErr) {
      result.error = `State insert failed: ${stateErr.message}`;
      return result;
    }
    result.stateCreated = true;

    // 6. Log event if cancellation
    if (mapped.is_cancelled && currentState?.status !== "CANCELLED") {
      await sb.from("reservation_events").insert({
        reservation_id: reservationId,
        event_type: "CANCELLED",
        event_at: new Date().toISOString(),
        details: { previous_status: currentState?.status, source: "guesty_sync" },
        source_system: "guesty",
        triggered_by: "SYSTEM_AUTO",
      });
    }

    return result;
  } catch (err) {
    result.error = `Exception: ${(err as Error).message}`;
    return result;
  }
}

// ─── Sync log ───────────────────────────────────────────────────────────────

export async function createSyncLogEntry(syncType: string): Promise<number> {
  const sb = getSupabase();
  const { data, error } = await sb
    .from("sync_log")
    .insert({
      source_system: "guesty",
      sync_type: syncType,
      started_at: new Date().toISOString(),
      status: "running",
    })
    .select("id")
    .single();

  if (error) throw new Error(`Failed to create sync_log: ${error.message}`);
  return data.id as number;
}

export async function updateSyncLog(id: number, result: SyncResult): Promise<void> {
  const sb = getSupabase();
  const { error } = await sb.from("sync_log").update({
    finished_at: result.finished_at,
    status: result.status,
    reservations_fetched: result.reservations_fetched,
    reservations_upserted: result.reservations_upserted,
    states_created: result.states_created,
    cancellations_detected: result.cancellations_detected,
    listings_synced: result.listings_synced,
    skipped_unmapped: result.skipped_unmapped,
    error_message: result.error_message || null,
    metadata: result.metadata || null,
  }).eq("id", id);

  if (error) console.error(`Failed to update sync_log ${id}: ${error.message}`);
}

export async function getLastSuccessfulSync(): Promise<string | null> {
  const sb = getSupabase();
  const { data } = await sb
    .from("sync_log")
    .select("finished_at")
    .eq("source_system", "guesty")
    .eq("status", "success")
    .order("finished_at", { ascending: false })
    .limit(1)
    .single();

  return data?.finished_at as string | null;
}
