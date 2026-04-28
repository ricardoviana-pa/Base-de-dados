// ============================================================================
// Guesty Sync — Edge Function Entry Point
// ============================================================================
// Sprint 4: Full sync via Guesty Open API v2
//
// Invocation:
//   POST /functions/v1/guesty-sync
//   Body: { "mode": "incremental" | "full_backfill" | "listings_only", "since?": "ISO8601", "dry_run?": true }
//
// Schedule: pg_cron daily at 02:00 UTC
// ============================================================================

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { fetchAllReservations, fetchAllListings } from "./guesty-client.ts";
import { mapReservation } from "./mapper.ts";
import {
  createSyncLogEntry,
  updateSyncLog,
  getLastSuccessfulSync,
  upsertReservation,
  syncListings,
  clearListingMapCache,
} from "./db.ts";
import type { SyncRequest, SyncResult } from "./types.ts";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

serve(async (req: Request) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: CORS_HEADERS });
  }

  const startedAt = new Date().toISOString();
  let syncLogId: number | null = null;

  try {
    // Parse request
    const body: SyncRequest = req.method === "POST"
      ? await req.json().catch(() => ({ mode: "incremental" }))
      : { mode: "incremental" };

    const mode = body.mode || "incremental";
    const dryRun = body.dry_run === true;
    console.log(`=== Guesty Sync: mode=${mode}, dry_run=${dryRun} ===`);

    // Create sync log entry
    if (!dryRun) {
      syncLogId = await createSyncLogEntry(mode);
    }

    const result: SyncResult = {
      sync_type: mode,
      started_at: startedAt,
      finished_at: "",
      status: "success",
      reservations_fetched: 0,
      reservations_upserted: 0,
      states_created: 0,
      cancellations_detected: 0,
      listings_synced: 0,
      skipped_unmapped: 0,
      metadata: { dry_run: dryRun },
    };

    // ─── Step 1: Sync listings (always on full_backfill or listings_only) ───
    if (mode === "full_backfill" || mode === "listings_only") {
      console.log("Fetching all listings from Guesty...");
      const listings = await fetchAllListings();
      console.log(`Fetched ${listings.length} listings`);

      if (!dryRun) {
        result.listings_synced = await syncListings(listings);
        console.log(`Synced ${result.listings_synced} listings to guesty_listing_map`);
        clearListingMapCache(); // Force reload after sync
      } else {
        result.listings_synced = listings.length;
        console.log(`[DRY RUN] Would sync ${listings.length} listings`);
      }

      if (mode === "listings_only") {
        result.finished_at = new Date().toISOString();
        if (syncLogId) await updateSyncLog(syncLogId, result);
        return jsonResponse(result);
      }
    }

    // ─── Step 2: Determine sync window ──────────────────────────────────
    let since: string | undefined;

    if (mode === "incremental") {
      since = body.since || (await getLastSuccessfulSync()) || undefined;
      if (!since) {
        console.warn("No previous sync found and no 'since' provided. Falling back to full_backfill.");
      }
      console.log(`Incremental sync since: ${since || "ALL TIME (fallback)"}`);
    } else {
      since = body.since; // full_backfill: only use explicit since
      console.log(`Full backfill${since ? ` since ${since}` : " (all time)"}`);
    }

    // ─── Step 3: Fetch reservations ─────────────────────────────────────
    const { reservations, totalCount } = await fetchAllReservations(since);
    result.reservations_fetched = reservations.length;
    console.log(`Fetched ${reservations.length} reservations (API reports ${totalCount} total)`);

    if (dryRun) {
      // Report what we'd do without writing
      let wouldCancel = 0;
      let wouldSkip = 0;
      for (const r of reservations) {
        const mapped = mapReservation(r);
        if (mapped.is_cancelled) wouldCancel++;
        if (!mapped.header.listing_id) wouldSkip++;
      }
      result.cancellations_detected = wouldCancel;
      result.skipped_unmapped = wouldSkip;
      result.finished_at = new Date().toISOString();
      console.log(`[DRY RUN] Would process ${reservations.length} reservations, ${wouldCancel} cancellations`);
      return jsonResponse(result);
    }

    // ─── Step 4: Process each reservation ───────────────────────────────
    const errors: string[] = [];

    for (let i = 0; i < reservations.length; i++) {
      const r = reservations[i];
      try {
        const mapped = mapReservation(r);
        const upsertResult = await upsertReservation(mapped);

        if (upsertResult.skipped) {
          result.skipped_unmapped++;
          if (upsertResult.error) {
            console.warn(`Skipped ${r._id}: ${upsertResult.error}`);
          }
          continue;
        }

        if (upsertResult.upserted) result.reservations_upserted++;
        if (upsertResult.stateCreated) result.states_created++;
        if (upsertResult.isCancellation) result.cancellations_detected++;

        if (upsertResult.error) {
          errors.push(`${r._id}: ${upsertResult.error}`);
        }
      } catch (err) {
        const msg = `Exception processing ${r._id}: ${(err as Error).message}`;
        console.error(msg);
        errors.push(msg);
      }

      // Progress log every 50 reservations
      if ((i + 1) % 50 === 0) {
        console.log(`Processed ${i + 1}/${reservations.length}...`);
      }
    }

    // ─── Step 5: Finalize ───────────────────────────────────────────────
    result.finished_at = new Date().toISOString();

    if (errors.length > 0) {
      result.status = errors.length > reservations.length / 2 ? "error" : "partial_failure";
      result.error_message = errors.slice(0, 20).join(" | ");
      result.metadata = { ...result.metadata, total_errors: errors.length };
    }

    if (syncLogId) await updateSyncLog(syncLogId, result);

    const duration = (new Date(result.finished_at).getTime() - new Date(startedAt).getTime()) / 1000;
    console.log(`=== Sync complete: ${result.status} in ${duration.toFixed(1)}s ===`);
    console.log(`  Fetched: ${result.reservations_fetched}`);
    console.log(`  Upserted: ${result.reservations_upserted}`);
    console.log(`  States created: ${result.states_created}`);
    console.log(`  Cancellations: ${result.cancellations_detected}`);
    console.log(`  Skipped (unmapped): ${result.skipped_unmapped}`);

    return jsonResponse(result);
  } catch (err) {
    const errorMsg = (err as Error).message || String(err);
    console.error(`Fatal error: ${errorMsg}`);

    const failResult: SyncResult = {
      sync_type: "unknown",
      started_at: startedAt,
      finished_at: new Date().toISOString(),
      status: "error",
      reservations_fetched: 0,
      reservations_upserted: 0,
      states_created: 0,
      cancellations_detected: 0,
      listings_synced: 0,
      skipped_unmapped: 0,
      error_message: errorMsg,
    };

    if (syncLogId) await updateSyncLog(syncLogId, failResult);

    return jsonResponse(failResult, 500);
  }
});

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
  });
}
