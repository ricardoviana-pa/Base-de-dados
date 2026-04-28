// ============================================================================
// Guesty API Client — Auth + Pagination
// ============================================================================

import type {
  GuestyTokenResponse,
  GuestyReservation,
  GuestyListing,
  GuestyPaginatedResponse,
} from "./types.ts";

const BASE_URL = "https://open-api.guesty.com";
const TOKEN_URL = `${BASE_URL}/oauth2/token`;
const API_V1 = `${BASE_URL}/v1`;
const PAGE_SIZE = 100;
const MAX_RETRIES = 3;

let cachedToken: string | null = null;
let tokenExpiresAt = 0;

// ─── Auth ───────────────────────────────────────────────────────────────────

async function getToken(): Promise<string> {
  const now = Date.now();
  if (cachedToken && now < tokenExpiresAt - 60_000) {
    return cachedToken;
  }

  const clientId = Deno.env.get("GUESTY_CLIENT_ID");
  const clientSecret = Deno.env.get("GUESTY_CLIENT_SECRET");
  if (!clientId || !clientSecret) {
    throw new Error("Missing GUESTY_CLIENT_ID or GUESTY_CLIENT_SECRET");
  }

  const resp = await fetch(TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret,
    }),
  });

  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`Auth failed (${resp.status}): ${body}`);
  }

  const data: GuestyTokenResponse = await resp.json();
  cachedToken = data.access_token;
  tokenExpiresAt = now + data.expires_in * 1000;
  return cachedToken;
}

// ─── HTTP with retry ────────────────────────────────────────────────────────

async function apiGet<T>(path: string, params?: Record<string, string>): Promise<T> {
  const url = new URL(`${API_V1}${path}`);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      url.searchParams.set(k, v);
    }
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const token = await getToken();
    const resp = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (resp.status === 429) {
      const waitMs = Math.pow(2, attempt) * 1000;
      console.warn(`Rate limited (attempt ${attempt}/${MAX_RETRIES}), waiting ${waitMs}ms...`);
      await new Promise((r) => setTimeout(r, waitMs));
      continue;
    }

    if (resp.status === 401 && attempt < MAX_RETRIES) {
      console.warn("Token expired mid-session, re-authenticating...");
      cachedToken = null;
      continue;
    }

    if (!resp.ok) {
      const body = await resp.text();
      throw new Error(`API ${resp.status} on ${path}: ${body}`);
    }

    return await resp.json();
  }

  throw new Error(`Failed after ${MAX_RETRIES} retries on ${path}`);
}

// ─── Paginated fetchers ─────────────────────────────────────────────────────

/**
 * Fetch all reservations, paginating automatically.
 * @param since ISO 8601 timestamp — only reservations updated after this date
 */
export async function fetchAllReservations(since?: string): Promise<{
  reservations: GuestyReservation[];
  totalCount: number;
}> {
  const allResults: GuestyReservation[] = [];
  let skip = 0;
  let totalCount = 0;

  const fields = [
    "_id", "status", "checkIn", "checkOut", "source", "nightsCount",
    "guestsCount", "listingId", "guestId", "createdAt", "confirmationCode",
    "listing", "guest", "money", "integration",
  ].join(" ");

  while (true) {
    const params: Record<string, string> = {
      limit: String(PAGE_SIZE),
      skip: String(skip),
      fields,
    };

    if (since) {
      params["sort"] = "updatedAt";
      params["filters"] = JSON.stringify([
        { field: "updatedAt", operator: "$gte", value: since },
      ]);
    }

    const page = await apiGet<GuestyPaginatedResponse<GuestyReservation>>(
      "/reservations",
      params,
    );

    if (skip === 0) {
      totalCount = page.count;
      console.log(`Total reservations to fetch: ${totalCount}`);
    }

    allResults.push(...page.results);
    console.log(`Fetched page: skip=${skip}, got ${page.results.length}, total so far: ${allResults.length}`);

    if (page.results.length < PAGE_SIZE) break;
    skip += PAGE_SIZE;

    // Safety: small delay between pages to avoid rate limits
    await new Promise((r) => setTimeout(r, 200));
  }

  return { reservations: allResults, totalCount };
}

/**
 * Fetch all listings from Guesty.
 */
export async function fetchAllListings(): Promise<GuestyListing[]> {
  const allResults: GuestyListing[] = [];
  let skip = 0;

  while (true) {
    const page = await apiGet<GuestyPaginatedResponse<GuestyListing>>(
      "/listings",
      {
        limit: String(PAGE_SIZE),
        skip: String(skip),
        fields: "_id title nickname active address",
      },
    );

    allResults.push(...page.results);

    if (page.results.length < PAGE_SIZE) break;
    skip += PAGE_SIZE;
    await new Promise((r) => setTimeout(r, 200));
  }

  return allResults;
}
