# Guesty Sync — Deploy Guide

## Pré-requisitos

1. Migration `014_guesty_sync.sql` executada no Supabase SQL Editor
2. Supabase CLI instalado (`npm install -g supabase`)
3. Credenciais OAuth2 do Guesty (Client ID + Secret)

## 1. Configurar Secrets

Os valores reais estão no `.env` local (gitignored). Substitui pelos valores reais ao executar:

```bash
supabase secrets set GUESTY_CLIENT_ID=<see .env: GUESTY_CLIENT_ID>
supabase secrets set GUESTY_CLIENT_SECRET=<see .env: GUESTY_CLIENT_SECRET>
```

Em alternativa, exporta do `.env` numa só linha:

```bash
source .env && supabase secrets set GUESTY_CLIENT_ID=$GUESTY_CLIENT_ID GUESTY_CLIENT_SECRET=$GUESTY_CLIENT_SECRET
```

⚠️ **NUNCA committar credenciais em plaintext.** Este DEPLOY.md vai para git; o `.env` não.

## 2. Deploy da Edge Function

```bash
cd supabase
supabase functions deploy guesty-sync --project-ref xsvpeyckzpwoaancdsyt
```

## 3. Testar (Dry Run)

```bash
curl -X POST https://xsvpeyckzpwoaancdsyt.supabase.co/functions/v1/guesty-sync \
  -H "Authorization: Bearer <SERVICE_ROLE_KEY>" \
  -H "Content-Type: application/json" \
  -d '{"mode": "full_backfill", "dry_run": true}'
```

## 4. Sync Listings (primeira vez)

```bash
curl -X POST https://xsvpeyckzpwoaancdsyt.supabase.co/functions/v1/guesty-sync \
  -H "Authorization: Bearer <SERVICE_ROLE_KEY>" \
  -H "Content-Type: application/json" \
  -d '{"mode": "listings_only"}'
```

Depois verificar `guesty_listing_map` no Supabase para confirmar mapeamentos.

## 5. Full Backfill

```bash
curl -X POST https://xsvpeyckzpwoaancdsyt.supabase.co/functions/v1/guesty-sync \
  -H "Authorization: Bearer <SERVICE_ROLE_KEY>" \
  -H "Content-Type: application/json" \
  -d '{"mode": "full_backfill"}'
```

## 6. Ativar Cron (após validação)

No Supabase SQL Editor:

```sql
SELECT cron.schedule(
  'guesty-daily-sync',
  '0 2 * * *',
  $$
  SELECT net.http_post(
    url := 'https://xsvpeyckzpwoaancdsyt.supabase.co/functions/v1/guesty-sync',
    headers := '{"Authorization": "Bearer <SERVICE_ROLE_KEY>", "Content-Type": "application/json"}'::jsonb,
    body := '{"mode": "incremental"}'::jsonb
  ) AS request_id;
  $$
);
```

## Monitorização

```sql
-- Últimos 10 syncs
SELECT id, sync_type, status, reservations_fetched, states_created, 
       cancellations_detected, skipped_unmapped,
       finished_at - started_at AS duration, error_message
FROM sync_log ORDER BY id DESC LIMIT 10;

-- Listings sem match
SELECT guesty_listing_id, guesty_title 
FROM guesty_listing_map WHERE property_id IS NULL;
```
