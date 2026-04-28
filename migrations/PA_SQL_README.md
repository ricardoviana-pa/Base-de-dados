# Portugal Active — Master Data SQL + Dashboard Package

Pacote completo do projeto de Master Data SQL + Dashboard da Portugal Active.

## Estrutura do pacote

```
sql_package/
├── README.md                                              ← este ficheiro
├── briefs/
│   ├── 01_BRIEF_COWORK_SPRINT_0.md                        ← Sprint 0: Cowork cria Supabase + Pipedrive
│   ├── 02_BRIEF_CLAUDE_CODE_SPRINT_1.md                   ← Sprint 1: Claude Code popula SQL
│   └── 03_BRIEF_CLAUDE_CODE_SPRINT_2_DASHBOARD.md         ← Sprint 2: Claude Code constrói dashboard
└── migrations/
    ├── 001_extensions_and_enums.sql
    ├── 002_entities_and_cost_centers.sql
    ├── 003_owners_properties_contracts.sql
    ├── 004_reservations.sql
    ├── 005_cleaning_and_costs.sql
    ├── 006_financial_empresa.sql
    ├── 007_pricing_decisions.sql
    ├── 008_experiences.sql
    ├── 009_pipeline.sql
    ├── 010_budgets.sql
    ├── 011_daily_snapshots.sql
    ├── 012_audit.sql
    └── 013_views.sql
```

## Sequência de execução

### Sprint 0 (Cowork) — ~90 minutos
1. Lança `01_BRIEF_COWORK_SPRINT_0.md` no Cowork
2. Output: 4 credenciais Supabase + ficheiro export Pipedrive

### Sprint 1 + 2 em paralelo (Claude Code) — 3-4 semanas
3. Após receber credenciais, lança 2 sessões do Claude Code:
   - **Sessão A:** `02_BRIEF_CLAUDE_CODE_SPRINT_1.md` (popular SQL)
   - **Sessão B:** `03_BRIEF_CLAUDE_CODE_SPRINT_2_DASHBOARD.md` (construir dashboard)

### Execução das migrations
As 13 migrations executam-se no Supabase SQL Editor em ordem (001 → 013), uma de cada vez. Pode ser feito por ti, Cowork, ou Claude Code (Sessão A normalmente trata disto antes de avançar para imports).

## Tabelas criadas (16 tabelas + 6 views)

**Domínio 1 — Entidades**
- `entities` — multi-tenancy
- `cost_centers` — centros de custo Primavera (dual code: 4-digit + 9-digit)

**Domínio 2 — Catálogo**
- `owners`
- `properties`
- `property_tier_history`
- `owner_contracts`

**Domínio 3 — Reservas (event-sourced)**
- `guests`
- `reservations` (header)
- `reservation_states` (append-only)
- `reservation_events` (log)
- `channels` (catálogo, seeded)

**Domínio 4 — Custos diretos**
- `cleaning_service_catalog` (matriz da Ops Manager)
- `consumables_baseline`
- `laundry_baseline`
- `cleanings`
- `laundry`
- `property_expenses`

**Domínio 5 — Financeiro empresa**
- `primavera_accounts`
- `monthly_pnl`
- `company_costs`

**Domínio 6 — Pricing & AI**
- `pricing_decisions`
- `pricelabs_snapshots`

**Domínio 7 — Atividades**
- `experiences` (seeded com 13 categorias)
- `experience_bookings`

**Domínio 8 — Pipeline (Pipedrive)**
- `pipeline_stages`
- `lead_sources`
- `loss_reasons`
- `properties_pipeline`
- `pipeline_activities`

**Domínio 9 — Budget**
- `budgets`
- `budget_lines_property`
- `budget_lines_company`

**Domínio 10 — Snapshots diários**
- `daily_property_snapshots`
- `daily_company_snapshots`

**Domínio 11 — Audit**
- `audit_log` + triggers em tabelas críticas

**Views analíticas**
- `v_reservation_current`
- `v_reservation_margin`
- `v_otb_vs_stly`
- `v_property_performance_monthly`
- `v_company_pnl_monthly`
- `v_budget_vs_actual_monthly`

## Princípios não-negociáveis

1. **Append-only** — reservation_states nunca é UPDATE
2. **Multi-tenancy desde dia 1** — entity_id em toda tabela transacional
3. **Audit log automático** — triggers gravam mutations em tabelas críticas
4. **Views são API contract** — apps consomem views, nunca tabelas raw
5. **Postgres puro** — zero features Supabase-specific exceto painel

## Próximos sprints (não incluídos)

- Sprint 3: Reconciliação fiscal automatizada vs EB Consultores
- Sprint 4: Live sync Guesty (cron diário)
- Sprint 5: Auth no dashboard + RLS policies
- Sprint 6: Snapshots diários (job às 02:00)
- Sprint 7: Camada Claude AI (chat em português sobre os dados)

---

Documento gerado em Abril 2026. Confidencial Portugal Active.
