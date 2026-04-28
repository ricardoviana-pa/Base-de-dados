# Portugal Active — Master Data SQL (Sprint 1)

Scripts Python idempotentes para importar dados históricos (Doc Único 2020-2024,
Rental Ready 2024-2025, Guesty 2026, contabilidade Primavera, budget 2026) para
a base Master no Supabase.

## Estrutura

```
.
├── migrations/                       # 13 SQL files (001-013) + PA_SQL_README.md
│                                     #   Correr no Supabase SQL Editor por ordem
├── scripts/
│   ├── common/                       # Módulos partilhados
│   │   ├── db.py                     #   conn(), upsert helpers, get_entity_id()
│   │   ├── excel_utils.py            #   to_decimal, to_date, header_map, ...
│   │   ├── logging_utils.py          #   setup_logging() → logs/<script>_<ts>.log
│   │   └── property_match.py         #   PropertyResolver (fuzzy match across sources)
│   ├── import_cost_centers.py        # 01: Listagem CC → cost_centers
│   ├── import_cleaning_catalog.py    # 02: Ops Costs → cleaning_service_catalog,
│   │                                 #     consumables_baseline, laundry_baseline
│   ├── import_doc_unico.py           # 03: Doc Único 7 sheets → owners, properties,
│   │                                 #     contracts, guests, reservations, ...
│   ├── import_excel_rr.py            # 04: RentalReady Export → reservations
│   ├── import_guesty.py              # 05: Guesty Excel → reservations (Sprint 6 = API)
│   ├── import_budget_2026.py         # 06: PTAC_BUD26_EBITDA → budget_lines_property + company
│   └── _inspect_excels.py            # dev helper to dump headers / sample rows
├── logs/                             # Gerado em runtime; um log por execução
├── .env.example                      # Copy → .env e preenche credenciais
├── requirements.txt
└── README.md
```

## Setup

```bash
# 1) Migrations no Supabase (SQL Editor)
#    Cola cada ficheiro em migrations/001 ... 013 por ordem, um de cada vez.

# 2) Credenciais
cp .env.example .env
# Edita .env com SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_DB_CONNECTION_STRING.

# 3) Ambiente Python
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Ordem de execução

Os scripts têm dependências de FK. Correr nesta ordem:

```bash
.venv/bin/python scripts/import_cost_centers.py                         # 1º — sem FK
.venv/bin/python scripts/import_cleaning_catalog.py                     # 2º — sem FK
.venv/bin/python scripts/import_doc_unico.py                            # 3º — cria properties + owners + contracts
.venv/bin/python scripts/import_excel_rr.py                             # 4º — depende de properties
.venv/bin/python scripts/import_guesty.py                               # 5º — depende de properties
.venv/bin/python scripts/import_budget_2026.py                          # 6º — depende de properties (placeholders OK)
.venv/bin/python scripts/dedupe_properties_and_reservations.py          # 7º — funde dupes RR↔Guesty
.venv/bin/python scripts/post_import_enrich.py                          # 8º — financial split, cleaning costs, links
```

Cada script:
- É **idempotente** — pode ser corrido N vezes sem duplicar.
- Imprime resumo no stdout e grava log detalhado em `logs/<script>_YYYYMMDD_HHMMSS.log`.
- Faz commit por sheet/secção (em vez de uma única transação enorme), para que falhas tardias não revertam o trabalho já feito.
- Em caso de erro fatal, faz rollback da transação aberta e sai com código 1.

## Onde estão os Excel-fonte

Os scripts procuram em `SOURCE_DATA_DIR` (default `~/Downloads`). Os 6 ficheiros
exatos esperados (já presentes em `~/Downloads/`):

| Script | Ficheiro |
|--------|----------|
| 01 | `Listagem Centros de Custo.xlsx` |
| 02 | `PA_Ops_Costs_final.xlsx` |
| 03 | `v30-DOCUMENTO ÚNICO_Release_2.1.1.xlsm` |
| 04 | `Accounting_RentalReady - NOVEMBRO - 2025 (1).xlsm` |
| 05 | `Teste_Accounting_Guesty - 2026.xlsm` |
| 06 | `PTAC_BUD26_EBITDA.xlsx` |

Se preferires consolidar localmente: copia/move os 6 ficheiros para `source_data/`
e ajusta `SOURCE_DATA_DIR` no `.env`.

## Princípios de idempotência (e os seus limites em Sprint 1)

| Tabela | Idempotente? | Mecanismo |
|--------|--------------|-----------|
| `cost_centers` | ✓ | UPSERT em `(entity_id, code_short, fiscal_year)` |
| `cleaning_service_catalog` | ✓ | UPSERT em `(tier, tipologia, service_type, effective_from)` |
| `consumables_baseline`, `laundry_baseline` | ✓ | Fecha rows abertas (`effective_to=hoje`) e insere novas |
| `owners` | ✓ | SELECT-then-INSERT em `(entity_id, legal_name)` |
| `properties` | ✓ | SELECT-then-INSERT em `(entity_id, doc_unico_id)` |
| `owner_contracts` | ✓ (best-effort) | INSERT com `ON CONFLICT DO NOTHING`; sem UNIQUE explícito |
| `guests` | ✓ se há email | UPSERT em `email_normalized`; sem email → não cria guest |
| `reservations` | ✓ | UPSERT em `(source_system, source_id)` |
| `reservation_states` | ✓ | Fecha estado atual (`effective_to=NOW()`) e insere novo |
| `reservation_events` | ✓ (BOOKED) | Insere `BOOKED` apenas se ainda não existir |
| `cleanings`, `laundry`, `property_expenses`, `experience_bookings` | ⚠ Sprint 2 | Sem UNIQUE — re-execução duplica. Limpar tabela antes de re-correr, ou aceitar e deduplicar manualmente. |
| `budgets` | ✓ | UPSERT em `(entity_id, fiscal_year, version_name)` |
| `budget_lines_property` | ✓ | UPSERT em `(budget_id, COALESCE(property_id::text, placeholder_label), year, month)` |
| `budget_lines_company` | ✓ | DELETE-then-INSERT por `budget_id` (não há UNIQUE) |

**Para re-correr o doc_unico do zero** sem preocupações de duplicados nas tabelas
fracas (`cleanings`, `laundry`, `property_expenses`, `experience_bookings`):

```sql
-- No SQL Editor, antes de correr o import_doc_unico.py uma 2ª vez:
DELETE FROM experience_bookings WHERE entity_id = (SELECT id FROM entities WHERE primavera_company_code='RTV');
DELETE FROM property_expenses;
DELETE FROM laundry;
DELETE FROM cleanings;
```

## Reconciliação cross-source

- **Properties:** `properties.doc_unico_id` (numérico), `rental_ready_id` (string),
  `guesty_id` (alfanumérico). `import_doc_unico.py` cria a row mestre; `import_excel_rr.py`
  e `import_guesty.py` preenchem os IDs nas rows existentes.
- **Reservas duplicadas entre sources:** Cada source mantém a sua própria reserva
  porque `(source_system, source_id)` é UNIQUE. A view `v_reservation_current`
  expõe estado atual por reserva, não tenta deduplicar — Sprint 3 faz reconciliação
  fiscal contra o balancete EB Consultores.
- **Owners:** No Doc Único não há nomes de owners → criados como placeholders
  (`Owner of <Building>`). Sprint 4 reconcilia contra contas Primavera 2782101xxx.

## Resultado do primeiro run completo (28-04-2026)

Após 1ª execução + recuperação de orfãos via COMISSÕES seeder + dedupe cross-source:

| Tabela | Rows | Notas |
|---|---:|---|
| entities | 1 | RTV |
| cost_centers | 74 | 17 inativos (offboarded), 54 ativos |
| properties | 130 | Após dedupe (de 201 iniciais → 29 nome-canonical → 40 reservation-overlap → 130 únicas, 83 com RR+Guesty IDs) |
| owners | 109 | 45 placeholders Doc Único + 64 reais (do COMISSÕES) |
| owner_contracts | 250 | 90 históricos (2020-2024) + 160 atuais (2025+) (após dedupe) |
| guests | 1 340 | Apenas com email (1 046 sem email skipados) |
| **reservations** | **6 780** | header rows preservadas (audit completo) |
| **reservation_states current** | **5 614** | Apenas estados não-superseded — 1 162+3 RR superseded por Guesty |
| reservation_states | 8 143 | Inclui re-runs (estado fechado + novo) |
| cleanings / laundry / property_expenses | 1 577 / 151 / 874 | Doc Único 2020-2024 |
| experience_bookings | 345 | 13 categorias seed |
| budget_lines_property / company | 1 452 / 732 | BUD2026_v1 status DRAFT |
| audit_log | 1 025 | Triggers a funcionar |

**Confirmed reservations 2026 (após dedupe):** 477 reservas / **€835 044**
- Guesty (PMS oficial): 428 / €771 620
- Rental Ready apenas (sem equivalente Guesty): 49 / €63 424 — provavelmente reservas Booking/Airbnb que não estavam no snapshot Excel Guesty

**`v_otb_vs_stly` ao vivo (após dedupe):**
- OTB 2026 (booked até hoje, checkin 2026): €287 789 (334 reservas)
- OTB STLY 2025 (booked à mesma data ano passado): €499 955 (361 reservas)
- BOB futuro: €667 852

**Orphans residuais:** 3 reservas Guesty (de 2 207) — Sprint 2 desambigua manualmente.

## Sobre dupla contagem cross-source (importante)

O brief avisava: "uma reserva pode existir tanto no Rental Ready como no Guesty
(período de overlap)". Esses dois sistemas duplicavam reservas (RR é accounting
derivado do Guesty PMS), inflacionando o gross 2026 de **€835k → €1.37M** se
não fossem deduplicadas.

A solução implementada:
1. **`scripts/dedupe_properties_and_reservations.py`** — fundir properties que
   se referem à mesma casa física em fontes diferentes (29 via nome canónico
   + 40 via overlap de reservas com mesma data + gross±10%).
2. Para reservas que partilhem `(property_id, checkin, checkout)` entre fontes,
   o estado da versão **Rental Ready é marcado superseded** (`effective_to=NOW()`)
   e a versão **Guesty fica como atual** (PMS é canónico). A reserva header
   permanece para audit; só o estado atual fica "fechado". Um evento
   `NOTE_ADDED` regista a supersedence.

**Como correr:** depois de `import_excel_rr.py` + `import_guesty.py`, sempre:
```bash
.venv/bin/python scripts/dedupe_properties_and_reservations.py
.venv/bin/python scripts/post_import_enrich.py
```

Para a sequência completa, ver "Ordem de execução" no início.

## Limitações conhecidas (Sprint 1 → 2 backlog)

1. **`reservation_states.pa_revenue_gross` / `owner_share` estão NULL** — view
   `v_property_performance_monthly` mostra `pa_revenue=0` para todas as properties.
   Os scripts não estão a calcular o split na altura do insert. Fix: depois de
   inserir o state, computar `pa_revenue_gross = (gross_total - vat_stay - cleaning_fee_net) * pa_commission_rate`
   e fazer UPDATE. Trivial mas requer cuidado com vista a vista.
2. **`cleanings.cost_gross` vem maioritariamente NULL** do Doc Único (a coluna
   está vazia no ficheiro). View `v_reservation_margin` devolve `margin=NULL`
   por isso. Sprint 2 cruza com a matriz `cleaning_service_catalog` para inferir
   custo a partir de `(tier, tipologia, service_type)`.
3. **`cleanings.service_type`** hard-coded a `CO_L`. Sprint 2 mapeia da coluna
   'Service' do CLEAN sheet.
4. **`laundry` do Doc Único** sem coluna de propriedade — todas as rows
   atribuídas à primeira property encontrada. Sprint 2 separa por dia/lodge.
5. **Sem owner names em Doc Único** — owners 2020-2024 são placeholders
   ("Owner of <Building>"). Sprint 4 reconcilia contra Primavera 2782101xxx.
6. **Country code** dos guests é heurística básica. Sprint 2: lookup ISO 3166.
7. **Channel mapping** caso "Feel Viana", "Outros Lodges", etc. → `OTHER`.
8. **Property name match cross-source** — 16 doc_unico+RR, 12 doc_unico+guesty,
   24 RR+guesty (de 201 properties). Sprint 2 deduplica por proximidade ou
   alias manual.
9. **3 orphans Guesty** persistem (provavelmente properties novas só vistas
   no Export, não na COMISSÕES) — verificar manualmente no log.
10. **3 valores absurdos em experience_bookings** foram capados a NULL (ex.
    €49 999 998 para um SHUTTLE). Sprint 2 trata o parsing decimal com
    delimitadores variados.

## Repo Git

Este projeto vai migrar para um repo dedicado no GitHub (separado dos outros
projetos Portugal Active) — para já está num diretório local. Antes de fazer
push:

```bash
cd /Users/ricardoviana/Documents/Claude/Projects/REVENUE
git init
git add .                    # .gitignore exclui .env, .venv/, logs/, source_data/
git commit -m "Sprint 1: Master Data import scripts"
# gh repo create portugal-active-master-data --private --source=. --push
```
