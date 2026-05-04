# PriceLabs Back-to-Back Analysis — Sprint 5 Phase 2

**Data:** 2026-05-04 · **Source:** PriceLabs Open API + Master SQL (Supabase)

Coverage: 232 PriceLabs listings, 64 push-enabled, 63 mapped to properties.
Forward 365-day curve pulled (23 424 day-rows). Realised pricing from
1 119 confirmed reservations 2024 + 1 325 (2025) + 552 (2026 ano-corrente).

---

## TL;DR — onde está o dinheiro a fugir

| # | Diagnóstico | Magnitude estimada |
|---|---|---|
| 1 | **Booking pace Jun-Dez 2026 está 60-86% atrás de STLY** | **€833k em risco** se não acelerar |
| 2 | **DIRECT colapsou em Abril 2026** (18 → 2 reservas, -89%) | Canal mais rentável, ADR -10% |
| 3 | **LUXURY tier vendeu 0 em Abril 2026** (vs 5 em 2025) | -€18k só nesse mês |
| 4 | **PriceLabs base está sub-calibrado em 10 properties top** | Habitos Lodge vende +52% acima do base; Bandeira Retreat +33%; Nature Hill Duo +30% |
| 5 | **PriceLabs base está sobre-calibrado em outras 7** | T2-Nature-QPA: base €360 mas vende a €191 (-47%) |
| 6 | **Manual DOWN overrides em 4 033 dias** custam ~€32/dia | ~€129k risco se viessem a vender |

---

## 1. Abril 2026 vs Abril 2025 — autópsia

### Por tier
| Tier | Resv 25 | ADR 25 | Gross 25 | Resv 26 | ADR 26 | Gross 26 | Δ ADR | Δ Resv |
|---|---|---|---|---|---|---|---|---|
| STANDARD | 54 | €129 | €22 567 | 67 | €128 | €34 469 | -0.8% | +24.1% |
| PREMIUM | 35 | €365 | €49 096 | 38 | €379 | €42 663 | +3.8% | +8.6% |
| **LUXURY** | **5** | **€645** | **€17 692** | **0** | **—** | **—** | **—** | **-100%** |

**Leitura:** STANDARD +24% reservas mas ADR flat. PREMIUM saudável.
**LUXURY é a ferida aberta** — 0 vendas em Abril 2026 vs 5 em 2025.
Ver Q3 abaixo: investigar se o problema é base price (PriceLabs alto demais para o segmento luxury actual) ou min-stay/restrições.

### Por canal
| Canal | Resv 25 | ADR 25 | Resv 26 | ADR 26 | Δ ADR | Δ Resv |
|---|---|---|---|---|---|---|
| AIRBNB | 57 | €242 | 68 | €213 | **-12.0%** | +19.3% |
| BOOKING | 18 | €159 | 23 | €191 | +20.1% | +27.8% |
| MANUAL | 0 | — | 16 | €249 | — | — |
| **DIRECT** | **18** | **€294** | **2** | **€264** | **-10.2%** | **-88.9%** |
| VRBO | 2 | €667 | 0 | — | — | -100% |

**Leituras críticas:**

1. **DIRECT desapareceu** — 18 → 2 reservas (-89%). Direct é o canal de maior margem (sem comissão Airbnb 15-18% nem Booking 17%). Perder 16 directs vale ~€4 700 só em comissões evitadas. **Investigar** se algo mudou no funil direto (site, Google Ads, retargeting) ou se foram reclassificadas como MANUAL.
2. **MANUAL apareceu do nada** — 0 → 16 reservas. Possivelmente são as DIRECT antigas mas com taxonomia diferente em Guesty (canal "manual" típico de reservas inseridas à mão, e.g. WhatsApp, e-mail). Validar com a equipa de operações.
3. **AIRBNB ADR -12%** com volume +19% — consistente com PriceLabs mais agressivo no preço, captando volume mas erodindo ADR. Ver Q5 abaixo (manual DOWN overrides).
4. **BOOKING ADR +20%** com volume +28% — esta é a história positiva.

---

## 2. Forward Booking Pace — STLY vs hoje

| Mês | Booked-now | Booked-STLY | Gap (dias) | Avg Price € | **Revenue at risk €** |
|---|---|---|---|---|---|
| 2026-05 | 270 | 230 | **+40** ✅ | 234 | (-9 360) |
| 2026-06 | 103 | 309 | -206 | 323 | **66 538** |
| 2026-07 | 209 | 612 | -403 | 463 | **186 589** |
| 2026-08 | 279 | 762 | -483 | 590 | **284 970** |
| 2026-09 | 66 | 413 | -347 | 352 | **122 144** |
| 2026-10 | 37 | 270 | -233 | 278 | **64 774** |
| 2026-11 | 1 | 128 | -127 | 250 | **31 750** |
| 2026-12 | 0 | 201 | -201 | 344 | **69 144** |
| **TOTAL** | | | **-1960** | | **~€833 000** |

**Atenção à interpretação:** -1 960 day-nights atrás de STLY × preço médio = €833k de gap se o ritmo não recuperar. Parte deste gap vai fechar via late bookings (booking window 25% mais curto este ano), mas a magnitude é muito maior do que o ajuste típico de window. **A pace de Jul-Ago está 65% atrás — historicamente as nossas duas semanas mais quentes do ano.**

**Causas possíveis (a investigar):**
- PriceLabs base price elevado demais → preço final visível alto → conversão baixa
- Min-stay restritivo a tirar searches do funil
- Comp set acima de nós em 2025 (ADR_STLY) mas abaixo em 2026 (ADR) — mercado a baixar e nós a manter, ou nós a baixar mais devagar que o mercado.

---

## 3. PriceLabs base price calibration — onde está mal afinado

### Sub-calibrado (vendemos acima do base) → **subir base**
Properties que sistematicamente vendem mais do que o PriceLabs base sugere.
Implica que o algoritmo está a deixar dinheiro na mesa.

| Property | PL Base | ADR realizado 2024-25 | Resv | Gap |
|---|---|---|---|---|
| Portugal Active Habitos Lodge | €330 | **€502** | 28 | **+52%** |
| T3-Bandeira Retreat | €140 | €187 | 36 | +33% |
| T8-Nature Hill Duo | €545 | €711 | 41 | +30% |
| T3-Villa Luzia | €200 | €257 | 11 | +28% |
| T6-Atlantic Lodge | €425 | €535 | 56 | +26% |
| Slow Living Countryside House | €250 | €306 | 34 | +22% |
| T6-Montaria Lodge | €390 | €454 | 44 | +16% |
| BlueGreen Beach Apartment | €115 | €128 | 28 | +11% |
| T2-Encosta House | €105 | €116 | 36 | +11% |
| Cabedelo Beach Lodge | €480 | €527 | 40 | +10% |

**Acção:** subir base price 10-20% nestas. Nas top 3 (Habitos, Bandeira, Nature Hill) o gap é tão grande que justifica revisão imediata para o algoritmo passar a recomendar mais perto do realised.

### Sobre-calibrado (vendemos abaixo do base) → **revisar base ou overrides**
| Property | PL Base | ADR realizado | Resv | Gap |
|---|---|---|---|---|
| **T2-Nature-QPA** | **€360** | **€191** | 9 | **-47%** |
| T1-Beach Flat | €145 | €117 | 49 | -19% |
| T4-SãoJuliãoRetreat | €290 | €248 | 25 | -15% |
| T6-S. Salvador-LRH | €420 | €362 | 21 | -14% |
| T1-Ocean Bliss | €115 | €102 | 47 | -11% |
| T2-DivineWavesDuplex | €130 | €118 | 64 | -9% |
| T3-Salty Escape | €190 | €179 | 34 | -6% |

**Acção:** baixar base 10-15% OU rever regras de override que estão a forçar preços abaixo do base. T2-Nature-QPA é o caso mais alarmante — €360 base mas vende a €191 e só 9 reservas em dois anos. Provavelmente base demasiado optimista — baixar para ~€220.

---

## 4. Manual overrides — comportamento humano

| Tipo override | Dias (próximos 180d) | Δ médio vs PL algoritmo |
|---|---|---|
| Manual UP | 4 648 | **+€34** |
| Manual DOWN | 4 033 | **-€32** |
| Sem override | 2 701 | (delta natural +€25) |
| Igual | 138 | -€0 |

**Leitura:** estamos a fazer overrides em **63% dos dias** dos próximos 180. Isso é muito.
A regra geral: se subir e vender → algoritmo está calibrado baixo (subir base permanente).
Se baixar e vender → conservadorismo em demand alta? Ou medo de perder a reserva?

**4 033 dias com manual DOWN × €32 = ~€129k de revenue potencial não capturado** — assumindo que aqueles dias acabavam por vender ao preço algorítmico em vez do override mais baixo. Realista? Difícil de afirmar sem AB test, mas **a magnitude justifica reduzir overrides DOWN em pelo menos 50%**.

---

## 5. Margin per property — onde está o lucro real

### Top 10 por margem absoluta 2024-2025 (após cleaning + lavandaria)
| Property | Resv | Gross | PA Rev | Direct | Margem | Margem % |
|---|---|---|---|---|---|---|
| EBEN LODGE | 102 | 207 470 | 77 625 | 5 020 | **72 605** | 32.9% |
| T4-SunsetBeachLodge | 104 | 166 520 | 59 655 | 673 | **58 982** | 32.2% |
| T8-Nature Hill Duo | 76 | 134 348 | 46 102 | 0 | **46 102** | 32.5% |
| Cabedelo Beach Lodge | 71 | 134 270 | 44 735 | 2 530 | **42 206** | 30.0% |
| T6-Atlantic Lodge | 79 | 124 434 | 42 212 | 1 680 | **40 532** | 29.0% |
| T8-LimaRiver-LRH | 43 | 84 171 | 37 594 | 0 | **37 594** | 41.8% |
| T6-Montaria Lodge | 68 | 92 503 | 31 338 | 1 788 | **29 549** | 26.0% |
| Habitos Lodge | 35 | 82 147 | 31 399 | 2 038 | **29 361** | 31.5% |
| VF - Vale da Fonte | 101 | 58 960 | 26 921 | 327 | **26 594** | 44.8% |
| T7-Carreço's Farm | 51 | 73 534 | 24 795 | 0 | **24 795** | 29.3% |

### Worst 10 por margem %
| Property | Resv | Gross | Margem | Margem % |
|---|---|---|---|---|
| **Oliveira's Farm \| Heated Pool** | 29 | 54 827 | 10 361 | **17.4%** |
| T0 - Vineyard Loft | 95 | 15 182 | 3 753 | 19.5% |
| Mexia Galvão City Center | 23 | 34 239 | 9 678 | 24.7% |
| T1+1-Heritage Loft | 11 | 2 775 | 733 | 25.3% |
| T6-Montaria Lodge | 68 | 92 503 | 29 549 | 26.0% |
| T2-Encosta House | 56 | 24 516 | 7 140 | 26.2% |
| T5-Refúgio Abreu | 13 | 41 994 | 11 966 | 26.8% |

**Oliveira's Farm 17%** é alarme: gross €55k, margem €10k. Owner share + cleaning consomem 81%. Renegociar contrato ou subir ADR.

---

## 6. Recomendações imediatas

| # | Acção | Owner | Impacto estimado | Prazo |
|---|---|---|---|---|
| 1 | **Investigar collapse DIRECT Abril 2026** (18→2) | Marketing | +€4-6k margin/mês se recuperar | 1 semana |
| 2 | **Subir base price em 10 properties sub-calibradas** (Habitos +20%, Bandeira +20%, Nature Hill +15%) | Revenue (Ricardo) | +€10-15k ADR/mês peak | 2 dias |
| 3 | **Baixar base price em 7 sobre-calibradas** (T2-Nature-QPA -40%) | Revenue | unlock vendas → +€8-12k/mês | 2 dias |
| 4 | **Rever min-stay LUXURY** para Abril (0 vendas é red flag) | Revenue | recuperar 5 reservas/mês × €645 = €3 200 | 1 semana |
| 5 | **Reduzir manual DOWN overrides** em 50% | Revenue | unlock €60-65k revenue 6 meses | imediato |
| 6 | **Push lateral em July-Aug 2026** — pace -65% | Revenue + Marketing | recuperar 20-30% do gap = €100-140k | 4 semanas |
| 7 | **Renegociar contrato Oliveira's Farm** (margem 17%) | Owner Relations | +€3-5k margin/ano | 30 dias |

---

## Cobertura de dados / limitações

- Análise sobre 63 das 64 push-enabled listings (99%). 1 unmatched: "Cabedelo Lima View Duplex".
- Realised pricing de 2024-2025 (5 anos no DB mas 2024-2025 são mais comparáveis ao 2026 actual).
- Mercado/comp set: PriceLabs `ADR_STLY` para Junho-Dezembro 2026 sugere mercado em geral mais fraco que 2025. **Confirma que parte do pace gap é mercado, não exclusivamente nosso pricing**, mas a magnitude (-65% Jul-Ago) ultrapassa qualquer correcção de mercado normal.
- Não fizemos histórico de PriceLabs base prices ainda — só temos snapshot de hoje. Para análise causal "PL recommended X em Janeiro, vendemos Y em Fevereiro", precisamos correr `sync_pricelabs.py prices` diariamente daqui em diante. **Sprint 5.1 deveria automatizar isto via cron.**

---

## Files / artifacts

- `migrations/018_pricelabs_sync.sql` — schema (listing_map + daily_prices)
- `scripts/sync_pricelabs.py` — pull diário de listings + forward curve
- `scripts/analyze_pricelabs_back_to_back.py` — re-correr para ver actualização
- `pricelabs_listing_map` — 232 rows, 209 com property_id
- `pricelabs_daily_prices` — 23 424 day-rows snapshot 2026-05-04
