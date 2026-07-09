
[![30-second visual demo](before-after-demo.png)](before-after-demo.pdf)

# Automotive Data Engineering — Pipeline to Agent Knowledge Workflow

One workflow in two complementary stages:

1. **Medallion Data Pipeline** — cleans and analyses a raw car-specs dataset through
   a Bronze → Silver → Gold lakehouse, ending in an analytical dashboard.
2. **WikiBrain** — captures the reusable engineering patterns and results from that
   work as searchable notes, and gives an AI agent (**Hermes**) hybrid search over
   them.

The thread between them: **do the analysis, then pass the result to the agent.** The
pipeline produces the insights and the reusable patterns behind them; those are
written up as atomic notes that Hermes can retrieve later through WikiBrain.

```
raw car-specs data
   → [ Medallion pipeline: clean + type + aggregate + dashboard ]
      → engineering patterns & analytical results
         → [ WikiBrain: indexed, hybrid-searchable notes ]
            → Hermes agent retrieves them on demand
```

**A note on tooling and cost:** tools are chosen per stage, not by dogma. The pipeline
runs on **Databricks** (a cloud platform); the agent's retrieval layer runs on **local
hardware** because it's cheaper per query and keeps the knowledge base private. Cloud
is used where it's the right tool; local is preferred where it saves cost.

---
---

# Stage 1 — Medallion Data Pipeline (Databricks)

An end-to-end **Bronze → Silver → Gold** lakehouse over a raw car-specs dataset,
implemented in PySpark and Spark SQL on Databricks Free Edition, with a native
AI/BI dashboard on top.

## Dataset

`Cars Datasets 2025.csv` — 1,218 rows, 11 columns, all raw dirty strings
(prices like `$1,450,000`, horsepower embedded in free text, mixed fuel labels
with typos). The data spans the whole market: mainstream and economy models
through to luxury cars and hypercars. Ingested via the Databricks UI into a Delta
table.

## Bronze — raw ingestion

The CSV lands untouched in `workspace.layer_bronze.raw_cars` as a Delta table.
No cleaning at this layer: it preserves the source exactly as received, so the
pipeline is always replayable from a faithful copy.

## Silver — cleaning & typing (PySpark)

The Silver layer turns dirty strings into a typed, analysis-ready Delta table
(`workspace.silver.clean_cars`). Key logic:

- **Numeric extraction with `try_cast`** — regex-extract horsepower, top speed,
  price, cc, etc., then `try_cast` (not `cast`) so malformed values become `NULL`
  instead of throwing under Spark's strict casting.
- **`fuel_category`** — normalises messy fuel labels into clean categories, with a
  thermal-priority rule for hybrids and tolerance for source typos (e.g. `hyrbrid`).
- **`engine_cc` / `battery_capacity_kwh` split** — separates combustion displacement
  from electric capacity into two clean columns.
- **`vehicle_class`** — `light` / `heavy`: a curated list of high-end brands is always
  `light`; otherwise a large thermal engine (`engine_cc > 7000` and non-electric)
  marks a `heavy` vehicle (trucks).
- **`price_segment`** — price tiers for cars (`< $40k` Economy, `< $100k` Premium,
  else Luxury), plus a dedicated **Commercial** label for heavy vehicles (trucks)
  rather than a meaningless `NULL`.
- **`is_showroom_model`** — a single centralised boolean flag introduced in Silver,
  computed from the model name (`~car_name RLIKE 'Race Car|XL1'`). It marks race
  cars and prototypes — the Mazda 787B (a Le Mans prototype whose name carries
  "Race Car") and the VW XL1 — as non-showroom. These rows are **kept in Silver**
  (they're real data) but excluded downstream in Gold, so the rule lives in one
  place instead of being repeated as a filter in every Gold query.
- **Drop rows with no price** — the final Silver `select` filters out records with a
  null `price_usd`, since a car with no price can't feed the price-based analyses.
- Written with `.option("overwriteSchema", "true")` to handle Delta schema evolution.

### Data-quality principle: three separate mechanisms

The guiding rule, documented in the companion exploration notebook, is: *exclude data
that is impossible or internally contradictory, never data that is merely surprising —
a surprising-but-real value is an insight, not an error.* That rule plays out through
three deliberately distinct mechanisms, each removing a different kind of row:

- **Showroom flag** (`is_showroom_model`, in Silver) — removes *non-showroom vehicles*
  from Gold: race cars and prototypes (Mazda 787B, VW XL1). Discovered when
  `brand_metrics` showed Mazda with a $5,000,000 max price.
- **Validity filter** (`horsepower <= 2000`, in Gold) — removes *impossible values*:
  the Nissan Urvan listed at 2,488 hp, almost certainly its `2488cc` displacement
  mis-entered into the horsepower column. The real Bugatti hypercars at 1,600–1,850 hp
  were verified and kept.
- **Scope filter** (`horsepower >= 50`, in Gold) — removes *valid-but-out-of-scope*
  micro-cars and vintage models (e.g. Tata Nano, Karmann Ghia) that don't represent
  the modern market being analysed.

The counter-examples were kept on purpose: 300+ hp Economy cars (Corvette, Mustang,
hot hatches) are real performance-per-dollar and stay; GMC sitting in the Luxury tier
with a low top speed is valid (expensive SUVs/pickups) and is documented as a known
limitation of price-only segmentation rather than scrubbed away.

## Gold — analytics (Spark SQL)

Six purpose-built tables in `workspace.gold.*`, all gated on the same two conditions —
`vehicle_class = 'light'` (excludes Commercial trucks) and `is_showroom_model = true`
(excludes race cars/prototypes) — so the data-quality rules defined in Silver are
applied consistently everywhere:

| Table | What it answers |
|-------|-----------------|
| `brand_metrics` | Aggregate metrics per brand. |
| `fuel_by_segment` | Fuel mix grouped by **fuel category × price segment** — so petrol sports cars aren't averaged against petrol city cars. |
| `brand_metrics_economy` / `_premium` / `_luxury` | Per-segment brand tables, built by a single parameterised Python function `build_segment_table` rather than three copy-pasted queries. |
| `price_per_hp_petrol` | Price efficiency ($/hp) on petrol cars only, for a clean like-for-like comparison. |
| `power_sweet_spot` | Where horsepower concentrates, petrol-only. |

## Dashboard (Databricks AI/BI)

A native four-chart dashboard:

- **Price-per-hp by segment** (bar): 163 → 182 → 714 $/hp (Economy → Premium → Luxury).
- **Brand prices in the Luxury tier** (horizontal bar).
- **Petrol model distribution by segment** (donut): Economy 47% / Premium 35% /
  Luxury 18% (418 / 311 / 164 of 893 petrol models).
- **Average horsepower by segment** (horizontal bar).

## Stack

`Databricks` · `PySpark` · `Spark SQL` · `Delta Lake` · `Unity Catalog` ·
`Databricks AI/BI` · medallion / lakehouse architecture

---
---

# Stage 2 — WikiBrain: the Agent's Knowledge Layer

A knowledge engine that turns a plain folder of Markdown notes into a searchable,
graph-linked knowledge base for the **Hermes** AI agent. This is where the results and
reusable patterns from Stage 1 land: written as notes, indexed, and made retrievable so
the agent can pull the right fragment on demand instead of re-deriving it.

The system implements the **LLM-Wiki pattern**: the `.md` files remain the single
source of truth (human-readable, git-versionable, editable in Obsidian), and a
**SQLite** index sits *on top* of them, giving the agent a real search engine
instead of "open files and hope." The retrieval layer runs on local hardware — no
per-query API cost, and the knowledge base stays private.

## What it does

Hermes asks one tool a natural-language question and gets back only the relevant
passages, fused from four retrieval signals:

1. **Keyword** — SQLite **FTS5** full-text index (standard unicode tokenizer).
2. **Alias resolution** — a deterministic lookup table maps terms and slugs to the
   right page (filename → title → alias priority), so entities resolve without any
   model inference.
3. **Link graph** — the `[[wikilink]]` graph gives backlinks and related pages.
4. **Semantic** — 768-dimensional embeddings (`nomic-embed-text`) in **`sqlite-vec`**,
   cosine distance, for meaning-based matches beyond exact keywords.

Results from these signals are combined with **Reciprocal Rank Fusion (RRF)** in the
MCP server before being handed to the agent.

## A note on cost — stated honestly

WikiBrain is a **retrieval layer**, not a replacement for an LLM, and the honest
framing is stronger than the "$0 per query" one:

- **The retrieval layer itself is genuinely token-free.** FTS5 search, alias/graph
  resolution, vector search, and RRF fusion are pure local computation. No LLM tokens
  are spent to *find* the relevant content, and results are deterministic — the same
  vault and query always produce the same ranking, which is useful for testing.
- **The agent on top still costs tokens.** For Hermes to *use* the results, the LLM
  spends tokens deciding to call the tool, receiving the returned passages into
  context, and reasoning over them to answer.

So the value isn't "free queries" — it's that retrieval work is **pushed out of the
LLM**. Instead of stuffing the whole knowledge base into the context window
(expensive, and capped by context length), the model calls a tool that returns only
the relevant fragments. This **reduces** token usage and scales past what a context
window can hold. That's the accurate claim, and it's a good one.

## Architecture

```
        Obsidian vault  (the .md files = the source of truth)
         │
         │  (indexed by)
         ▼
   ┌──────────────────────────────────────────────┐
   │  SQLite  (wiki-index/wiki.db)                 │
   │                                               │
   │   vault ──┬── vault_fts   (FTS5 keyword)      │
   │           └── vault_vec   (sqlite-vec 768d)   │
   │                                               │
   │   link / alias / link_suggestion  (graph)     │
   └──────────────────────────────────────────────┘
         │                          ▲
         │ (queries)                │ (embed queue)
         ▼                          │
   wiki_mcp.py  (FastMCP)     wikivec  (Rust worker)
   RRF fusion search          nomic-embed-text → vectors
         │
         ▼
      Hermes agent  (via MCP tool call)
```

Obsidian and SQLite never talk to each other — they both talk to the files. Edit a
note in Obsidian, re-index, and the SQLite layer updates. If the database is ever
lost, it can be rebuilt from the files. No lock-in.

## Under the hood: database schema

The whole design turns on one idea: **the `.md` files are the truth; two search
indexes are kept in sync with them automatically.** SQLite triggers do the syncing.

### Core tables

| Table | Purpose |
|-------|---------|
| `vault` | Mirror of the source notes: `path` (PK), `content_text`, `size`. |
| `vault_fts` | FTS5 virtual table over `vault.content_text` — the keyword index. |
| `vault_vec` | `vec0` virtual table, `embedding float[768]`, cosine distance — the semantic index. |
| `link` | Wikilink graph: `src_path → dst_slug`, with `dst_path` (NULL if the link is broken) and `kind` (`explicit` \| `auto`). |
| `alias` | Term → page resolution: `term`, `target_path`, `slug`, `kind` (`filename` \| `title` \| `alias` \| `manual`). |
| `link_suggestion` | Auto-detected linking candidates with a `status` workflow (`pending` → `approved`/`rejected` → `applied`). |
| `to_embed` / `embedded` | The async embedding queue and its done-set (see below). |
| `deleted` | Tombstones for removed paths, so downstream consumers can reconcile. |
| `meta` / `run_log` | Config key-value store and an operation log. |

Indexes: `idx_alias_term` on `alias(term)` and `idx_link_dst` on `link(dst_path)`
keep alias lookups and reverse-link (backlink) queries fast.

### The clever part: synchronous FTS, asynchronous embeddings

Six triggers on `vault` keep everything consistent — but they treat the two indexes
differently, on purpose.

**FTS5 is synced inline** (it's cheap — just tokenization):

- `vault_ai` — after INSERT → add the row to `vault_fts`
- `vault_ad` — after DELETE → remove it from `vault_fts`
- `vault_au` — after UPDATE → delete + re-insert in `vault_fts`

**Embeddings are queued, not computed in-trigger** — because turning text into a
768-d vector requires the neural embedding model, which can't (and shouldn't) run
inside a SQLite trigger:

- `vault_embed_insert` — after INSERT → enqueue the row in `to_embed`
- `vault_embed_update` — after UPDATE **when `content_text` actually changed** →
  re-enqueue in `to_embed` and clear its `embedded` marker
- `vault_embed_delete` — after DELETE → remove from `embedded` and `to_embed`, and
  write a tombstone into `deleted`

The **`wikivec`** Rust worker then drains the `to_embed` queue out-of-band: it reads
the queued rows, computes embeddings with `nomic-embed-text` (via local Ollama),
writes them into `vault_vec`, and marks them in `embedded`. The
`WHEN old.content_text IS NOT new.content_text` guard means a save that doesn't change
the body never triggers a pointless re-embed.

The result: keyword search is always instantly consistent, expensive embedding work
happens asynchronously in a separate process, and neither ever blocks a note edit.

## Worked example: adding a model and searching

> Demo subject: the **Ferrari SF90 Stradale** (a real plug-in-hybrid V8 model).
> *Note: the earlier draft of this demo used a model called "F90" — Ferrari has no
> F90; the real car is the SF90. Use a real model here so the demo holds up to a
> domain reviewer.*

**1. Add a note** — create `entities/sf90-stradale.md` with YAML frontmatter
(title, aliases, tags) and the body text.

**2. Run the indexing pipeline** — sync mirrors the file into `vault`, the alias
indexer registers its title/aliases, and the link extractor auto-detects references
to `[[ferrari]]`. The new row is queued for embedding.

**3. Embed via the local Rust worker** — `wikivec` drains the queue, computes the
768-dim vector locally in ~39 ms (no API call), and stores it in `vault_vec`.

**4. Hybrid search** — a query like `"ferrari sf90"` runs FTS5 + alias + graph +
vector search and fuses them with RRF. The SF90 note ranks first, backed by all four
signals: exact title match (FTS5), unambiguous filename alias, inbound link from
`[[ferrari]]` (a hub page), and top semantic similarity (~0.72 cosine). The parent
`ferrari.md` and related models follow.

The retrieval steps above consume **zero LLM tokens** — they are local SQLite + Rust
computation. Hermes then spends tokens only to consume and reason over the returned
passages.

> The exact counts and latencies depend on your current vault; paste fresh
> `wikivec search` / `wikibrain status` output here if you want reproducible numbers
> a reviewer can verify against the live DB.

## Components

| Component | Role |
|-----------|------|
| **Obsidian vault** | Human interface + source of truth (`.md` files). |
| **SQLite (`wiki.db`)** | FTS5 + sqlite-vec indexes and the link/alias graph. |
| **`wikivec`** (Rust) | Embedding + search worker: drains `to_embed`, calls `nomic-embed-text`, writes vectors, runs vector search. |
| **`wiki_mcp.py`** (Python, FastMCP) | MCP server exposing the wiki to Hermes; runs the searches and fuses them with RRF. |
| **Hermes agent** | Consumes the wiki as an MCP tool. |

Built in three milestones: **(1)** FTS5 keyword search → **(2)** semantic search via
the Rust embedding worker + `sqlite-vec` → **(3)** the FastMCP server with RRF fusion.

## Design choices

- **Files as truth, DB as index.** Keeps notes portable, versionable, and rebuildable
  — the same principle industrial knowledge systems (and Google's OKF format) follow.
- **Hybrid retrieval.** Keyword and alias search catch exact terms; semantic search
  catches paraphrases; graph search finds related concepts; RRF fuses the ranked lists
  without hand-tuning score weights across different scales.
- **Run locally where it pays off.** Retrieval has no per-query API cost and keeps the
  knowledge base private; cloud services (like Databricks in Stage 1) are used where
  they're the better tool.
- **Trigger-driven consistency.** Edits propagate to both indexes automatically, with
  expensive embedding work isolated in an async queue.

## Stack

`SQLite` · `FTS5` · `sqlite-vec` · `nomic-embed-text` (768-d) · `Ollama` · `Rust` ·
`Python` · `FastMCP` · `MCP` · `Obsidian` · `Hermes Agent`
