# LiveTwin Dashboard — Build Instructions

> Combined: user design requirements + system architect requirements.
> This file governs every decision made while building `dashboard.html` and `api_server.py`.

---

## 1. What We Are Building

**Not a visualisation. Not a PPT. Not a Streamlit app.**

This is the operational control face of LiveTwin — the interface through which plant operators, maintenance supervisors, and engineers interact with the 4-agent AI system running on Databricks. Every pixel on screen corresponds to a real system output. If the backend doesn't produce it, it doesn't appear on screen.

Think: industrial SCADA control room software, not a demo slideshow.

---

## 2. Design Rules (User Requirements — Non-Negotiable)

### 2.1 Design System Source
Reference: `/Users/saivinay/hackbricks_livetwin/html_ref/dashboard.html`

### 2.2 Color System (copy exactly)
```css
:root {
  --neu-bg:            #e5e7eb;
  --neu-shadow-light:  rgba(255,255,255,0.85);
  --neu-shadow-dark:   rgb(153,161,175);
  --neu-inset:         rgb(209,213,220);
  --text-primary:      #1f2937;
  --text-secondary:    #374151;
  --text-muted:        #6b7280;
  --accent:            #0369a1;
  --accent-green:      #10b981;
}
```
Every surface = `var(--neu-bg)`. No dark surfaces. No other background colors.

### 2.3 Neumorphic Component System (copy exactly)

| Component | Shadow Rule |
|---|---|
| `.glass-panel` | `-10px -10px 20px light, 10px 10px 20px dark, inset -10px -10px 20px inset` |
| `.kpi-card` | Same as glass-panel. Radius 20px. |
| `.kpi-card .card-icon` | `-2px -2px 4px light, 2px 2px 4px dark`. Rounded 50%. |
| `.kpi-card .card-percent` | Same as icon. Rounded 100px. |
| `.kpi-card .card-range` (progress trough) | `inset -2px -2px 4px light, inset 2px 2px 4px dark` |
| `header` | `0 4px 12px dark, 0 -2px 6px light` (bottom shadow only) |
| `aside` | `4px 0 12px dark, -2px 0 6px light` (right shadow only) |
| `.sidebar-link.active` | `inset -3px -3px 6px light, inset 3px 3px 6px dark` (pressed look) |
| `.neu-input` | `inset -3px -3px 6px light, inset 3px 3px 6px dark` |
| `.neu-btn` | `-3px -3px 6px light, 3px 3px 6px dark` |
| `.cm-cell` | `inset -3px -3px 6px light, inset 3px 3px 6px dark` |

### 2.4 Typography (copy exactly)
- Headlines: `Space Grotesk` (700, tracking -0.03em)
- Body: `Inter`
- Data / monospace: `JetBrains Mono`
- Table headers: `JetBrains Mono`, 10px, uppercase, `#6b7280`, letter-spacing 0.06em
- KPI value: `Space Grotesk`, 32px, 700, `#1f2937`
- KPI label: `Inter`, 11px, 700, uppercase, 0.08em tracking

### 2.5 Layout (copy exactly)
- Header: `fixed top-0 w-full h-16 z-50`
- Sidebar: `fixed left-0 top-16 w-20 h-full z-40`, icon + label layout, 5 nav items
- Main: `ml-20 mt-16 p-6`
- All panels: `glass-panel rounded-[20px]`

### 2.6 Table Style (`.vt` — copy exactly)
```css
.vt th  { 10px/16px pad, #6b7280, uppercase, 10px, mono }
.vt td  { 10px/16px pad, #374151, border-bottom rgba(0,0,0,0.04) }
.vt tr:hover td { background rgba(3,105,161,0.03) }
```

### 2.7 Live Data Row Flash (copy exactly)
```css
@keyframes rowFlash {
  0%   { background: rgba(34,197,94,0.35); border-left: 3px solid rgba(34,197,94,1.0); }
  50%  { background: rgba(34,197,94,0.15); border-left: 3px solid rgba(34,197,94,0.6); }
  100% { background: transparent;          border-left: 3px solid transparent; }
}
.row-new { animation: rowFlash 2s ease-out; }
```
Apply to every new row added to live tables.

### 2.8 SPA Routing (copy exactly)
```css
.view { display: none; }
.view.active { display: block; }
```
Nav clicks toggle `.active` on views. Active nav link gets `.active` class → `color: var(--accent)`, `border-bottom: 2px solid var(--accent)`.

### 2.9 Icons
Material Symbols Outlined only. No other icon libraries.

### 2.10 What to NOT do
- No dark mode toggle (always light neumorphic)
- No gradients as backgrounds
- No glow blobs (`.fixed.top-0.left-0.w-full.h-full { display: none }`)
- No Tailwind dark: classes
- No decorative elements that don't correspond to real data

---

## 3. Content — Five Views (LiveTwin Operational)

### View 1: Fleet Overview (`fleet-overview`) — default active view
**Purpose:** Real-time status of all 20 engines. First thing an operator sees.

**KPI Cards (4 cards, top row):**
1. **Engines Online** — count of engines with `health_status != 'failure_imminent'`. Icon: `sensors`. Blue fill.
2. **Active Alerts** — count of engines with `health_status IN ('critical','failure_imminent')`. Icon: `warning`. Red fill.
3. **Pending Orders** — count of `maintenance_orders WHERE status='pending'`. Icon: `build`. Yellow fill.
4. **Avg Fleet Health** — average `health_score` across all engines (0-100). Icon: `monitor_heart`. Green fill.

**Engine Grid (main content):** 20 cards in 4×5 grid (or responsive 5 cols). Each engine card:
- Engine number (large, mono)
- Health score (progress bar, color: green >80, yellow 50-80, orange 20-50, red <20)
- Health status badge (HEALTHY / DEGRADING / CRITICAL / FAILURE_IMMINENT)
- RUL hours remaining
- Last anomaly timestamp
- Click → navigates to Engine Detail view for that engine

**Alert Banner:** Fixed below KPI row. Visible only when `count(failure_imminent) > 0`. Red background, pulsing dot, "ENGINE XX — FAILURE IMMINENT — RUL: X.Xh" text.

**Live stream indicator:** Top-right of table/grid, green pulsing dot + "LIVE" text + row counter.

**Data source:** GET `/api/fleet` → polls every 3s.

---

### View 2: Engine Detail (`engine-detail`)
**Purpose:** Deep drill-down on a single engine. Opened by clicking any engine card in Fleet Overview, or by selecting engine from dropdown.

**Header row:** Engine number, plant assignment (A or B), current health status badge, RUL with confidence bounds.

**Sensor Readings Panel (left 2/3):** Live scrolling table of recent sensor readings.
Columns: `cycle | s2 (LPC temp) | s4 (LPT temp) | s7 (HPC pressure) | s11 | s12 | anomaly_flag`
Anomaly rows highlighted red. New rows get `rowFlash`. Last 50 readings shown.

**Health Trend Panel (right 1/3):** Simple sparkline showing `health_score` over last 100 cycles. SVG inline chart, no external chart lib.

**ML Predictions Panel (below):** Three `.cm-cell` components:
- `RUL: 23.4h` (large value, mono)
- `RUL CI: [18.2 — 28.6]` (confidence interval)
- `Failure Mode: bearing_degradation` (classifier output)
- `Confidence: 87%` (classifier confidence)

**Similar Incidents Panel:** `.vt` table. Columns: `Incident ID | Failure Mode | Similarity | Resolution | Cost`.
Populated from Vector Search results for this engine.

**Data source:** GET `/api/engine/{id}` → polls every 5s when view is active.

---

### View 3: Agent Chain (`agent-chain`)
**Purpose:** Show the 4-agent pipeline executing in real time. This is the "brain" view.

**Agent Pipeline Timeline:** Horizontal chain showing: Monitor → Diagnosis → Plan → Guardian.
Each agent shown as a `.kpi-card`-style box with:
- Agent name + icon
- Status badge: IDLE / RUNNING / COMPLETE / BLOCKED
- Last execution time (timestamp)
- Latency (ms)
- Last output summary (1-2 lines of text)

Monitor: shows anomaly count detected
Diagnosis: shows RUL + failure mode from last run
Plan: shows maintenance window + estimated cost
Guardian: shows APPROVED/BLOCKED verdict + branch_id if approved

**Live Decision Log (below pipeline):** `.vt` table of `agent_decisions` — last 20 entries.
Columns: `Timestamp | Agent | Asset | Action | Verdict | Duration`
New rows get `rowFlash`.
BLOCKED rows: `color: #dc2626`. APPROVED rows: `color: #10b981`.

**Trigger Demo Button:** `.neu-btn` at top right. "Trigger Engine 15 Demo" — calls `POST /api/demo/trigger`. Shows spinner while running.

**Data source:** GET `/api/agents/status` → polls every 2s.

---

### View 4: Maintenance Orders (`maintenance-orders`)
**Purpose:** All maintenance orders created by Guardian agent. The output record of the system.

**Filter Bar:** `.neu-input` for search (asset_id, failure_mode text search). Status filter pills: ALL / PENDING / IN_PROGRESS / COMPLETED / CANCELLED. Urgency filter: ALL / SCHEDULED / URGENT / CRITICAL.

**Orders Table (`.vt`):**
Columns: `Order ID | Engine | Failure Mode | Urgency | Status | Est. Cost | Window Start | Approved By | Created`
- CRITICAL rows: left border 3px `#dc2626`
- URGENT rows: left border 3px `#f59e0b`
- Costs in ₹ format (Indian Rupees)
- New rows: `rowFlash`

**Summary stats (below table):** Three `.cm-cell` stats:
- Total orders this month
- Total estimated cost (₹)
- Avg resolution time

**Data source:** GET `/api/orders` → polls every 10s.

---

### View 5: Governance (`governance`)
**Purpose:** Unity Catalog ABAC + OBO authorization audit. Guardian branch log. Shows that the system has proper controls.

**OBO Authorization Panel (top left):**
Current user context: `user_id`, `groups`, `permissions`. Shows what this user can and cannot see.
Plant A users see only engines 1-10. Plant B users see only 11-20. This panel shows the active filter.

**Guardian Branch Log (top right):**
Table of Lakebase branch operations: `Branch ID | Asset | Action (CREATE/MERGE/DROP) | Outcome | Timestamp`
Outcome column: MERGED (green), DROPPED (red), IN_PROGRESS (yellow).

**Cost Authorization Panel (bottom left):** `.cm-cell` grid:
- Operator limit: ₹10,000 (from config)
- Supervisor limit: ₹50,000
- Admin limit: unlimited
- Current user level (highlighted)

**Guardian Decisions Audit (bottom right):** Last 10 Guardian decisions.
Each row: engine, verdict, reason (if BLOCKED), cost, user.
BLOCKED in red. APPROVED in green.

**Data source:** GET `/api/governance` → polls every 30s.

---

## 4. Header Content (LiveTwin-specific)

```
[LiveTwin]  Fleet Overview | Engine Detail | Agent Chain | Maintenance Orders | Governance
                                                          [● LIVE]  [MONITOR ACTIVE]  [settings] [person]
```

- Title: `LIVETWIN` in `var(--accent)`, `Space Grotesk`, 700, uppercase
- Status dot: green pulsing = Monitor agent running
- Status label: `MONITOR ACTIVE` or `AGENTS RUNNING`
- Right badges: `MLflow Active`, `Lakebase Connected`

---

## 5. Sidebar Content

5 icons matching the 5 views:

| Icon (Material) | Label | View |
|---|---|---|
| `grid_view` | Fleet | fleet-overview |
| `memory` | Engine | engine-detail |
| `account_tree` | Agents | agent-chain |
| `build` | Orders | maintenance-orders |
| `shield` | Govern | governance |

Bottom of sidebar: small `PLANT A` or `PLANT B` badge showing current user's plant scope.

---

## 6. Backend API Contract

File: `src/08_dashboard/api_server.py` — FastAPI, runs as Databricks Job or locally.

### Endpoints Required

```
GET  /api/fleet             → fleet overview data (all 20 engines)
GET  /api/engine/{id}       → single engine detail + sensor readings + ML predictions
GET  /api/agents/status     → 4 agent statuses + last decision log (20 rows)
GET  /api/orders            → maintenance orders with filter params
GET  /api/governance        → OBO context + branch log + cost limits + audit
POST /api/demo/trigger      → trigger Engine 15 demo chain
GET  /health                → API health check
```

### Data Sources

| Endpoint | Primary Source | Fallback |
|---|---|---|
| `/api/fleet` | Lakebase `assets` + `gold_engine_health` view | Delta `livetwin.processed.gold_engine_health` |
| `/api/engine/{id}` | Lakebase `sensor_readings` (last 50) + ML endpoints | Delta silver table |
| `/api/agents/status` | Lakebase `agent_decisions` (last 20) + `agent_inflight` | Delta `livetwin.operational.agent_decisions` |
| `/api/orders` | Lakebase `maintenance_orders` | Delta `livetwin.operational.maintenance_orders` |
| `/api/governance` | Lakebase `agent_decisions` + `config` + OBO context | — |

### CORS
Enable all origins for hackathon demo. Dashboard served as static file from same FastAPI app.

### Response shapes: documented inline in `api_server.py`.

---

## 7. Technical Architecture

```
dashboard.html          ← Single HTML file, SPA, no build step
    │
    │  fetch() polling (3s/5s/10s/30s per view)
    ▼
api_server.py           ← FastAPI, port 8000, runs on Databricks driver or locally
    │
    ├── psycopg2         → Lakebase (PostgreSQL protocol, sub-10ms reads)
    ├── databricks-sdk   → Delta tables (fallback + orders)
    └── mlflow           → ML endpoint calls for real-time predictions
```

No React. No bundler. No build step. Plain HTML + vanilla JS + Tailwind CDN.

---

## 8. Product Rules

1. **No mock data in the HTML file.** All values come from the API. On load, show `—` until first poll completes.
2. **Error state.** If API is unreachable, show the toast: `#api-error` div (bottom-right), `"⚠ API unreachable — showing cached data"`.
3. **Loading states.** KPI cards show `—` while loading. Table shows single row "Loading data..." in mono.
4. **Polling.** Each view only polls when it's the active view (check `document.getElementById('view-X').classList.contains('active')`).
5. **No navigation to external URLs.** All links internal to SPA.
6. **Engine cards are clickable.** Click → `navigateTo('engine-detail', engineId)` → loads that engine's data.
7. **Demo button feedback.** On click: disable button, show `spinner` + "Running chain...", re-enable when POST returns.
8. **All monetary values:** ₹ (Indian Rupees). Format: `₹1,23,456` (Indian comma style) or `₹1.2L` abbreviated.
9. **Status badges:** pill with color. HEALTHY=green, DEGRADING=yellow, CRITICAL=orange, FAILURE_IMMINENT=red.
10. **Timestamps:** `DD Mon HH:MM:SS` format. JetBrains Mono.

---

## 9. File Output

| File | Purpose |
|---|---|
| `src/08_dashboard/dashboard.html` | Complete SPA — the product face |
| `src/08_dashboard/api_server.py` | FastAPI backend — data bridge |
| `src/08_dashboard/run_dashboard.py` | Databricks notebook to start the FastAPI server |

---

## 10. Demo Integration

The dashboard must support the 5-scene demo from `docs/07_demo_script.md`:

- **Scene 0:** Open dashboard → all 20 engines green → Avg Fleet Health KPI shows ~82
- **Scene 1:** Zerobus stream starts → Engine 15 card turns red → Alert Banner appears → rowFlash on engine grid update
- **Scene 2:** Agent Chain view → pipeline animates → Diagnosis shows `RUL: 23.4h, bearing_degradation 87%`
- **Scene 3:** Guardian card in Agent Chain shows `APPROVED`, branch_id appears, order appears in Maintenance Orders view
- **Scene 4:** Governance view → OBO context, Guardian branch log shows MERGED

The "Trigger Engine 15 Demo" button in Agent Chain view is the single button that starts this sequence.
