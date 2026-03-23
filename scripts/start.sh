#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# LiveTwin — Single-command system starter
#
# Usage:
#   ./scripts/start.sh                        # local dev mode (auto-detects)
#   ./scripts/start.sh --local                # force local mode
#   ./scripts/start.sh --databricks           # deploy to Databricks
#   ./scripts/start.sh --databricks --bootstrap  # run full setup workflow first
#
# Local mode requires:
#   LAKEBASE_HOST, LAKEBASE_PASSWORD in env or .env file
#
# Databricks mode requires:
#   DATABRICKS_HOST, DATABRICKS_TOKEN in env or ~/.databrickscfg [DEFAULT]
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DASHBOARD_DIR="$REPO_ROOT/src/08_dashboard"

# ── Color output ──────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[LiveTwin]${NC} $*"; }
success() { echo -e "${GREEN}[✓]${NC} $*"; }
warn()    { echo -e "${YELLOW}[⚠]${NC} $*"; }
error()   { echo -e "${RED}[✗]${NC} $*"; exit 1; }

# ── Parse args ────────────────────────────────────────────────────────────────
MODE="auto"
BOOTSTRAP=false
for arg in "$@"; do
  case $arg in
    --local)       MODE="local"      ;;
    --databricks)  MODE="databricks" ;;
    --bootstrap)   BOOTSTRAP=true    ;;
    --help|-h)
      grep '^#' "$0" | head -20 | sed 's/^# \?//'
      exit 0
      ;;
  esac
done

# ── Load .env if present ──────────────────────────────────────────────────────
if [[ -f "$REPO_ROOT/.env" ]]; then
  info "Loading .env"
  set -a; source "$REPO_ROOT/.env"; set +a
fi

# ── Auto-detect mode ──────────────────────────────────────────────────────────
if [[ "$MODE" == "auto" ]]; then
  if [[ -n "${DATABRICKS_HOST:-}" && -n "${DATABRICKS_TOKEN:-}" ]]; then
    MODE="databricks"
    info "Auto-detected Databricks credentials — using Databricks mode"
  else
    MODE="local"
    info "No Databricks credentials found — using local mode"
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# LOCAL MODE
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$MODE" == "local" ]]; then
  info "Starting LiveTwin in LOCAL mode"

  # Validate required env vars
  if [[ -z "${LAKEBASE_HOST:-}" ]]; then
    warn "LAKEBASE_HOST not set. API will run without database (demo/mock data only)."
    warn "Set LAKEBASE_HOST and LAKEBASE_PASSWORD in .env or environment to connect."
  fi

  # Install Python deps
  if ! python3 -c "import fastapi" 2>/dev/null; then
    info "Installing Python dependencies..."
    pip install -q -r "$DASHBOARD_DIR/requirements.txt"
  fi

  # Export env
  export LAKEBASE_HOST="${LAKEBASE_HOST:-}"
  export LAKEBASE_PASSWORD="${LAKEBASE_PASSWORD:-}"
  export LAKEBASE_USER="${LAKEBASE_USER:-token}"
  export LAKEBASE_DB="${LAKEBASE_DB:-livetwin}"
  export PORT="${PORT:-8000}"

  success "LiveTwin API server starting on port $PORT"
  echo ""
  echo -e "  ${GREEN}Dashboard URL:${NC} http://localhost:$PORT"
  echo -e "  ${GREEN}API health:${NC}    http://localhost:$PORT/health"
  echo ""
  echo "  Press Ctrl+C to stop"
  echo ""

  cd "$DASHBOARD_DIR"
  exec python3 -m uvicorn api_server:app --host 0.0.0.0 --port "$PORT" --reload
fi

# ─────────────────────────────────────────────────────────────────────────────
# DATABRICKS MODE
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$MODE" == "databricks" ]]; then
  info "Starting LiveTwin in DATABRICKS mode"

  # Validate Databricks CLI / env
  if ! command -v databricks &>/dev/null; then
    error "Databricks CLI not found. Install: pip install databricks-cli  OR  brew install databricks"
  fi

  DBHOST="${DATABRICKS_HOST:-}"
  DBTOKEN="${DATABRICKS_TOKEN:-}"

  if [[ -z "$DBHOST" || -z "$DBTOKEN" ]]; then
    # Try reading from ~/.databrickscfg
    if [[ -f "$HOME/.databrickscfg" ]]; then
      DBHOST=$(awk '/\[DEFAULT\]/,/^$/{if(/^host/){print $3}}' "$HOME/.databrickscfg" | head -1)
      DBTOKEN=$(awk '/\[DEFAULT\]/,/^$/{if(/^token/){print $3}}' "$HOME/.databrickscfg" | head -1)
    fi
  fi

  [[ -z "$DBHOST"  ]] && error "DATABRICKS_HOST not set and not found in ~/.databrickscfg"
  [[ -z "$DBTOKEN" ]] && error "DATABRICKS_TOKEN not set and not found in ~/.databrickscfg"

  REPO_PATH="${REPO_PATH:-/Workspace/Repos/livetwin}"
  info "Using workspace: $DBHOST"
  info "Repo path:       $REPO_PATH"

  # ── Bootstrap: run full setup workflow (one-time) ──────────────────────────
  if [[ "$BOOTSTRAP" == "true" ]]; then
    info "Running bootstrap workflow (this runs once — takes ~20-30 min)..."

    # Create the workflow via API
    WORKFLOW_JSON="$REPO_ROOT/src/09_workflow/databricks_workflow.json"
    if [[ ! -f "$WORKFLOW_JSON" ]]; then
      error "Workflow definition not found: $WORKFLOW_JSON"
    fi

    # Check if workflow already exists
    EXISTING_JOB=$(curl -s -H "Authorization: Bearer $DBTOKEN" \
      "$DBHOST/api/2.1/jobs/list?name=LiveTwin+—+Full+System+Bootstrap" | \
      python3 -c "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); print(jobs[0]['job_id'] if jobs else '')" 2>/dev/null || echo "")

    if [[ -n "$EXISTING_JOB" ]]; then
      info "Bootstrap workflow already exists (job_id=$EXISTING_JOB) — triggering run..."
      RUN_ID=$(curl -s -X POST -H "Authorization: Bearer $DBTOKEN" \
        -H "Content-Type: application/json" \
        "$DBHOST/api/2.1/jobs/run-now" \
        -d "{\"job_id\": $EXISTING_JOB}" | \
        python3 -c "import sys,json; print(json.load(sys.stdin).get('run_id',''))")
    else
      info "Creating bootstrap workflow..."
      JOB_RESP=$(curl -s -X POST -H "Authorization: Bearer $DBTOKEN" \
        -H "Content-Type: application/json" \
        "$DBHOST/api/2.1/jobs/create" \
        --data-binary "@$WORKFLOW_JSON")
      NEW_JOB_ID=$(echo "$JOB_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('job_id',''))")
      [[ -z "$NEW_JOB_ID" ]] && error "Failed to create workflow: $JOB_RESP"
      success "Bootstrap workflow created (job_id=$NEW_JOB_ID)"

      RUN_ID=$(curl -s -X POST -H "Authorization: Bearer $DBTOKEN" \
        -H "Content-Type: application/json" \
        "$DBHOST/api/2.1/jobs/run-now" \
        -d "{\"job_id\": $NEW_JOB_ID}" | \
        python3 -c "import sys,json; print(json.load(sys.stdin).get('run_id',''))")
    fi

    if [[ -n "$RUN_ID" ]]; then
      success "Bootstrap workflow running (run_id=$RUN_ID)"
      echo -e "  Monitor at: ${BLUE}$DBHOST/#job/$EXISTING_JOB/run/$RUN_ID${NC}"
      echo ""
      warn "Bootstrap takes 20-30 minutes. Re-run this script without --bootstrap after it completes."
      exit 0
    else
      error "Failed to start bootstrap run"
    fi
  fi

  # ── Start dashboard server as persistent job ───────────────────────────────
  info "Starting LiveTwin dashboard server on Databricks..."

  DASHBOARD_JOB_NAME="LiveTwin — Dashboard Server"
  EXISTING_DASH=$(curl -s -H "Authorization: Bearer $DBTOKEN" \
    "$DBHOST/api/2.1/jobs/list?name=${DASHBOARD_JOB_NAME// /+}" | \
    python3 -c "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); print(jobs[0]['job_id'] if jobs else '')" 2>/dev/null || echo "")

  DASH_JOB_DEF=$(cat <<EOF
{
  "name": "$DASHBOARD_JOB_NAME",
  "format": "SINGLE_TASK",
  "task": {
    "task_key": "dashboard",
    "notebook_task": {
      "notebook_path": "$REPO_PATH/src/08_dashboard/run_dashboard"
    },
    "new_cluster": {
      "spark_version": "15.4.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "num_workers": 0,
      "spark_conf": {}
    }
  },
  "max_concurrent_runs": 1,
  "tags": { "project": "livetwin", "role": "dashboard" }
}
EOF
)

  if [[ -n "$EXISTING_DASH" ]]; then
    info "Dashboard job exists (job_id=$EXISTING_DASH) — triggering run..."
    RUN=$(curl -s -X POST -H "Authorization: Bearer $DBTOKEN" \
      -H "Content-Type: application/json" \
      "$DBHOST/api/2.1/jobs/run-now" -d "{\"job_id\": $EXISTING_DASH}")
    RUN_ID=$(echo "$RUN" | python3 -c "import sys,json; print(json.load(sys.stdin).get('run_id',''))")
  else
    info "Creating dashboard server job..."
    CREATED=$(curl -s -X POST -H "Authorization: Bearer $DBTOKEN" \
      -H "Content-Type: application/json" \
      "$DBHOST/api/2.1/jobs/create" -d "$DASH_JOB_DEF")
    EXISTING_DASH=$(echo "$CREATED" | python3 -c "import sys,json; print(json.load(sys.stdin).get('job_id',''))")
    [[ -z "$EXISTING_DASH" ]] && error "Failed to create dashboard job: $CREATED"
    success "Dashboard job created (job_id=$EXISTING_DASH)"
    RUN=$(curl -s -X POST -H "Authorization: Bearer $DBTOKEN" \
      -H "Content-Type: application/json" \
      "$DBHOST/api/2.1/jobs/run-now" -d "{\"job_id\": $EXISTING_DASH}")
    RUN_ID=$(echo "$RUN" | python3 -c "import sys,json; print(json.load(sys.stdin).get('run_id',''))")
  fi

  echo ""
  success "LiveTwin Dashboard is starting on Databricks"
  echo ""
  echo -e "  ${GREEN}Job ID:${NC}       $EXISTING_DASH"
  echo -e "  ${GREEN}Run ID:${NC}       $RUN_ID"
  echo -e "  ${GREEN}Monitor:${NC}      $DBHOST/#job/$EXISTING_DASH"
  echo ""
  echo "  The dashboard URL will be printed in the job run output after ~30 seconds."
  echo "  Format: https://<workspace>/driver-proxy/o/<org>/<cluster>/8000/"
  echo ""

  # ── Start agent loop as a separate always-on job ───────────────────────────
  AGENT_JOB_NAME="LiveTwin — Agent Loop"
  EXISTING_AGENT=$(curl -s -H "Authorization: Bearer $DBTOKEN" \
    "$DBHOST/api/2.1/jobs/list?name=${AGENT_JOB_NAME// /+}" | \
    python3 -c "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); print(jobs[0]['job_id'] if jobs else '')" 2>/dev/null || echo "")

  if [[ -n "$EXISTING_AGENT" ]]; then
    info "Agent loop job already exists (job_id=$EXISTING_AGENT)"
  else
    AGENT_JOB_DEF=$(cat <<EOF
{
  "name": "$AGENT_JOB_NAME",
  "format": "SINGLE_TASK",
  "task": {
    "task_key": "agent-loop",
    "notebook_task": {
      "notebook_path": "$REPO_PATH/src/05_agents/agent_orchestrator"
    },
    "new_cluster": {
      "spark_version": "15.4.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "num_workers": 0,
      "spark_conf": {}
    }
  },
  "max_concurrent_runs": 1,
  "tags": { "project": "livetwin", "role": "agents" }
}
EOF
)
    CREATED=$(curl -s -X POST -H "Authorization: Bearer $DBTOKEN" \
      -H "Content-Type: application/json" \
      "$DBHOST/api/2.1/jobs/create" -d "$AGENT_JOB_DEF")
    EXISTING_AGENT=$(echo "$CREATED" | python3 -c "import sys,json; print(json.load(sys.stdin).get('job_id',''))")
    success "Agent loop job created (job_id=$EXISTING_AGENT)"
  fi

  # ── Start Zerobus producer as a scheduled job ──────────────────────────────
  PRODUCER_JOB_NAME="LiveTwin — Zerobus Producer"
  EXISTING_PROD=$(curl -s -H "Authorization: Bearer $DBTOKEN" \
    "$DBHOST/api/2.1/jobs/list?name=${PRODUCER_JOB_NAME// /+}" | \
    python3 -c "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); print(jobs[0]['job_id'] if jobs else '')" 2>/dev/null || echo "")

  if [[ -n "$EXISTING_PROD" ]]; then
    info "Zerobus producer job already exists (job_id=$EXISTING_PROD)"
  else
    PROD_JOB_DEF=$(cat <<EOF
{
  "name": "$PRODUCER_JOB_NAME",
  "format": "SINGLE_TASK",
  "task": {
    "task_key": "producer",
    "notebook_task": {
      "notebook_path": "$REPO_PATH/src/01_data/01_zerobus_producer"
    },
    "new_cluster": {
      "spark_version": "15.4.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "num_workers": 0
    }
  },
  "max_concurrent_runs": 1,
  "tags": { "project": "livetwin", "role": "producer" }
}
EOF
)
    CREATED=$(curl -s -X POST -H "Authorization: Bearer $DBTOKEN" \
      -H "Content-Type: application/json" \
      "$DBHOST/api/2.1/jobs/create" -d "$PROD_JOB_DEF")
    EXISTING_PROD=$(echo "$CREATED" | python3 -c "import sys,json; print(json.load(sys.stdin).get('job_id',''))")
    success "Zerobus producer job created (job_id=$EXISTING_PROD)"
  fi

  echo ""
  success "All LiveTwin jobs are configured"
  echo ""
  echo "  ┌─────────────────────────────────────────────────────────┐"
  echo "  │  LIVETWIN DATABRICKS DEPLOYMENT SUMMARY                 │"
  echo "  ├─────────────────────────────────────────────────────────┤"
  echo "  │  Dashboard Server:   Job $EXISTING_DASH                             │"
  echo "  │  Agent Loop:         Job $EXISTING_AGENT                             │"
  echo "  │  Zerobus Producer:   Job $EXISTING_PROD                             │"
  echo "  ├─────────────────────────────────────────────────────────┤"
  echo "  │  To start all jobs:                                      │"
  echo "  │    ./scripts/start.sh --databricks                      │"
  echo "  │                                                          │"
  echo "  │  To reset and re-bootstrap:                              │"
  echo "  │    ./scripts/start.sh --databricks --bootstrap          │"
  echo "  └─────────────────────────────────────────────────────────┘"
fi
