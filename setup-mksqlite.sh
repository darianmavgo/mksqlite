#!/usr/bin/env bash
# setup-mksqlite.sh
# Provisions all Cloudflare infrastructure for mksqlite.mavgo.com:
#   - GitHub repo (darianmavgo/mksqlite-web)
#   - Cloudflare Pages (marketing site)
#   - Cloudflare D1 database (mksqlite-db)
#   - Cloudflare R2 bucket  (mksqlite-jobs, 24h lifecycle)
#   - Cloudflare Worker     (mksqlite-api)
#   - DNS CNAME             (mksqlite.mavgo.com → pages.dev)
#
# Usage: bash setup-mksqlite.sh [--skip-github] [--skip-dns]

set -euo pipefail

# ─── CONFIG ──────────────────────────────────────────────────────────────────
ZONE_NAME="mavgo.com"
SUBDOMAIN="mksqlite"
FULL_DOMAIN="${SUBDOMAIN}.${ZONE_NAME}"

PAGES_PROJECT="mksqlite-web"
WORKER_NAME="mksqlite-api"
DB_NAME="mksqlite-db"
BUCKET_NAME="mksqlite-jobs"

REPO_NAME="mksqlite-web"
REPO_ORG="darianmavgo"

SKIP_GITHUB=false
SKIP_DNS=false

for arg in "$@"; do
  case $arg in
    --skip-github) SKIP_GITHUB=true ;;
    --skip-dns)    SKIP_DNS=true    ;;
  esac
done

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✅ $*${NC}"; }
warn() { echo -e "${YELLOW}⚠️  $*${NC}"; }
fail() { echo -e "${RED}❌ $*${NC}" >&2; exit 1; }
step() { echo -e "\n${YELLOW}▶ $*${NC}"; }
skip() { echo -e "${CYAN}⏭  $* (already done — skipping)${NC}"; }

# ─── STATE FILE ──────────────────────────────────────────────────────────────
# Tracks which steps have completed so re-runs skip finished work.
STATE_FILE="$(pwd)/.mksqlite-state"
touch "${STATE_FILE}"

step_done() { grep -qxF "$1" "${STATE_FILE}" 2>/dev/null; }
mark_done() { echo "$1" >> "${STATE_FILE}"; }
state_get() { grep "^${1}=" "${STATE_FILE}" 2>/dev/null | cut -d= -f2- || echo ""; }
state_set() { 
  # Remove old value then append new
  grep -v "^${1}=" "${STATE_FILE}" > "${STATE_FILE}.tmp" 2>/dev/null || true
  echo "${1}=${2}" >> "${STATE_FILE}.tmp"
  mv "${STATE_FILE}.tmp" "${STATE_FILE}"
}

# ─── 1. DEPENDENCY CHECK ─────────────────────────────────────────────────────
step "Checking required CLI tools..."
for cmd in wrangler gh jq curl git; do
  command -v "$cmd" &>/dev/null || fail "$cmd is required but not installed."
done

if ! wrangler whoami &>/dev/null; then
  fail "Not logged into Cloudflare. Run: wrangler login"
fi

if ! $SKIP_GITHUB && ! gh auth status &>/dev/null; then
  fail "Not logged into GitHub CLI. Run: gh auth login"
fi
ok "All checks passed."

# ─── PROGRESS SUMMARY ────────────────────────────────────────────────────────
ALL_STEPS=("secrets" "site_files" "d1_created" "d1_schema" "r2_created" "r2_lifecycle" "worker_deployed" "worker_secrets" "pages_deployed" "dns" "github")
DONE_COUNT=0
for s in "${ALL_STEPS[@]}"; do step_done "$s" && ((DONE_COUNT++)) || true; done
TOTAL=${#ALL_STEPS[@]}

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}  📋  Setup Progress: ${DONE_COUNT}/${TOTAL} steps complete${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
for s in "${ALL_STEPS[@]}"; do
  if step_done "$s"; then
    echo -e "  ${GREEN}✅ ${s}${NC}"
  else
    echo -e "  ${RED}○  ${s}${NC}"
  fi
done
echo ""

if [[ $DONE_COUNT -eq $TOTAL ]]; then
  echo -e "${GREEN}🎉 All steps already complete! Nothing to do.${NC}"
  echo "   To force a full re-run: rm ${STATE_FILE} && ./setup-mksqlite.sh"
  exit 0
fi

# ─── SECRETS: each saved individually to disk immediately after entry ─────────
echo ""
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}  🔑  Secret & Token Setup${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# 1. Cloudflare API Token
_saved_cf=$(state_get "CF_DNS_TOKEN")
if [[ -n "${CF_DNS_TOKEN:-}" ]]; then
  ok "[1/3] CF_DNS_TOKEN in environment."; state_set "CF_DNS_TOKEN" "${CF_DNS_TOKEN}"
elif [[ -n "$_saved_cf" ]]; then
  CF_DNS_TOKEN="$_saved_cf"; ok "[1/3] CF_DNS_TOKEN loaded from state."
else
  echo -e "${YELLOW}[1/3] Cloudflare API Token${NC}"
  echo "      Needed to create the DNS CNAME for mksqlite.mavgo.com."
  echo "      Create at: https://dash.cloudflare.com/profile/api-tokens"
  echo "      Template: Edit zone DNS → mavgo.com"
  echo ""
  read -rp "      Paste CF API Token (Enter=skip DNS): " CF_DNS_TOKEN
  CF_DNS_TOKEN="${CF_DNS_TOKEN// /}"
  if [[ -z "$CF_DNS_TOKEN" ]]; then warn "Skipping DNS."; SKIP_DNS=true; else ok "CF_DNS_TOKEN set."; fi
  state_set "CF_DNS_TOKEN" "${CF_DNS_TOKEN:-}"  # written to disk NOW
fi

# 2. Lemon Squeezy Webhook Secret
_saved_ls_secret=$(state_get "LS_WEBHOOK_SECRET")
if [[ -n "${LS_WEBHOOK_SECRET:-}" ]]; then
  ok "[2/3] LS_WEBHOOK_SECRET in environment."; state_set "LS_WEBHOOK_SECRET" "${LS_WEBHOOK_SECRET}"
elif [[ -n "$_saved_ls_secret" ]]; then
  LS_WEBHOOK_SECRET="$_saved_ls_secret"; export LS_WEBHOOK_SECRET; ok "[2/3] LS_WEBHOOK_SECRET loaded from state."
else
  echo -e "${YELLOW}[2/3] Lemon Squeezy Webhook Signing Secret${NC}"
  echo "      https://app.lemonsqueezy.com/settings/webhooks"
  echo "      Add webhook → URL: https://${FULL_DOMAIN}/api/v1/webhooks/lemonsqueezy"
  echo "      Events: order_created, subscription_created, subscription_cancelled"
  echo ""
  read -rsp "      Paste Signing Secret (hidden, Enter=skip): " LS_WEBHOOK_SECRET; echo ""
  LS_WEBHOOK_SECRET="${LS_WEBHOOK_SECRET// /}"
  if [[ -z "$LS_WEBHOOK_SECRET" ]]; then warn "Skipping LS secret."; else export LS_WEBHOOK_SECRET; ok "LS_WEBHOOK_SECRET set."; fi
  state_set "LS_WEBHOOK_SECRET" "${LS_WEBHOOK_SECRET:-}"  # written to disk NOW
fi

# 3. Lemon Squeezy API Key — skipped at setup time (JWT too long for terminal input buffer)
#    Add after setup: wrangler secret put LS_API_KEY --name mksqlite-api
#    Key URL: https://app.lemonsqueezy.com/settings/api
ok "[3/3] LS_API_KEY — add after setup: wrangler secret put LS_API_KEY --name ${WORKER_NAME}"


mark_done "secrets"
echo ""
echo -e "${GREEN}  Secrets done. Starting infrastructure...${NC}"
echo ""

# Resolve the Cloudflare Zone ID for mavgo.com
step "Resolving Zone ID for ${ZONE_NAME}..."
ZONE_ID=""
if [[ -n "${CF_DNS_TOKEN:-}" ]]; then
  ZONE_ID=$(curl -s -X GET "https://api.cloudflare.com/client/v4/zones?name=${ZONE_NAME}" \
    -H "Authorization: Bearer ${CF_DNS_TOKEN}" \
    -H "Content-Type: application/json" | jq -r '.result[0].id // empty')
fi

if [[ -z "$ZONE_ID" || "$ZONE_ID" == "null" ]]; then
  warn "Could not resolve Zone ID — DNS will be skipped."
  warn "Manual DNS: CNAME ${SUBDOMAIN} → ${PAGES_PROJECT}.pages.dev in the Cloudflare dashboard."
  warn "Direct link: https://dash.cloudflare.com/?to=/:account/mavgo.com/dns/records"
  SKIP_DNS=true
else
  ok "Zone ID: ${ZONE_ID}"
fi

# ─── 2. SCAFFOLD MARKETING SITE FILES ────────────────────────────────────────
if step_done "site_files"; then
  skip "Marketing site files"
  SITE_DIR="$(pwd)/mksqlite-web"
else
  step "Scaffolding marketing site files..."
  SITE_DIR="$(pwd)/mksqlite-web"
  mkdir -p "${SITE_DIR}/api-worker"

# --- index.html ---
cat > "${SITE_DIR}/index.html" << 'HTMLEOF'
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>mksqlite — Any File. Pure SQLite.</title>
  <meta name="description" content="Convert CSV, Excel, JSON, HTML, Markdown and more into SQLite databases via API or CLI.">
  <link href="https://fonts.googleapis.com/css2?family=Bitter:wght@400;600;700&family=Montserrat:wght@500;600;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="style.css">
</head>
<body>
  <nav class="topnav">
    <div class="logo">🗄 mksqlite</div>
    <div class="navlinks">
      <a href="#features">Features</a>
      <a href="#pricing">Pricing</a>
      <a href="#docs">Docs</a>
      <a href="https://github.com/darianmavgo/mksqlite" target="_blank">GitHub ↗</a>
    </div>
  </nav>

  <header class="hero">
    <h1>Any file. One command.<br>Pure SQLite.</h1>
    <p>Convert CSV, Excel, JSON, HTML, Markdown, ZIP archives and entire directories into SQLite databases — via API or CLI.</p>
    <div class="hero-actions">
      <a href="#pricing" class="btn-primary">Get API Key</a>
      <a href="https://github.com/darianmavgo/mksqlite/releases" class="btn-secondary">Download CLI ↓</a>
    </div>
    <div class="hero-demo">
      <pre><code>$ mksqlite data.csv output.db
✓ Table "data" created with 1,482 rows

$ curl -X POST https://mksqlite.mavgo.com/api/v1/convert \
    -H "X-API-Key: mk_live_..." \
    -F "file=@data.csv" \
    -F "output=db"
→ { "job_id": "j_abc123", "status": "queued" }</code></pre>
    </div>
  </header>

  <main>
    <section id="features" class="card">
      <h2>Supported Formats</h2>
      <div class="format-grid">
        <div class="format-tile">📊 CSV</div>
        <div class="format-tile">📗 Excel (.xlsx/.xls)</div>
        <div class="format-tile">🌐 HTML tables</div>
        <div class="format-tile">📄 JSON</div>
        <div class="format-tile">📝 Markdown tables</div>
        <div class="format-tile">📃 Plain Text</div>
        <div class="format-tile">🗜 ZIP archives</div>
        <div class="format-tile">📁 Directories</div>
      </div>
    </section>

    <section id="pricing" class="card">
      <h2>Pricing</h2>
      <div class="pricing-grid">
        <div class="plan-card">
          <div class="plan-name">Free CLI</div>
          <div class="plan-price">$0</div>
          <ul>
            <li>Unlimited local conversions</li>
            <li>Open source on GitHub</li>
            <li>All formats supported</li>
          </ul>
          <a href="https://github.com/darianmavgo/mksqlite/releases" class="btn-secondary">Download</a>
        </div>
        <div class="plan-card featured">
          <div class="plan-name">Starter</div>
          <div class="plan-price">$9<span>/mo</span></div>
          <ul>
            <li>500 API conversions/mo</li>
            <li>Files up to 50 MB</li>
            <li>REST API access</li>
            <li>Email support</li>
          </ul>
          <a href="#" class="btn-primary">Get Started</a>
        </div>
        <div class="plan-card">
          <div class="plan-name">Pro</div>
          <div class="plan-price">$29<span>/mo</span></div>
          <ul>
            <li>5,000 API conversions/mo</li>
            <li>Files up to 500 MB</li>
            <li>Priority queue</li>
            <li>Webhook callbacks</li>
          </ul>
          <a href="#" class="btn-primary">Get Started</a>
        </div>
        <div class="plan-card">
          <div class="plan-name">Team</div>
          <div class="plan-price">$79<span>/mo</span></div>
          <ul>
            <li>25,000 API conversions/mo</li>
            <li>Files up to 2 GB</li>
            <li>Team API keys</li>
            <li>SLA uptime guarantee</li>
          </ul>
          <a href="#" class="btn-primary">Get Started</a>
        </div>
      </div>
    </section>

    <section id="docs" class="card">
      <h2>API Quick Reference</h2>
      <pre><code># Convert a file
POST https://mksqlite.mavgo.com/api/v1/convert
  Headers:  X-API-Key: mk_live_...
  Body:     multipart/form-data { file, output: "db"|"sql" }

# Check job status
GET  https://mksqlite.mavgo.com/api/v1/jobs/:id

# Download result
GET  https://mksqlite.mavgo.com/api/v1/jobs/:id/download</code></pre>
    </section>
  </main>

  <footer>
    <p>mksqlite · A <a href="https://mavgo.com">Mavgo</a> product · <a href="https://github.com/darianmavgo/mksqlite">Open Source</a></p>
  </footer>
</body>
</html>
HTMLEOF

# --- style.css ---
cat > "${SITE_DIR}/style.css" << 'CSSEOF'
:root {
  --bg: #F1F3F4; --surface: #fff; --surface2: #F8F9FA;
  --border: #E0E0E0; --primary: #007BBF; --accent: #185BA9;
  --text: #333; --muted: #5F6368; --radius: 8px;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Bitter', serif; background: var(--bg); color: var(--text); line-height: 1.6; }
.topnav { display: flex; align-items: center; justify-content: space-between; padding: 1rem 2rem;
  background: var(--primary); color: #fff; position: sticky; top: 0; z-index: 100; box-shadow: 0 2px 6px rgba(0,0,0,.15); }
.logo { font-family: 'Montserrat', sans-serif; font-size: 1.3rem; font-weight: 700; }
.navlinks a { color: rgba(255,255,255,.9); text-decoration: none; margin-left: 1.5rem;
  font-family: 'Montserrat', sans-serif; font-weight: 500; font-size: .95rem; }
.navlinks a:hover { color: #fff; }
.hero { background: #fff; padding: 5rem 2rem 4rem; text-align: center; border-bottom: 1px solid var(--border); }
.hero h1 { font-size: 2.8rem; color: var(--accent); margin-bottom: 1rem; line-height: 1.2; }
.hero p { color: var(--muted); font-size: 1.1rem; max-width: 600px; margin: 0 auto 2rem; }
.hero-actions { display: flex; gap: 1rem; justify-content: center; margin-bottom: 2.5rem; flex-wrap: wrap; }
.btn-primary { background: var(--primary); color: #fff; padding: .75rem 1.75rem; border-radius: 6px;
  text-decoration: none; font-family: 'Montserrat', sans-serif; font-weight: 600; transition: opacity .2s; }
.btn-primary:hover { opacity: .88; }
.btn-secondary { border: 2px solid var(--primary); color: var(--primary); padding: .75rem 1.75rem;
  border-radius: 6px; text-decoration: none; font-family: 'Montserrat', sans-serif; font-weight: 600; transition: background .2s; }
.btn-secondary:hover { background: #e8f0fe; }
.hero-demo { max-width: 680px; margin: 0 auto; background: #1e1e2e; border-radius: 10px;
  text-align: left; overflow: hidden; }
.hero-demo pre { padding: 1.5rem; overflow-x: auto; }
.hero-demo code { color: #cdd6f4; font-family: 'Menlo', monospace; font-size: .88rem; line-height: 1.7; }
main { max-width: 1100px; margin: 0 auto; padding: 2rem 1.5rem; }
.card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
  padding: 2rem; margin-bottom: 2rem; box-shadow: 0 1px 3px rgba(0,0,0,.05); }
.card h2 { font-size: 1.6rem; color: var(--accent); margin-bottom: 1.5rem; }
.format-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: .75rem; }
.format-tile { background: var(--surface2); border: 1px solid var(--border); border-radius: 8px;
  padding: 1rem; text-align: center; font-size: .95rem; }
.pricing-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 1rem; }
.plan-card { background: var(--surface2); border: 1px solid var(--border); border-radius: 8px; padding: 1.5rem; }
.plan-card.featured { border-color: var(--primary); border-width: 2px; background: #e8f4ff; }
.plan-name { font-family: 'Montserrat', sans-serif; font-weight: 700; color: var(--accent); margin-bottom: .25rem; }
.plan-price { font-size: 2rem; font-weight: 700; color: var(--primary); margin-bottom: 1rem; }
.plan-price span { font-size: 1rem; color: var(--muted); }
.plan-card ul { padding-left: 1.2rem; margin-bottom: 1.5rem; font-size: .92rem; color: var(--muted); }
.plan-card li { margin-bottom: .4rem; }
.plan-card .btn-primary, .plan-card .btn-secondary { display: block; text-align: center; }
pre code { display: block; background: #1e1e2e; color: #cdd6f4; padding: 1.5rem; border-radius: 8px;
  font-family: 'Menlo', monospace; font-size: .88rem; line-height: 1.7; overflow-x: auto; }
footer { text-align: center; padding: 2rem; color: var(--muted); font-size: .88rem;
  border-top: 1px solid var(--border); background: #fff; }
footer a { color: var(--primary); text-decoration: none; }
CSSEOF

  ok "Marketing site files written to ${SITE_DIR}/"
  mark_done "site_files"
fi

# ─── 3. D1 DATABASE ──────────────────────────────────────────────────────────
if step_done "d1_created"; then
  skip "D1 database creation"
  DB_ID=$(state_get "DB_ID")
else
  step "Creating Cloudflare D1 database: ${DB_NAME}..."
  DB_OUTPUT=$(wrangler d1 create "${DB_NAME}" 2>&1 || true)
  if echo "$DB_OUTPUT" | grep -q "database_id"; then
    DB_ID=$(echo "$DB_OUTPUT" | grep "database_id" | awk -F'"' '{print $4}')
    ok "D1 created. ID: ${DB_ID}"
  elif echo "$DB_OUTPUT" | grep -qi "already exists"; then
    warn "D1 database '${DB_NAME}' already exists — fetching ID."
    DB_ID=$(wrangler d1 list 2>/dev/null | grep "${DB_NAME}" | awk '{print $2}' || echo "EXISTING")
  else
    warn "D1 output was: ${DB_OUTPUT}"; DB_ID="UNKNOWN"
  fi
  state_set "DB_ID" "${DB_ID}"
  mark_done "d1_created"
fi

if step_done "d1_schema"; then
  skip "D1 schema"
else
  step "Applying D1 schema..."
  wrangler d1 execute "${DB_NAME}" --remote --command "
    CREATE TABLE IF NOT EXISTS api_keys (
      key_hash   TEXT PRIMARY KEY,
      tier       TEXT NOT NULL DEFAULT 'starter',
      email      TEXT,
      conversions_remaining INTEGER NOT NULL DEFAULT 500,
      reset_at   TEXT NOT NULL DEFAULT (datetime('now', '+1 month')),
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS jobs (
      id         TEXT PRIMARY KEY,
      key_hash   TEXT NOT NULL,
      status     TEXT NOT NULL DEFAULT 'queued',
      input_key  TEXT,
      output_key TEXT,
      error      TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS usage_log (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      key_hash   TEXT NOT NULL,
      job_id     TEXT NOT NULL,
      mb_size    REAL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
  " && ok "Schema applied." && mark_done "d1_schema" || warn "Schema apply failed — re-run to retry."
fi  # end d1_schema block

# ─── 4. R2 BUCKET ────────────────────────────────────────────────────────────
if step_done "r2_created"; then
  skip "R2 bucket creation"
else
  step "Creating Cloudflare R2 bucket: ${BUCKET_NAME}..."
  wrangler r2 bucket create "${BUCKET_NAME}" 2>&1 | grep -v "^$" || true
  mark_done "r2_created"
fi

if step_done "r2_lifecycle"; then
  skip "R2 lifecycle rule"
else
  step "Setting R2 lifecycle rule (24h auto-delete) on ${BUCKET_NAME}..."
  wrangler r2 bucket lifecycle add "${BUCKET_NAME}" "auto-delete-24h" \
    --expire-days 1 --force \
    && ok "R2 lifecycle rule set." && mark_done "r2_lifecycle" \
    || warn "Lifecycle rule failed — set manually at: https://dash.cloudflare.com/?to=/:account/r2/default/buckets/${BUCKET_NAME}/settings"
fi

# ─── 5. API WORKER ───────────────────────────────────────────────────────────
step "Writing Cloudflare API Worker files..."
mkdir -p "${SITE_DIR}/api-worker"

# wrangler.toml for the worker
cat > "${SITE_DIR}/api-worker/wrangler.toml" << TOMLEOF
name = "${WORKER_NAME}"
main = "worker.js"
compatibility_date = "2025-01-01"

[[d1_databases]]
binding = "DB"
database_name = "${DB_NAME}"
database_id = "${DB_ID}"

[[r2_buckets]]
binding = "JOBS"
bucket_name = "${BUCKET_NAME}"

[[routes]]
pattern = "${FULL_DOMAIN}/api/v1/*"
zone_name = "${ZONE_NAME}"

[vars]
DOMAIN = "${FULL_DOMAIN}"
TOMLEOF

# The Worker itself
cat > "${SITE_DIR}/api-worker/worker.js" << 'JSEOF'
const TIER_LIMITS = { starter: 500, pro: 5000, team: 25000 };

async function hashKey(key) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(key));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
}

async function validateKey(env, request) {
  const key = request.headers.get('X-API-Key') || '';
  if (!key.startsWith('mk_live_')) return null;
  const hash = await hashKey(key);
  const row = await env.DB.prepare(
    'SELECT * FROM api_keys WHERE key_hash = ? AND conversions_remaining > 0'
  ).bind(hash).first();
  return row || null;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^\/api\/v1/, '');
    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'X-API-Key, Content-Type',
    };

    if (request.method === 'OPTIONS') return new Response(null, { headers: cors });

    // ── Lemon Squeezy Webhook ──────────────────────────────────────────────
    if (path === '/webhooks/lemonsqueezy' && request.method === 'POST') {
      const body = await request.json();
      const event = request.headers.get('X-Event-Name') || '';

      if (event === 'order_created' || event === 'subscription_created') {
        const email = body?.data?.attributes?.user_email || 'unknown';
        const variantName = (body?.data?.attributes?.first_order_item?.variant_name || 'starter').toLowerCase();
        const tier = ['pro', 'team'].includes(variantName) ? variantName : 'starter';

        const rawKey = `mk_live_${crypto.randomUUID().replace(/-/g, '')}`;
        const hash = await hashKey(rawKey);
        const limit = TIER_LIMITS[tier];

        await env.DB.prepare(
          `INSERT OR REPLACE INTO api_keys (key_hash, tier, email, conversions_remaining)
           VALUES (?, ?, ?, ?)`
        ).bind(hash, tier, email, limit).run();

        // NOTE: In production, email the rawKey to the customer via your mail provider.
        // The raw key is only available here — store it in env vars for webhook secret.
        console.log(`New key for ${email} (${tier}): ${rawKey}`);

        return new Response(JSON.stringify({ ok: true }), { headers: { ...cors, 'Content-Type': 'application/json' } });
      }

      if (event === 'subscription_cancelled') {
        const email = body?.data?.attributes?.user_email || '';
        await env.DB.prepare(
          `UPDATE api_keys SET conversions_remaining = 0 WHERE email = ?`
        ).bind(email).run();
        return new Response(JSON.stringify({ ok: true }), { headers: { ...cors, 'Content-Type': 'application/json' } });
      }

      return new Response('ok', { headers: cors });
    }

    // ── All other routes require an API key ────────────────────────────────
    const keyRow = await validateKey(env, request);
    if (!keyRow) {
      return new Response(JSON.stringify({ error: 'Invalid or exhausted API key.' }), {
        status: 401, headers: { ...cors, 'Content-Type': 'application/json' },
      });
    }

    // ── POST /convert ──────────────────────────────────────────────────────
    if (path === '/convert' && request.method === 'POST') {
      const jobId = crypto.randomUUID();
      const keyHash = keyRow.key_hash;

      // Store the upload in R2 for the conversion worker to pick up
      const formData = await request.formData();
      const file = formData.get('file');
      const outputFormat = formData.get('output') || 'db';

      if (!file) {
        return new Response(JSON.stringify({ error: 'No file provided.' }), {
          status: 400, headers: { ...cors, 'Content-Type': 'application/json' },
        });
      }

      const inputKey = `jobs/${jobId}/input/${file.name}`;
      await env.JOBS.put(inputKey, file.stream(), {
        httpMetadata: { contentType: file.type },
        customMetadata: { output: outputFormat, jobId },
      });

      await env.DB.prepare(
        `INSERT INTO jobs (id, key_hash, status, input_key) VALUES (?, ?, 'queued', ?)`
      ).bind(jobId, keyHash, inputKey).run();

      await env.DB.prepare(
        `UPDATE api_keys SET conversions_remaining = conversions_remaining - 1 WHERE key_hash = ?`
      ).bind(keyHash).run();

      const pollUrl = `https://${env.DOMAIN}/api/v1/jobs/${jobId}`;
      return new Response(JSON.stringify({ job_id: jobId, poll_url: pollUrl, status: 'queued' }), {
        status: 202, headers: { ...cors, 'Content-Type': 'application/json' },
      });
    }

    // ── GET /jobs/:id ──────────────────────────────────────────────────────
    const jobMatch = path.match(/^\/jobs\/([^/]+)$/);
    if (jobMatch && request.method === 'GET') {
      const job = await env.DB.prepare('SELECT * FROM jobs WHERE id = ?').bind(jobMatch[1]).first();
      if (!job) return new Response(JSON.stringify({ error: 'Job not found.' }), {
        status: 404, headers: { ...cors, 'Content-Type': 'application/json' },
      });
      const downloadUrl = job.status === 'done'
        ? `https://${env.DOMAIN}/api/v1/jobs/${job.id}/download` : null;
      return new Response(JSON.stringify({ ...job, download_url: downloadUrl }), {
        headers: { ...cors, 'Content-Type': 'application/json' },
      });
    }

    // ── GET /jobs/:id/download ─────────────────────────────────────────────
    const dlMatch = path.match(/^\/jobs\/([^/]+)\/download$/);
    if (dlMatch && request.method === 'GET') {
      const job = await env.DB.prepare('SELECT * FROM jobs WHERE id = ?').bind(dlMatch[1]).first();
      if (!job || job.status !== 'done') return new Response(JSON.stringify({ error: 'Not ready.' }), {
        status: 404, headers: { ...cors, 'Content-Type': 'application/json' },
      });
      const obj = await env.JOBS.get(job.output_key);
      if (!obj) return new Response('Output file expired.', { status: 410, headers: cors });
      return new Response(obj.body, {
        headers: { ...cors, 'Content-Type': 'application/octet-stream',
          'Content-Disposition': `attachment; filename="output.${job.output_key.endsWith('.sql') ? 'sql' : 'db'}"` },
      });
    }

    return new Response(JSON.stringify({ error: 'Not found.' }), {
      status: 404, headers: { ...cors, 'Content-Type': 'application/json' },
    });
  },
};
JSEOF

ok "Worker files written to ${SITE_DIR}/api-worker/"

# ─── 6. DEPLOY PAGES ─────────────────────────────────────────────────────────
if step_done "pages_deployed"; then
  skip "Cloudflare Pages deployment"
else
  step "Deploying marketing site to Cloudflare Pages (${PAGES_PROJECT})..."
  wrangler pages project create "${PAGES_PROJECT}" --production-branch main 2>/dev/null || warn "Pages project may already exist — continuing."

  PAGES_UPLOAD_DIR=$(mktemp -d)
  cp "${SITE_DIR}/index.html" "${PAGES_UPLOAD_DIR}/"
  cp "${SITE_DIR}/style.css"  "${PAGES_UPLOAD_DIR}/"

  wrangler pages deploy "${PAGES_UPLOAD_DIR}" --project-name="${PAGES_PROJECT}" --branch=main
  rm -rf "${PAGES_UPLOAD_DIR}"
  ok "Pages deployed → https://${PAGES_PROJECT}.pages.dev"
  mark_done "pages_deployed"
fi

if step_done "worker_deployed"; then
  skip "API Worker deployment"
  [[ -z "${DB_ID:-}" ]] && DB_ID=$(state_get "DB_ID") || true
else
  step "Deploying API Worker (${WORKER_NAME})..."
  cd "${SITE_DIR}/api-worker"
  wrangler deploy
  cd - > /dev/null
  ok "Worker deployed → https://${FULL_DOMAIN}/api/v1/"
  mark_done "worker_deployed"
fi

if step_done "worker_secrets"; then
  skip "Worker secrets"
else
  step "Pushing Worker secrets..."
  LS_WEBHOOK_SECRET=$(state_get "LS_WEBHOOK_SECRET")
  LS_API_KEY=$(state_get "LS_API_KEY")
  if [[ -n "${LS_WEBHOOK_SECRET:-}" ]]; then
    echo "${LS_WEBHOOK_SECRET}" | wrangler secret put LS_WEBHOOK_SECRET --name "${WORKER_NAME}" && ok "LS_WEBHOOK_SECRET pushed."
  else
    warn "LS_WEBHOOK_SECRET not set. Add it later:"
    echo "  wrangler secret put LS_WEBHOOK_SECRET --name ${WORKER_NAME}"
    echo "  Get it from: https://app.lemonsqueezy.com/settings/webhooks"
  fi
  if [[ -n "${LS_API_KEY:-}" ]]; then
    echo "${LS_API_KEY}" | wrangler secret put LS_API_KEY --name "${WORKER_NAME}" && ok "LS_API_KEY pushed."
  fi
  mark_done "worker_secrets"
fi


# ─── 8. DNS CNAME ────────────────────────────────────────────────────────────
if step_done "dns"; then
  skip "DNS CNAME"
else
  if ! $SKIP_DNS && [[ -n "${CF_DNS_TOKEN:-}" ]] && [[ -n "$ZONE_ID" && "$ZONE_ID" != "null" ]]; then
    step "Adding DNS CNAME: ${SUBDOMAIN}.${ZONE_NAME} → ${PAGES_PROJECT}.pages.dev ..."

    EXISTING=$(curl -s -X GET \
      "https://api.cloudflare.com/client/v4/zones/${ZONE_ID}/dns_records?type=CNAME&name=${FULL_DOMAIN}" \
      -H "Authorization: Bearer ${CF_DNS_TOKEN}" \
      -H "Content-Type: application/json" | jq -r '.result | length')

    if [[ "$EXISTING" == "0" ]]; then
      curl -s -X POST \
        "https://api.cloudflare.com/client/v4/zones/${ZONE_ID}/dns_records" \
        -H "Authorization: Bearer ${CF_DNS_TOKEN}" \
        -H "Content-Type: application/json" \
        --data "{
          \"type\": \"CNAME\",
          \"name\": \"${SUBDOMAIN}\",
          \"content\": \"${PAGES_PROJECT}.pages.dev\",
          \"proxied\": true
        }" | jq -r '.success' | grep -q "true" && ok "CNAME added." || warn "CNAME creation may have failed."
    else
      warn "CNAME ${FULL_DOMAIN} already exists."
    fi
    step "Linking custom domain to Pages project..."
    wrangler pages domain add "${PAGES_PROJECT}" "${FULL_DOMAIN}" 2>/dev/null || warn "Custom domain may already be linked."
    mark_done "dns"
  else
    warn "Skipping DNS: add CNAME manually at:"
    echo "  👉 https://dash.cloudflare.com/?to=/:account/mavgo.com/dns/records"
    echo "     Type: CNAME  Name: ${SUBDOMAIN}  Target: ${PAGES_PROJECT}.pages.dev  Proxy: ✅"
    echo "  👉 https://dash.cloudflare.com/?to=/:account/pages/view/${PAGES_PROJECT}/domains"
  fi
fi

# ─── 9. GITHUB REPO ──────────────────────────────────────────────────────────
if step_done "github"; then
  skip "GitHub repository"
else
  if ! $SKIP_GITHUB; then
    step "Setting up GitHub repository (${REPO_ORG}/${REPO_NAME})..."
    cd "${SITE_DIR}"

    [ ! -d ".git" ] && git init

    cat > .gitignore << 'GIEOF'
.wrangler/
node_modules/
*.db
GIEOF

    git add .
    if ! git diff --cached --quiet; then
      git commit -m "feat: initial mksqlite.mavgo.com setup"
    else
      warn "Nothing new to commit."
    fi

    if ! gh repo view "${REPO_ORG}/${REPO_NAME}" &>/dev/null; then
      gh repo create "${REPO_ORG}/${REPO_NAME}" --public --source=. --remote=origin --push
      ok "GitHub repo created and pushed."
    else
      git remote add origin "https://github.com/${REPO_ORG}/${REPO_NAME}.git" 2>/dev/null || true
      git push origin main 2>/dev/null || git push origin HEAD:main
      ok "Pushed to existing repo."
    fi

    cd - > /dev/null
    mark_done "github"
  fi
fi

# ─── DONE ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  🎉  Setup complete!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "  🌐 Marketing site:  https://${FULL_DOMAIN}"
echo "  🔌 API base URL:    https://${FULL_DOMAIN}/api/v1/"
echo "  📦 Pages project:   https://dash.cloudflare.com/?to=/:account/pages/view/${PAGES_PROJECT}"
echo "  🗄  D1 database:     https://dash.cloudflare.com/?to=/:account/d1/databases"
echo "  💾 R2 bucket:       https://dash.cloudflare.com/?to=/:account/r2/default/buckets/${BUCKET_NAME}"
echo "  🔑 Worker secrets:  https://dash.cloudflare.com/?to=/:account/workers/services/view/${WORKER_NAME}/settings/bindings"
echo ""

echo -e "${YELLOW}Remaining items to do in Lemon Squeezy:${NC}"
echo ""
echo "  1. Create your 3 products (Starter \$9, Pro \$29, Team \$79):"
echo "     👉 https://app.lemonsqueezy.com/products/new"
echo ""
echo "  2. Enable License Keys on each product:"
echo "     Product → Edit → License keys → toggle ON"
echo ""
echo "  3. Set webhook URL + events (if not already done):"
echo "     👉 https://app.lemonsqueezy.com/settings/webhooks"
echo "     URL:    https://${FULL_DOMAIN}/api/v1/webhooks/lemonsqueezy"
echo "     Events: subscription_created, order_created, subscription_cancelled"
echo ""
if [[ -z "${LS_WEBHOOK_SECRET:-}" ]]; then
  echo -e "  ${RED}⚠ LS_WEBHOOK_SECRET was not set. Push it when ready:${NC}"
  echo "     wrangler secret put LS_WEBHOOK_SECRET --name ${WORKER_NAME}"
  echo "     (secret is on the webhook page linked above)"
  echo ""
fi
if $SKIP_DNS; then
  echo -e "  ${RED}⚠ DNS was not configured. Add manually:${NC}"
  echo "     👉 https://dash.cloudflare.com/?to=/:account/mavgo.com/dns/records"
  echo "     CNAME  ${SUBDOMAIN}  →  ${PAGES_PROJECT}.pages.dev  (Proxied)"
  echo ""
fi
