CREATE TABLE IF NOT EXISTS channels (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    is_active   INTEGER NOT NULL DEFAULT 1,
    config_json TEXT,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
    id              TEXT PRIMARY KEY,
    channel_id      TEXT NOT NULL,
    name            TEXT NOT NULL,
    credential_enc  TEXT NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 50,
    rpm_limit       INTEGER,
    quota_total     INTEGER,
    quota_period    TEXT,
    quota_used      INTEGER NOT NULL DEFAULT 0,
    quota_period_start INTEGER,
    is_active       INTEGER NOT NULL DEFAULT 1,
    metadata_json   TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_accounts_channel ON accounts(channel_id, is_active);

CREATE TABLE IF NOT EXISTS proxy_entries (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    proxy_url_enc TEXT NOT NULL,
    proxy_key     TEXT,
    scheme        TEXT NOT NULL,
    host          TEXT NOT NULL,
    port          INTEGER,
    username      TEXT,
    is_active     INTEGER NOT NULL DEFAULT 1,
    notes         TEXT,
    health_status TEXT NOT NULL DEFAULT 'unknown',
    latency_ms    INTEGER,
    last_checked_at INTEGER,
    last_error    TEXT,
    failure_count INTEGER NOT NULL DEFAULT 0,
    exit_ip       TEXT,
    country       TEXT,
    region        TEXT,
    city          TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_proxy_entries_active_created ON proxy_entries(is_active, created_at DESC);

CREATE TABLE IF NOT EXISTS request_logs (
    id              TEXT PRIMARY KEY,
    channel_id      TEXT NOT NULL,
    account_id      TEXT,
    session_id      TEXT,
    method          TEXT NOT NULL,
    path            TEXT NOT NULL,
    stream          INTEGER NOT NULL DEFAULT 0,
    selection_key   TEXT,
    model           TEXT,
    status          INTEGER NOT NULL,
    response_class  TEXT NOT NULL,
    latency_ms      INTEGER NOT NULL,
    first_response_ms INTEGER,
    tokens_in       INTEGER,
    tokens_out      INTEGER,
    phase_timings_json TEXT,
    error           TEXT,
    created_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_logs_channel_time ON request_logs(channel_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_logs_account_time ON request_logs(account_id, created_at DESC);

CREATE TABLE IF NOT EXISTS system_logs (
    id            TEXT PRIMARY KEY,
    level         TEXT NOT NULL,
    component     TEXT NOT NULL,
    event         TEXT NOT NULL,
    message       TEXT NOT NULL,
    job_id        TEXT,
    metadata_json TEXT,
    created_at    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_system_logs_created ON system_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_system_logs_component_time ON system_logs(component, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_system_logs_job_time ON system_logs(job_id, created_at DESC);

CREATE TABLE IF NOT EXISTS freebuff_account_state (
    account_id       TEXT PRIMARY KEY,
    upstream_user_id TEXT,
    email            TEXT,
    access_tier      TEXT,
    last_sync_at     INTEGER NOT NULL,
    raw_json         TEXT
);

CREATE TABLE IF NOT EXISTS freebuff_quota_snapshots (
    account_id        TEXT NOT NULL,
    quota_group       TEXT NOT NULL,
    model             TEXT NOT NULL,
    limit_count       INTEGER,
    recent_count      REAL,
    period            TEXT,
    reset_timezone    TEXT,
    reset_at          INTEGER,
    window_hours      INTEGER,
    updated_at        INTEGER NOT NULL,
    raw_json          TEXT,
    PRIMARY KEY (account_id, quota_group, model)
);

CREATE INDEX IF NOT EXISTS idx_freebuff_quota_account ON freebuff_quota_snapshots(account_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS freebuff_upstream_sessions (
    instance_id       TEXT PRIMARY KEY,
    account_id        TEXT NOT NULL,
    local_session_id  TEXT,
    model             TEXT NOT NULL,
    quota_group       TEXT NOT NULL,
    status            TEXT NOT NULL,
    admitted_at       INTEGER,
    expires_at        INTEGER,
    remaining_ms      INTEGER,
    ended_at          INTEGER,
    updated_at        INTEGER NOT NULL,
    raw_json          TEXT
);

CREATE INDEX IF NOT EXISTS idx_freebuff_sessions_account ON freebuff_upstream_sessions(account_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS auth_keys (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    key_hash     TEXT NOT NULL UNIQUE,
    key_prefix   TEXT NOT NULL,
    is_active    INTEGER NOT NULL DEFAULT 1,
    last_used_at INTEGER,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_auth_keys_active_created ON auth_keys(is_active, created_at DESC);
