-- Phase 4: authoritative job lifecycle (Postgres). Redis remains transport-only.
-- Lifecycle contracts and recovery rules are frozen in internal/scheduler/doc.go and docs/PHASE4.md.

CREATE TABLE jobs (
    job_id                  text PRIMARY KEY,
    queue                   text        NOT NULL,
    type                    text        NOT NULL,
    payload                 jsonb       NOT NULL,
    priority                integer     NOT NULL DEFAULT 0,
    status                  text        NOT NULL
        CHECK (status IN ('queued', 'leased', 'running', 'succeeded', 'failed', 'dead')),
    attempts_made           integer     NOT NULL DEFAULT 0,
    max_attempts            integer     NOT NULL,
    created_at              timestamptz NOT NULL,
    updated_at              timestamptz NOT NULL,
    scheduled_at            timestamptz NOT NULL,
    lease_owner_worker_id   text,
    lease_expires_at        timestamptz,
    last_error_message      text,
    last_error_code         text,
    last_error_retryable    boolean,
    last_failed_at          timestamptz,
    idempotency_key         text,
    version                 integer     NOT NULL DEFAULT 0
);

CREATE TABLE execution_audit (
    event_id    text PRIMARY KEY,
    job_id      text,
    worker_id   text,
    scheduler_id text,
    source      text        NOT NULL,
    type        text        NOT NULL,
    occurred_at timestamptz NOT NULL,
    details     jsonb       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_jobs_status_scheduled_at
    ON jobs (status, scheduled_at);

CREATE INDEX idx_jobs_lease_expires_at
    ON jobs (lease_expires_at);

CREATE INDEX idx_execution_audit_job_occurred
    ON execution_audit (job_id, occurred_at);

-- Used by producer InsertJob duplicate detection (idempotency_key).
CREATE UNIQUE INDEX idx_jobs_idempotency_key_unique
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
