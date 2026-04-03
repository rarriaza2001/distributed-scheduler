DROP INDEX IF EXISTS idx_jobs_idempotency_key_unique;
DROP INDEX IF EXISTS idx_execution_audit_job_occurred;
DROP INDEX IF EXISTS idx_jobs_lease_expires_at;
DROP INDEX IF EXISTS idx_jobs_status_scheduled_at;

DROP TABLE IF EXISTS execution_audit;
DROP TABLE IF EXISTS jobs;
