-- V1: Initial schema for the tasks table
-- Flyway runs this once and tracks it in flyway_schema_history.

CREATE TABLE IF NOT EXISTS tasks (
    id           UUID         PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    status       VARCHAR(20)  NOT NULL CHECK (status IN ('PENDING', 'QUEUED', 'COMPLETED', 'FAILED')),
    scheduled_at TIMESTAMP    NOT NULL,
    created_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    retry_count  INT          NOT NULL DEFAULT 0,
    version      BIGINT       NOT NULL DEFAULT 0
);

-- Composite index: the scheduler polls this exact filter on every tick
CREATE INDEX IF NOT EXISTS idx_tasks_status_scheduled
    ON tasks (status, scheduled_at);