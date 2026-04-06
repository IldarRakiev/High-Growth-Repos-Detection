-- ============================================================
-- PostgreSQL: Create tables for GitHub events star schema
-- ============================================================
-- Dimension table: repositories (loaded first)
-- Fact table:      events       (references repositories)
--
-- Tables are dropped and recreated for idempotency.
-- Indexes are created after data loading (see stage1.sh).
-- ============================================================

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS repositories;

CREATE TABLE repositories (
    repo_id       BIGINT PRIMARY KEY,
    repo_name     TEXT NOT NULL,
    first_seen_at TIMESTAMP,
    language      VARCHAR(100)
);

CREATE TABLE events (
    event_type    VARCHAR(50) NOT NULL,
    repo_id       BIGINT      NOT NULL,
    event_date    DATE        NOT NULL,
    event_count   INTEGER     NOT NULL DEFAULT 0,
    unique_actors INTEGER     NOT NULL DEFAULT 0
);
