-- ============================================================
-- BigQuery: Extract GitHub events from GH Archive
-- ============================================================
-- Single query covering all 18 months (Jan 2023 – Jun 2024).
-- 10% deterministic sample via MOD(repo.id, 10) = 0.
-- Daily aggregation per (event_type, repo_id, event_date).
--
-- Run in Cloud Shell:
--   bq query --use_legacy_sql=false \
--     --destination_table=gharchive_temp.events_daily_10pct \
--     --replace "<this query>"
--
-- Expected result: ~58M rows, ~4 GB
-- Scan cost: ~157 GB (within 1 TB/month free tier)
-- ============================================================

SELECT
  type                              AS event_type,
  repo.id                           AS repo_id,
  repo.name                         AS repo_name,
  CAST(created_at AS DATE)          AS event_date,
  COUNT(*)                          AS event_count,
  COUNT(DISTINCT actor.id)          AS unique_actors
FROM `githubarchive.month.*`
WHERE _TABLE_SUFFIX BETWEEN '202301' AND '202406'
  AND type IN (
    'WatchEvent', 'ForkEvent', 'PullRequestEvent',
    'PushEvent', 'IssuesEvent', 'CreateEvent'
  )
  AND MOD(repo.id, 10) = 0
GROUP BY event_type, repo_id, repo_name, event_date;
