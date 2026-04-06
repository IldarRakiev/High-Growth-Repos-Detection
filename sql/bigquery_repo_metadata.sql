-- ============================================================
-- BigQuery: Extract repository creation dates from GH Archive
-- ============================================================
-- Same 10% sample as events (MOD(repo.id, 10) = 0).
-- Finds the earliest CreateEvent per repo as a proxy for
-- repo creation date.
--
-- Run in Cloud Shell:
--   bq query --use_legacy_sql=false \
--     --destination_table=gharchive_temp.repo_metadata_10pct \
--     --replace "<this query>"
--
-- Expected result: ~2-3M rows
-- ============================================================

SELECT
  repo.id         AS repo_id,
  repo.name       AS repo_name,
  MIN(created_at) AS repo_first_seen
FROM `githubarchive.month.*`
WHERE _TABLE_SUFFIX BETWEEN '202301' AND '202406'
  AND type = 'CreateEvent'
  AND MOD(repo.id, 10) = 0
GROUP BY repo_id, repo_name;
