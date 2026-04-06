-- ============================================================
-- BigQuery: Extract repository languages (optional enrichment)
-- ============================================================
-- Uses the public GitHub repos dataset for language info.
-- This is a separate dataset from GH Archive.
--
-- Run in BigQuery Console.
-- Export result as CSV → data/repo_languages.csv
-- ============================================================

SELECT
  repo_name,
  lang.name AS language,
  lang.bytes AS language_bytes
FROM `bigquery-public-data.github_repos.languages`,
     UNNEST(language) AS lang
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY repo_name
  ORDER BY lang.bytes DESC
) = 1;
