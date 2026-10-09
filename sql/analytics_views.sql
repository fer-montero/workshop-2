-- =============================================================================
-- Resonance Records - analytical views for the dashboard (database: music_dw)
--
-- SQL counterpart of the Power BI (DAX) measures, one view per dashboard block.
-- They are used to reconcile the dashboard figures against independent SQL queries
-- over the star schema (KPI queries deliverable).
-- Idempotent: CREATE OR REPLACE VIEW, safe to run again after every load.
-- The unknown member (artist_key = 0, "Desconocido") is excluded from artist analysis.
-- =============================================================================

-- ----------------------------------------------------------------------------- AR1
-- One row per artist WITH Grammy history AND Spotify tracks (needed to place it on
-- the recognition x popularity chart). main_genre_family = the family where the
-- artist has the most tracks (ties broken alphabetically), used for slicing/tooltips.
CREATE OR REPLACE VIEW vw_artist_segment AS
WITH artist_family AS (
    SELECT bta.artist_key,
           g.genre_family,
           COUNT(DISTINCT bta.track_key) AS tracks,
           ROW_NUMBER() OVER (PARTITION BY bta.artist_key
                              ORDER BY COUNT(DISTINCT bta.track_key) DESC, g.genre_family) AS rn
    FROM bridge_track_artist bta
    JOIN bridge_track_genre  btg ON btg.track_key = bta.track_key
    JOIN dim_genre           g   ON g.genre_key   = btg.genre_key
    GROUP BY bta.artist_key, g.genre_family
)
SELECT a.artist_key,
       a.artist_name,
       a.total_awards,
       a.max_popularity,
       a.avg_popularity,
       a.track_count,
       a.segment,
       a.popularity_zero,
       af.genre_family AS main_genre_family
FROM dim_artist a
LEFT JOIN artist_family af ON af.artist_key = a.artist_key AND af.rn = 1
WHERE a.artist_key <> 0
  AND a.in_grammy
  AND a.in_spotify;

-- ----------------------------------------------------------------------------- AR2
-- For each segment (candidate / consolidated) and genre family: how many track-genre
-- assignments of the segment's artists fall in that family, and the share inside the
-- segment (shares of one segment add up to 100%). A track with several genres counts
-- once per genre; a track shared by artists of both segments counts in both.
CREATE OR REPLACE VIEW vw_genre_segment_share AS
WITH seg_pairs AS (
    SELECT DISTINCT a.segment, btg.track_key, btg.genre_key, bta.artist_key
    FROM dim_artist a
    JOIN bridge_track_artist bta ON bta.artist_key = a.artist_key
    JOIN bridge_track_genre  btg ON btg.track_key  = bta.track_key
    WHERE a.segment IN ('candidato', 'consolidado') AND a.artist_key <> 0
),
by_family AS (
    SELECT p.segment,
           g.genre_family,
           COUNT(DISTINCT (p.track_key, p.genre_key)) AS track_genre_pairs,
           COUNT(DISTINCT p.artist_key)               AS artists
    FROM seg_pairs p
    JOIN dim_genre g ON g.genre_key = p.genre_key
    GROUP BY p.segment, g.genre_family
)
SELECT segment,
       genre_family,
       track_genre_pairs,
       artists,
       ROUND(100.0 * track_genre_pairs / SUM(track_genre_pairs) OVER (PARTITION BY segment), 2) AS share_pct
FROM by_family;

-- ----------------------------------------------------------------------------- AR3
-- Average audio features of the distinct tracks of each segment, in long format
-- (one row per segment x feature) so the chart can group by feature.
CREATE OR REPLACE VIEW vw_sonic_profile AS
WITH seg_tracks AS (
    SELECT DISTINCT a.segment, t.track_key, t.energy, t.valence, t.danceability, t.acousticness
    FROM dim_artist a
    JOIN bridge_track_artist bta ON bta.artist_key = a.artist_key
    JOIN fact_track          t   ON t.track_key    = bta.track_key
    WHERE a.segment IN ('candidato', 'consolidado') AND a.artist_key <> 0
)
SELECT segment, 'Energy' AS feature, ROUND(AVG(energy)::numeric, 3) AS avg_value, COUNT(*) AS tracks FROM seg_tracks GROUP BY segment
UNION ALL
SELECT segment, 'Valence', ROUND(AVG(valence)::numeric, 3), COUNT(*) FROM seg_tracks GROUP BY segment
UNION ALL
SELECT segment, 'Danceability', ROUND(AVG(danceability)::numeric, 3), COUNT(*) FROM seg_tracks GROUP BY segment
UNION ALL
SELECT segment, 'Acousticness', ROUND(AVG(acousticness)::numeric, 3), COUNT(*) FROM seg_tracks GROUP BY segment;

-- ----------------------------------------------------------------------------- KPIs
-- One row with the headline numbers of the dashboard.
-- Award coverage uses the SAME definition as the transformation metric
-- (transform_metrics.json -> award_match_rate_pct): awards with an identifiable artist
-- (artist_key <> 0) that have at least one credited artist present in Spotify.
-- The share over ALL awards (including the unknown member) is also exposed for context.
-- NOTE: views are recreated with DROP + CREATE because CREATE OR REPLACE cannot
-- rename or reorder existing view columns.
DROP VIEW IF EXISTS vw_kpi_summary;
CREATE VIEW vw_kpi_summary AS
WITH awards AS (
    SELECT f.source_row_number,
           BOOL_OR(f.artist_key <> 0) AS has_identified_artist,
           BOOL_OR(a.in_spotify)      AS has_spotify_artist
    FROM fact_award f
    JOIN dim_artist a ON a.artist_key = f.artist_key
    GROUP BY f.source_row_number
)
SELECT
    (SELECT COUNT(*) FROM dim_artist WHERE segment = 'candidato')                          AS candidates,
    (SELECT COUNT(*) FROM dim_artist WHERE segment = 'candidato' AND popularity_zero)      AS candidates_popularity_zero,
    (SELECT COUNT(*) FROM dim_artist WHERE segment = 'consolidado')                        AS consolidated,
    (SELECT COUNT(*) FROM dim_artist WHERE in_grammy AND in_spotify AND artist_key <> 0)    AS artists_in_both_sources,
    (SELECT COUNT(*) FROM dim_artist WHERE artist_key <> 0)                                AS artists_total,
    (SELECT COUNT(*) FROM awards)                                                          AS grammy_awards,
    (SELECT COUNT(*) FROM awards WHERE has_identified_artist)                              AS awards_with_identified_artist,
    (SELECT COUNT(*) FROM awards WHERE has_spotify_artist)                                 AS awards_with_spotify_artist,
    (SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE has_spotify_artist)
                  / NULLIF(COUNT(*) FILTER (WHERE has_identified_artist), 0), 1) FROM awards) AS pct_awards_matched_spotify,
    (SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE has_spotify_artist)
                  / NULLIF(COUNT(*), 0), 1) FROM awards)                                   AS pct_all_awards_matched_spotify,
    (SELECT COALESCE(SUM(award_count), 0) FROM fact_award WHERE artist_key = 0)            AS awards_unknown_artist;