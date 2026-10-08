-- =============================================================================
-- Resonance Records - Data Warehouse (database: music_dw, PostgreSQL 16)
-- Star schema in constellation form: fact_award + fact_track share dim_artist.
--
-- Idempotent DDL: safe to run on every load (CREATE ... IF NOT EXISTS).
-- Constraints are the last line of defense: even if a bad batch passed every
-- earlier check, PostgreSQL rejects it and the load transaction rolls back.
-- =============================================================================

-- ----------------------------------------------------------------- dimensions
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_key       INTEGER      PRIMARY KEY,                -- surrogate key (0 = "Desconocido")
    artist_name      TEXT         NOT NULL,
    artist_norm      TEXT         NOT NULL UNIQUE,            -- business / integration key
    in_grammy        BOOLEAN      NOT NULL,
    in_spotify       BOOLEAN      NOT NULL,
    total_awards     INTEGER      NOT NULL CHECK (total_awards >= 0),
    track_count      INTEGER      NOT NULL CHECK (track_count >= 0),
    avg_popularity   NUMERIC(5,2)          CHECK (avg_popularity BETWEEN 0 AND 100),
    max_popularity   SMALLINT              CHECK (max_popularity BETWEEN 0 AND 100),
    segment          VARCHAR(20)  NOT NULL CHECK (segment IN ('candidato', 'consolidado', 'no aplica')),
    popularity_zero  BOOLEAN      NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_genre (
    genre_key        INTEGER      PRIMARY KEY,
    genre            VARCHAR(60)  NOT NULL UNIQUE,            -- Spotify label (business key)
    genre_family     VARCHAR(60)  NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_key     INTEGER      PRIMARY KEY,
    category_name    TEXT         NOT NULL UNIQUE,            -- business key
    category_type    VARCHAR(20)  NOT NULL CHECK (category_type IN ('artística', 'técnica'))
);

CREATE TABLE IF NOT EXISTS dim_year (
    year_key         INTEGER      PRIMARY KEY,
    year             SMALLINT     NOT NULL UNIQUE CHECK (year >= 1958),
    decade           VARCHAR(6)   NOT NULL
);

-- ----------------------------------------------------------------- facts
-- Grain: one row per unique track (track_id).
CREATE TABLE IF NOT EXISTS fact_track (
    track_key        INTEGER          PRIMARY KEY,
    track_id         VARCHAR(40)      NOT NULL UNIQUE,        -- degenerate dimension
    popularity       SMALLINT         NOT NULL CHECK (popularity BETWEEN 0 AND 100),
    energy           DOUBLE PRECISION NOT NULL CHECK (energy BETWEEN 0 AND 1),
    valence          DOUBLE PRECISION NOT NULL CHECK (valence BETWEEN 0 AND 1),
    danceability     DOUBLE PRECISION NOT NULL CHECK (danceability BETWEEN 0 AND 1),
    acousticness     DOUBLE PRECISION NOT NULL CHECK (acousticness BETWEEN 0 AND 1),
    is_explicit      SMALLINT         NOT NULL CHECK (is_explicit IN (0, 1))
);

-- Grain: one row per Grammy award per credited artist.
CREATE TABLE IF NOT EXISTS fact_award (
    award_key          INTEGER   PRIMARY KEY,
    source_row_number  INTEGER   NOT NULL,                    -- degenerate dimension (traceability)
    artist_key         INTEGER   NOT NULL REFERENCES dim_artist (artist_key),
    category_key       INTEGER   NOT NULL REFERENCES dim_category (category_key),
    year_key           INTEGER   NOT NULL REFERENCES dim_year (year_key),
    award_count        SMALLINT  NOT NULL DEFAULT 1 CHECK (award_count = 1),
    UNIQUE (source_row_number, artist_key)                    -- an award is never credited twice to the same artist
);

-- ----------------------------------------------------------------- bridges (N:M)
CREATE TABLE IF NOT EXISTS bridge_track_artist (
    track_key        INTEGER  NOT NULL REFERENCES fact_track (track_key),
    artist_key       INTEGER  NOT NULL REFERENCES dim_artist (artist_key),
    PRIMARY KEY (track_key, artist_key)
);

CREATE TABLE IF NOT EXISTS bridge_track_genre (
    track_key        INTEGER  NOT NULL REFERENCES fact_track (track_key),
    genre_key        INTEGER  NOT NULL REFERENCES dim_genre (genre_key),
    PRIMARY KEY (track_key, genre_key)
);

-- ----------------------------------------------------------------- join indexes
CREATE INDEX IF NOT EXISTS ix_fact_award_artist      ON fact_award (artist_key);
CREATE INDEX IF NOT EXISTS ix_fact_award_category    ON fact_award (category_key);
CREATE INDEX IF NOT EXISTS ix_bridge_artist_artist   ON bridge_track_artist (artist_key);
CREATE INDEX IF NOT EXISTS ix_bridge_genre_genre     ON bridge_track_genre (genre_key);
CREATE INDEX IF NOT EXISTS ix_dim_artist_segment     ON dim_artist (segment);

-- ----------------------------------------------------------------- load audit
-- One row per table per successful load: batch traceability and rerun evidence.
-- This table is append-only on purpose (it is history, not analytical data).
CREATE TABLE IF NOT EXISTS etl_load_audit (
    load_id          BIGSERIAL    PRIMARY KEY,
    run_id           TEXT         NOT NULL,
    table_name       TEXT         NOT NULL,
    rows_loaded      INTEGER      NOT NULL,
    loaded_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);