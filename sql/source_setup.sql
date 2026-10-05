-- Grammy SOURCE preparation. This is NOT the ETL Load stage.
-- Columns are nullable and mostly textual on purpose so that source
-- quality problems remain visible to the raw validation gate.
CREATE TABLE IF NOT EXISTS grammy_awards (
    source_row_number INTEGER       NOT NULL PRIMARY KEY,
    "year"            SMALLINT      NULL,
    title             VARCHAR(255)  NULL,
    published_at      VARCHAR(40)   NULL,
    updated_at        VARCHAR(40)   NULL,
    category          VARCHAR(255)  NULL,
    nominee           TEXT          NULL,
    artist            TEXT          NULL,
    workers           TEXT          NULL,
    img               TEXT          NULL,
    winner            VARCHAR(10)   NULL,
    loaded_at         TIMESTAMPTZ   NOT NULL DEFAULT now()
);