CREATE TABLE IF NOT EXISTS Endpoints (
    slug                    VARCHAR PRIMARY KEY,
    func_revision           BIGINT,
    brief_intro             TEXT,
    detailed_intro          TEXT,
    recommended_use_cases   JSONB NOT NULL DEFAULT '[]'::jsonb,
    tags                    JSONB NOT NULL DEFAULT '[]'::jsonb,
    provider                VARCHAR,
    parameter_count_b       NUMERIC(10,2),
    context_length          BIGINT,
    published_at            TIMESTAMPTZ,
    published_by            VARCHAR
);

ALTER TABLE Endpoints
    ADD COLUMN IF NOT EXISTS func_revision BIGINT,
    ADD COLUMN IF NOT EXISTS brief_intro TEXT,
    ADD COLUMN IF NOT EXISTS detailed_intro TEXT,
    ADD COLUMN IF NOT EXISTS recommended_use_cases JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS provider VARCHAR,
    ADD COLUMN IF NOT EXISTS parameter_count_b NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS context_length BIGINT,
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS published_by VARCHAR;

UPDATE Endpoints
SET func_revision = 0
WHERE func_revision IS NULL;

ALTER TABLE Endpoints
    ALTER COLUMN func_revision SET NOT NULL;

ALTER TABLE Endpoints
    DROP COLUMN IF EXISTS createtime,
    DROP COLUMN IF EXISTS updatetime;

DROP TRIGGER IF EXISTS endpoints_updatetime ON Endpoints;

CREATE INDEX IF NOT EXISTS idx_endpoints_tags ON Endpoints USING GIN (tags);
