CREATE TABLE IF NOT EXISTS Endpoints (
    slug                    VARCHAR PRIMARY KEY,
    func_revision           BIGINT NOT NULL,
    brief_intro             TEXT,
    detailed_intro          TEXT,
    cs_ttft                 TEXT,
    recommended_use_cases   JSONB NOT NULL DEFAULT '[]'::jsonb,
    tags                    JSONB NOT NULL DEFAULT '[]'::jsonb,
    provider                VARCHAR,
    parameter_count_b       NUMERIC(10,2),
    context_length          BIGINT,
    concurrency             NUMERIC(10,2),
    last_published_at       TIMESTAMPTZ,
    last_published_by       VARCHAR,
    updatetime              TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE Endpoints
    ADD COLUMN IF NOT EXISTS cs_ttft TEXT,
    ADD COLUMN IF NOT EXISTS concurrency NUMERIC(10,2);

ALTER TABLE Endpoints
    DROP COLUMN IF EXISTS max_token_length;

CREATE INDEX IF NOT EXISTS idx_endpoints_tags ON Endpoints USING GIN (tags);

CREATE OR REPLACE FUNCTION set_updatetime()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updatetime = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS endpoints_updatetime ON Endpoints;
CREATE TRIGGER endpoints_updatetime BEFORE UPDATE ON Endpoints
FOR EACH ROW EXECUTE FUNCTION set_updatetime();
