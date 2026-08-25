CREATE TABLE IF NOT EXISTS ThrottleLimit (
    id              SERIAL PRIMARY KEY,
    tenant          VARCHAR,          -- NULL = global default; set = per-tenant override
    req_per_min     BIGINT NOT NULL,
    req_per_hour    BIGINT NOT NULL,
    wtok_per_min    BIGINT NOT NULL,  -- weighted: (in-cached)*1 + cached*0.1 + out*2
    wtok_per_hour   BIGINT NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    added_by        VARCHAR NOT NULL,
    UNIQUE(tenant)                     -- one row per named tenant (NULLs are NOT covered — see below)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_throttle_global ON ThrottleLimit ((tenant IS NULL)) WHERE tenant IS NULL;

INSERT INTO ThrottleLimit (tenant, req_per_min, req_per_hour, wtok_per_min, wtok_per_hour, added_by)
VALUES (NULL, 60, 1500, 2500000, 35000000, 'system')
ON CONFLICT ((tenant IS NULL)) WHERE tenant IS NULL DO NOTHING;

INSERT INTO ThrottleLimit (tenant, req_per_min, req_per_hour, wtok_per_min, wtok_per_hour, added_by)
VALUES ('system', 100000, 6000000, 999000000, 999000000000, 'system')
ON CONFLICT (tenant) DO NOTHING;

INSERT INTO ThrottleLimit (tenant, req_per_min, req_per_hour, wtok_per_min, wtok_per_hour, added_by)
VALUES ('inferx', 100000, 6000000, 999000000, 999000000000, 'system')
ON CONFLICT (tenant) DO NOTHING;
