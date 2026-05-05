# Endpoint Policy Priority Case Summary

Source: `docs/funcpolicy-priority-codex.md`

This note pulls out the customer-facing case summary for where endpoint policy settings come from in different request paths.

## Case 1: Normal function

This means an ordinary function request path, not a tenant virtual endpoint and not a special summary of platform endpoint routing.

Current / intended policy resolution order:

1. Same-name standalone `funcpolicy` at `{tenant}/{namespace}/{name}`
2. Function `spec.policy.Obj`
3. Function `spec.policy.Link`
4. `FuncPolicySpec::default()`

Gateway-side fields typically affected:

- `queueTimeout`
- `queueLen`
- `parallel`
- `scaleoutPolicy`
- `scaleinTimeout`
- `maxReplica`

Scheduler-side fields typically affected:

- `minReplica`
- `standbyPerNode`
- `maxReplica`

## Case 2: Direct request to `/inferx/endpoint/...`

This is the direct platform function path. It is treated as a normal function path, not as the tenant virtual endpoint path.

So gateway-side request settings come from the normal function policy resolution:

1. Same-name standalone `funcpolicy` at `{inferx, "endpoint", slug}`
2. Function `spec.policy.Obj`
3. Function `spec.policy.Link`
4. `FuncPolicySpec::default()`

Gateway-side fields affected:

- `queueTimeout`
- `queueLen`
- `parallel`
- `scaleoutPolicy`
- `scaleinTimeout`
- `maxReplica`

Important note:

- direct `/inferx/endpoint/...` requests do not use tenant endpoint same-name policy at `{tenant, "endpoints", slug}`
- direct `/inferx/endpoint/...` requests do not use `GATEWAY_CONFIG.endpointsDefaultPolicy`

## Case 3: Tenant virtual endpoint request

This means the tenant endpoint path backed by the platform function under `inferx/endpoint/<slug>`.

For endpoint requests, same-name tenant endpoint `funcpolicy` remains highest priority:

- `{tenant, "endpoints", slug}`

### `maxReplica`

For gateway-side `maxReplica`, intended precedence is:

1. Same-name tenant endpoint `funcpolicy`
2. `GATEWAY_CONFIG.endpointsDefaultPolicy`

### Other gateway-relevant fields

For these fields:

- `parallel`
- `queueTimeout`
- `queueLen`
- `scaleoutPolicy`
- `scaleinTimeout`

Intended precedence is:

1. Same-name tenant endpoint `funcpolicy`
2. Backing function policy under `inferx/endpoint`
3. `GATEWAY_CONFIG.endpointsDefaultPolicy`

Important note:

- this is the intended design direction
- current implementation does not yet fully apply this field-specific fallback and still mainly uses same-name tenant endpoint `funcpolicy` or `GATEWAY_CONFIG.endpointsDefaultPolicy`

## Note On Current Defaults

Case 2 direct `/inferx/endpoint/...` requests and Case 3 tenant virtual endpoint requests both currently end up with `max_replica = 1` when nothing overrides them, but that equivalence is only because:

- Case 2 falls through to `FuncPolicySpec::default()`
- Case 3 falls through to `GATEWAY_CONFIG.endpointsDefaultPolicy`

They agree today by coincidence of current defaults, not because the two paths share the same intended fallback model.
