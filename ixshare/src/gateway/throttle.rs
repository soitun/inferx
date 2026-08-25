// Copyright (c) 2025 InferX Authors /
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

// Per-tenant token-bucket rate limiter for the shared-endpoint dispatch path.
// Scope: `/endpoints/v1/completions` (Direct) only — not `/v1/*`
// (OpenRouter), `/funccall/*`, `/modelcall/*`, or `/directfunccall/*`.
//
// Token counts (prompt/completion/cached) are only known once a response
// completes (see `req_token::record_usage`), so the two bucket kinds are
// enforced differently:
//   - request buckets: cost is exactly 1, known upfront -> checked AND
//     decremented atomically at admission.
//   - weighted-token buckets: cost is unknown at admission -> only peeked
//     (reject if already exhausted), then decremented post-hoc from the real
//     usage numbers once the response completes. Self-corrects within a
//     window; a single oversized request cannot exceed bucket capacity.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::audit::{SqlAudit, ThrottleLimitRecord};
use crate::gateway::http_gateway::GATEWAY_CONFIG;
use crate::gateway::metrics::GATEWAY_METRICS;

const MINUTE: Duration = Duration::from_secs(60);
const HOUR: Duration = Duration::from_secs(3600);

#[derive(Clone, Copy, Debug)]
pub struct ThrottleLimits {
    pub req_per_min: i64,
    pub req_per_hour: i64,
    pub wtok_per_min: i64,
    pub wtok_per_hour: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThrottleBranch {
    MinuteReq,
    HourReq,
    MinuteTok,
    HourTok,
}

#[derive(Default)]
struct ThrottleConfig {
    global: Option<ThrottleLimits>,
    overrides: HashMap<String, ThrottleLimits>,
}

#[derive(Clone, Copy, Debug)]
struct BucketState {
    fill: f64,
    last_refill: Instant,
}

impl BucketState {
    fn full(capacity: i64, now: Instant) -> Self {
        Self { fill: capacity as f64, last_refill: now }
    }

    fn refill(&mut self, capacity: i64, window: Duration, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            let rate = capacity as f64 / window.as_secs_f64();
            self.fill = (self.fill + elapsed * rate).min(capacity as f64);
            self.last_refill = now;
        }
    }
}

struct TenantBuckets {
    minute_req: BucketState,
    hour_req: BucketState,
    minute_tok: BucketState,
    hour_tok: BucketState,
}

impl TenantBuckets {
    fn full(limits: &ThrottleLimits, now: Instant) -> Self {
        Self {
            minute_req: BucketState::full(limits.req_per_min, now),
            hour_req: BucketState::full(limits.req_per_hour, now),
            minute_tok: BucketState::full(limits.wtok_per_min, now),
            hour_tok: BucketState::full(limits.wtok_per_hour, now),
        }
    }
}

pub struct ThrottleStateInner {
    limits: RwLock<ThrottleConfig>,
    buckets: DashMap<String, TenantBuckets>,
    config_loaded: AtomicBool,
    last_good_at: RwLock<Option<Instant>>,
}

#[derive(Clone)]
pub struct ThrottleState(Arc<ThrottleStateInner>);

impl Deref for ThrottleState {
    type Target = Arc<ThrottleStateInner>;

    fn deref(&self) -> &Arc<ThrottleStateInner> {
        &self.0
    }
}

impl ThrottleState {
    pub fn New() -> Self {
        let inner = ThrottleStateInner {
            limits: RwLock::new(ThrottleConfig::default()),
            buckets: DashMap::new(),
            config_loaded: AtomicBool::new(false),
            last_good_at: RwLock::new(None),
        };

        let state = ThrottleState(Arc::new(inner));
        let state1 = state.clone();
        tokio::spawn(async move {
            state1.ReloadLoop().await;
        });

        state
    }

    async fn ReloadLoop(&self) {
        let addr = GATEWAY_CONFIG.billingdbAddr.clone();
        let period = Duration::from_secs(GATEWAY_CONFIG.throttleConfigReloadIntervalSecs.max(1));
        let mut interval = tokio::time::interval(period);
        let mut sql: Option<SqlAudit> = None;

        loop {
            interval.tick().await;

            if addr.is_empty() {
                self.OnReloadProblem("billingdb address not configured");
                self.UpdateConfigMetrics().await;
                continue;
            }

            if sql.is_none() {
                match SqlAudit::New(&addr).await {
                    Ok(s) => sql = Some(s),
                    Err(e) => {
                        self.OnReloadProblem(&format!("billingdb connect failed: {:?}", e));
                        self.UpdateConfigMetrics().await;
                        continue;
                    }
                }
            }

            let s = sql.as_ref().unwrap();
            match s.LoadThrottleLimits().await {
                Ok(records) => self.ApplyRecords(records),
                Err(e) => self.OnReloadProblem(&format!("load failed: {:?}", e)),
            }
            self.UpdateConfigMetrics().await;
        }
    }

    fn ApplyRecords(&self, records: Vec<ThrottleLimitRecord>) {
        let mut global = None;
        let mut overrides = HashMap::new();
        for r in records {
            let limits = ThrottleLimits {
                req_per_min: r.req_per_min,
                req_per_hour: r.req_per_hour,
                wtok_per_min: r.wtok_per_min,
                wtok_per_hour: r.wtok_per_hour,
            };
            match r.tenant {
                None => global = Some(limits),
                Some(t) => {
                    overrides.insert(t, limits);
                }
            }
        }

        match global {
            Some(global) => {
                *self.limits.write().unwrap() = ThrottleConfig { global: Some(global), overrides };
                self.config_loaded.store(true, Ordering::SeqCst);
                *self.last_good_at.write().unwrap() = Some(Instant::now());
            }
            None => self.OnReloadProblem("no global (tenant IS NULL) row found in ThrottleLimit"),
        }
    }

    fn OnReloadProblem(&self, msg: &str) {
        if self.config_loaded.load(Ordering::SeqCst) {
            error!("throttle: config reload problem, keeping last-good config: {}", msg);
        } else {
            warn!(
                "throttle: no valid config loaded yet, /endpoints/v1/completions is fail-open: {}",
                msg
            );
        }
    }

    async fn UpdateConfigMetrics(&self) {
        let loaded = self.config_loaded.load(Ordering::SeqCst);
        let age = match *self.last_good_at.read().unwrap() {
            Some(t) => Instant::now().saturating_duration_since(t).as_secs() as i64,
            None => 0,
        };
        let metrics = GATEWAY_METRICS.lock().await;
        metrics.throttle_config_state.set(if loaded { 1 } else { 0 });
        metrics.throttle_config_age_seconds.set(age);
    }

    fn effective_limits(&self, tenant: &str) -> Option<ThrottleLimits> {
        let cfg = self.limits.read().unwrap();
        if let Some(o) = cfg.overrides.get(tenant) {
            return Some(*o);
        }
        cfg.global
    }

    /// Check + admit one request for `tenant`. Cold start (no config ever
    /// loaded) fails open (proposal §7). On success, both request buckets are
    /// decremented by 1; the weighted-token buckets are only peeked here —
    /// call `record_weighted_tokens` once the real usage is known.
    pub fn check_request(&self, tenant: &str) -> Result<(), ThrottleBranch> {
        if !self.config_loaded.load(Ordering::SeqCst) {
            return Ok(());
        }

        let limits = match self.effective_limits(tenant) {
            Some(l) => l,
            None => return Ok(()),
        };

        let now = Instant::now();
        let mut entry = self
            .buckets
            .entry(tenant.to_string())
            .or_insert_with(|| TenantBuckets::full(&limits, now));
        let b = entry.value_mut();

        b.minute_req.refill(limits.req_per_min, MINUTE, now);
        b.hour_req.refill(limits.req_per_hour, HOUR, now);
        b.minute_tok.refill(limits.wtok_per_min, MINUTE, now);
        b.hour_tok.refill(limits.wtok_per_hour, HOUR, now);

        if b.minute_req.fill < 1.0 {
            return Err(ThrottleBranch::MinuteReq);
        }
        if b.hour_req.fill < 1.0 {
            return Err(ThrottleBranch::HourReq);
        }
        if b.minute_tok.fill <= 0.0 {
            return Err(ThrottleBranch::MinuteTok);
        }
        if b.hour_tok.fill <= 0.0 {
            return Err(ThrottleBranch::HourTok);
        }

        b.minute_req.fill -= 1.0;
        b.hour_req.fill -= 1.0;
        Ok(())
    }

    /// Post-hoc charge of a completed request's weighted token cost against
    /// `tenant`'s minute/hour token buckets. Allowed to drive fill negative —
    /// that is the intentional self-limiting debt for an oversized request;
    /// the next refill climbs it back up.
    pub fn record_weighted_tokens(&self, tenant: &str, weighted: i64) {
        if !self.config_loaded.load(Ordering::SeqCst) {
            return;
        }

        let limits = match self.effective_limits(tenant) {
            Some(l) => l,
            None => return,
        };

        let now = Instant::now();
        let mut entry = self
            .buckets
            .entry(tenant.to_string())
            .or_insert_with(|| TenantBuckets::full(&limits, now));
        let b = entry.value_mut();

        b.minute_tok.refill(limits.wtok_per_min, MINUTE, now);
        b.hour_tok.refill(limits.wtok_per_hour, HOUR, now);
        b.minute_tok.fill -= weighted as f64;
        b.hour_tok.fill -= weighted as f64;
    }

    /// Seconds until the bucket that rejected `branch` next has capacity,
    /// for the `Retry-After` header (proposal §5).
    pub fn retry_after_secs(&self, tenant: &str, branch: ThrottleBranch) -> u64 {
        let limits = match self.effective_limits(tenant) {
            Some(l) => l,
            None => return 1,
        };

        let entry = match self.buckets.get(tenant) {
            Some(e) => e,
            None => return 1,
        };
        let b = entry.value();

        let (fill, capacity, window) = match branch {
            ThrottleBranch::MinuteReq => (b.minute_req.fill, limits.req_per_min, MINUTE),
            ThrottleBranch::HourReq => (b.hour_req.fill, limits.req_per_hour, HOUR),
            ThrottleBranch::MinuteTok => (b.minute_tok.fill, limits.wtok_per_min, MINUTE),
            ThrottleBranch::HourTok => (b.hour_tok.fill, limits.wtok_per_hour, HOUR),
        };

        let rate = capacity as f64 / window.as_secs_f64();
        if rate <= 0.0 {
            return window.as_secs();
        }
        let target = 1.0_f64.min(capacity as f64);
        if fill >= target {
            return 1;
        }
        (((target - fill) / rate).ceil() as u64).max(1)
    }
}

lazy_static::lazy_static! {
    pub static ref THROTTLE: ThrottleState = ThrottleState::New();
}

/// `(prompt_tokens - cached_tokens)*1 + cached_tokens*0.1 + completion_tokens*2`
/// (proposal §4), computed in tenths to avoid floating point on the hot path.
pub fn weighted_tokens(prompt: i64, cached: i64, completion: i64) -> i64 {
    let non_cached = (prompt - cached).max(0);
    (non_cached * 10 + cached + completion * 20) / 10
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits_record(
        tenant: Option<&str>,
        req_per_min: i64,
        req_per_hour: i64,
        wtok_per_min: i64,
        wtok_per_hour: i64,
    ) -> ThrottleLimitRecord {
        ThrottleLimitRecord {
            tenant: tenant.map(|s| s.to_string()),
            req_per_min,
            req_per_hour,
            wtok_per_min,
            wtok_per_hour,
            enabled: true,
        }
    }

    fn bare_state() -> ThrottleState {
        ThrottleState(Arc::new(ThrottleStateInner {
            limits: RwLock::new(ThrottleConfig::default()),
            buckets: DashMap::new(),
            config_loaded: AtomicBool::new(false),
            last_good_at: RwLock::new(None),
        }))
    }

    fn test_state(records: Vec<ThrottleLimitRecord>) -> ThrottleState {
        let state = bare_state();
        state.ApplyRecords(records);
        state
    }

    #[test]
    fn weighted_tokens_matches_worked_examples() {
        assert_eq!(weighted_tokens(1000, 0, 0), 1000);
        assert_eq!(weighted_tokens(1000, 1000, 0), 100);
        assert_eq!(weighted_tokens(0, 0, 1000), 2000);
        assert_eq!(weighted_tokens(1000, 200, 50), 800 + 20 + 100);
    }

    #[test]
    fn cold_start_fails_open() {
        let state = bare_state();
        for _ in 0..1000 {
            assert!(state.check_request("tn-x").is_ok());
        }
    }

    #[test]
    fn request_bucket_exhausts_then_rejects() {
        let state = test_state(vec![limits_record(None, 2, 1000, 1_000_000, 1_000_000)]);
        assert!(state.check_request("tn-a").is_ok());
        assert!(state.check_request("tn-a").is_ok());
        assert_eq!(state.check_request("tn-a"), Err(ThrottleBranch::MinuteReq));
    }

    #[test]
    fn per_tenant_override_wins_over_global() {
        let records = vec![
            limits_record(None, 2, 1000, 1_000_000, 1_000_000),
            limits_record(Some("tn-vip"), 5, 1000, 1_000_000, 1_000_000),
        ];
        let state = test_state(records);

        assert!(state.check_request("tn-plain").is_ok());
        assert!(state.check_request("tn-plain").is_ok());
        assert_eq!(state.check_request("tn-plain"), Err(ThrottleBranch::MinuteReq));

        for _ in 0..5 {
            assert!(state.check_request("tn-vip").is_ok());
        }
        assert_eq!(state.check_request("tn-vip"), Err(ThrottleBranch::MinuteReq));
    }

    #[test]
    fn weighted_token_bucket_exhausts_then_rejects() {
        let state = test_state(vec![limits_record(None, 1_000_000, 1_000_000, 100, 100_000)]);
        assert!(state.check_request("tn-b").is_ok());
        state.record_weighted_tokens("tn-b", 150);
        assert_eq!(state.check_request("tn-b"), Err(ThrottleBranch::MinuteTok));
    }

    #[test]
    fn bucket_refills_after_window_elapses() {
        let state = test_state(vec![limits_record(None, 2, 1000, 1_000_000, 1_000_000)]);
        assert!(state.check_request("tn-c").is_ok());
        assert!(state.check_request("tn-c").is_ok());
        assert_eq!(state.check_request("tn-c"), Err(ThrottleBranch::MinuteReq));

        {
            let mut entry = state.buckets.get_mut("tn-c").unwrap();
            entry.minute_req.last_refill =
                Instant::now().checked_sub(Duration::from_secs(70)).unwrap();
        }
        assert!(state.check_request("tn-c").is_ok());
    }
}
