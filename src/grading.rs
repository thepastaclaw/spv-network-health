//! Turns raw [`ProbeMetrics`] into a [`Grade`].
//!
//! Five dimensions, each scored 0–100 and combined with fixed weights:
//!
//! | dimension    | weight | measures                                          |
//! |--------------|--------|---------------------------------------------------|
//! | completeness | 0.45   | fraction of requested data the node actually served |
//! | throughput   | 0.20   | headers served per second (100 pts at ≥2500/s)    |
//! | reliability  | 0.20   | timeouts and validation failures                  |
//! | latency      | 0.10   | average ping round-trip                           |
//! | connectivity | 0.05   | TCP connect + protocol handshake time             |
//!
//! On top of the weighted average, incompleteness CAPS the score: a node
//! that failed to serve the requested range cannot grade well no matter how
//! fast it was while it lasted — a syncing wallet would have hung on it.
//! Any shortfall caps at C, serving under half caps at D, under a tenth is
//! an F outright.
//!
//! Dimensions whose measurement is unavailable (no ping data, never
//! connected) are excluded and the remaining weights renormalized, so a
//! missing measurement doesn't read as a bad one.

use std::time::Duration;

use crate::types::{Grade, LetterGrade, ProbeMetrics};

const W_COMPLETENESS: f64 = 0.45;
const W_THROUGHPUT: f64 = 0.20;
const W_RELIABILITY: f64 = 0.20;
const W_LATENCY: f64 = 0.10;
const W_CONNECTIVITY: f64 = 0.05;

/// Headers/sec at (or above) which throughput scores 100. Calibrated from
/// mainnet runs at depth ~50k: healthy nodes serve 700–5000/s, so full marks
/// sit near the top of the observed range instead of below the bottom.
const FULL_MARKS_HEADERS_PER_SEC: f64 = 2500.0;
/// Ping at (or below) which latency scores 100, and at which it scores 0.
const FULL_MARKS_PING: Duration = Duration::from_millis(50);
const ZERO_MARKS_PING: Duration = Duration::from_millis(1000);
/// Connect + handshake at (or below) which connectivity scores 100 / 0.
const FULL_MARKS_CONNECT: Duration = Duration::from_millis(150);
const ZERO_MARKS_CONNECT: Duration = Duration::from_secs(5);
/// Penalty per timeout / per validation failure (reliability starts at 100).
const TIMEOUT_PENALTY: f64 = 25.0;
const VALIDATION_FAILURE_PENALTY: f64 = 50.0;

/// Score caps for nodes that didn't serve the whole requested range:
/// any shortfall → at best a C, under half served → at best a D, under a
/// tenth → F territory.
const INCOMPLETE_SCORE_CAP: f64 = 69.0;
const UNDER_HALF_SCORE_CAP: f64 = 49.0;
const UNDER_TENTH_SCORE_CAP: f64 = 29.0;

/// Grade a completed (or partially completed) probe.
pub fn grade(m: &ProbeMetrics) -> Grade {
    let served_fraction = if m.headers_target == 0 {
        0.0
    } else {
        f64::from(m.headers_synced.min(m.headers_target)) / f64::from(m.headers_target)
    };
    let completeness = 100.0 * served_fraction;

    let throughput = 100.0 * (m.headers_per_sec / FULL_MARKS_HEADERS_PER_SEC).clamp(0.0, 1.0);

    let reliability = (100.0
        - f64::from(m.timeouts) * TIMEOUT_PENALTY
        - f64::from(m.validation_failures) * VALIDATION_FAILURE_PENALTY)
        .max(0.0);

    let latency = m
        .avg_ping
        .map(|p| inverse_scale(p, FULL_MARKS_PING, ZERO_MARKS_PING));

    let connect_total = match (m.connect_time, m.handshake_time) {
        (Some(c), Some(h)) => Some(c + h),
        (Some(c), None) => Some(c),
        _ => None,
    };
    let connectivity =
        connect_total.map(|t| inverse_scale(t, FULL_MARKS_CONNECT, ZERO_MARKS_CONNECT));

    // Weighted average over the dimensions that were actually measured.
    let mut weighted =
        completeness * W_COMPLETENESS + throughput * W_THROUGHPUT + reliability * W_RELIABILITY;
    let mut total_weight = W_COMPLETENESS + W_THROUGHPUT + W_RELIABILITY;
    if let Some(latency) = latency {
        weighted += latency * W_LATENCY;
        total_weight += W_LATENCY;
    }
    if let Some(connectivity) = connectivity {
        weighted += connectivity * W_CONNECTIVITY;
        total_weight += W_CONNECTIVITY;
    }
    let mut score = weighted / total_weight;

    // Failing to serve the requested range dominates everything else.
    let cap = if served_fraction >= 1.0 {
        100.0
    } else if served_fraction >= 0.5 {
        INCOMPLETE_SCORE_CAP
    } else if served_fraction >= 0.1 {
        UNDER_HALF_SCORE_CAP
    } else {
        UNDER_TENTH_SCORE_CAP
    };
    score = score.min(cap);

    Grade {
        score,
        letter: letter_for(score),
        connectivity,
        throughput,
        completeness,
        latency,
        reliability,
    }
}

/// 100 at `best` or faster, 0 at `worst` or slower, linear in between.
fn inverse_scale(value: Duration, best: Duration, worst: Duration) -> f64 {
    let v = value.as_secs_f64();
    let (b, w) = (best.as_secs_f64(), worst.as_secs_f64());
    if v <= b {
        100.0
    } else if v >= w {
        0.0
    } else {
        100.0 * (w - v) / (w - b)
    }
}

fn letter_for(score: f64) -> LetterGrade {
    match score {
        s if s >= 90.0 => LetterGrade::A,
        s if s >= 75.0 => LetterGrade::B,
        s if s >= 60.0 => LetterGrade::C,
        s if s >= 40.0 => LetterGrade::D,
        _ => LetterGrade::F,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn perfect_metrics() -> ProbeMetrics {
        ProbeMetrics {
            connect_time: Some(Duration::from_millis(40)),
            handshake_time: Some(Duration::from_millis(60)),
            avg_ping: Some(Duration::from_millis(30)),
            headers_synced: 1000,
            headers_target: 1000,
            headers_per_sec: 3000.0,
            total_time: Duration::from_secs(2),
            ..Default::default()
        }
    }

    #[test]
    fn perfect_node_gets_an_a() {
        let g = grade(&perfect_metrics());
        assert!(g.score > 95.0, "score was {}", g.score);
        assert_eq!(g.letter, LetterGrade::A);
    }

    #[test]
    fn unreachable_node_gets_an_f() {
        // Node never connected: latency/connectivity are unmeasured and the
        // remaining dimensions are all zero except reliability.
        let g = grade(&ProbeMetrics {
            headers_target: 1000,
            ..Default::default()
        });
        assert!(g.score <= 40.0, "score was {}", g.score);
        assert_eq!(g.letter, LetterGrade::F);
        assert_eq!(g.completeness, 0.0);
        assert_eq!(g.latency, None);
        assert_eq!(g.connectivity, None);
    }

    #[test]
    fn missing_ping_is_excluded_not_zeroed() {
        let mut with_ping = perfect_metrics();
        with_ping.avg_ping = Some(Duration::from_millis(10));
        let mut without_ping = perfect_metrics();
        without_ping.avg_ping = None;
        let a = grade(&with_ping);
        let b = grade(&without_ping);
        // A perfect node without ping data should not score worse than one
        // with a perfect ping.
        assert!(
            (a.score - b.score).abs() < 1.0,
            "with ping: {}, without: {}",
            a.score,
            b.score
        );
    }

    #[test]
    fn validation_failures_tank_reliability() {
        let mut m = perfect_metrics();
        m.validation_failures = 2;
        let g = grade(&m);
        assert_eq!(g.reliability, 0.0);
        assert!(
            g.letter > LetterGrade::A,
            "bad data should cost the top grade"
        );
    }

    #[test]
    fn partial_sync_scores_partially() {
        let mut m = perfect_metrics();
        m.headers_synced = 500;
        let g = grade(&m);
        assert!((g.completeness - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn any_shortfall_caps_at_c() {
        // Otherwise-perfect node that served 90% then stalled: a syncing
        // wallet would have hung on it, so speed can't buy back the grade.
        let mut m = perfect_metrics();
        m.headers_synced = 900;
        m.timeouts = 1;
        let g = grade(&m);
        assert!(g.score <= INCOMPLETE_SCORE_CAP, "score was {}", g.score);
        assert!(g.letter >= LetterGrade::C, "got {:?}", g.letter);
    }

    #[test]
    fn under_half_served_caps_at_d() {
        // Mirrors the observed mainnet stalls: ~35% served, fast while it
        // lasted, one stall timeout. Previously graded C — should be D.
        let mut m = perfect_metrics();
        m.headers_synced = 352;
        m.timeouts = 1;
        let g = grade(&m);
        assert!(g.score <= UNDER_HALF_SCORE_CAP, "score was {}", g.score);
        assert!(g.letter >= LetterGrade::D, "got {:?}", g.letter);
    }

    #[test]
    fn under_a_tenth_served_is_an_f() {
        let mut m = perfect_metrics();
        m.headers_synced = 50;
        m.timeouts = 1;
        let g = grade(&m);
        assert_eq!(g.letter, LetterGrade::F, "score was {}", g.score);
    }

    #[test]
    fn throughput_differentiates_below_full_marks() {
        let mut slow = perfect_metrics();
        slow.headers_per_sec = 800.0;
        let mut fast = perfect_metrics();
        fast.headers_per_sec = 2600.0;
        let slow_grade = grade(&slow);
        let fast_grade = grade(&fast);
        assert!(
            fast_grade.score > slow_grade.score + 5.0,
            "fast {} vs slow {}",
            fast_grade.score,
            slow_grade.score
        );
    }
}
