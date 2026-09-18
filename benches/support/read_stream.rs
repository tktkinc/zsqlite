//! Deterministic round ordering shared by transcript read benchmarks.

pub const DEFAULT_ROUNDS: usize = 3;

/// Shuffle every case deterministically for one phase and round. The phase is
/// included so repeated benchmark phases do not replay an identical sequence;
/// the profile is deliberately excluded so all profiles see the same order.
pub fn order(names: &[&str], phase: &str, round: usize) -> Vec<usize> {
    let mut cases = names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let mut hash = blake3::Hasher::new();
            hash.update(b"zsqlite-transcript-stream-v1");
            hash.update(phase.as_bytes());
            hash.update(&round.to_le_bytes());
            hash.update(name.as_bytes());
            (index, *hash.finalize().as_bytes())
        })
        .collect::<Vec<_>>();
    cases.sort_by_key(|(index, hash)| (*hash, *index));
    cases.into_iter().map(|(index, _)| index).collect()
}
