use crate::gorilla::{ChunkDecoder, ChunkEncoder};
use serde::Serialize;
use std::array;
use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const NUM_SHARDS: usize = 16;

#[derive(Debug)]
pub struct InputPoint {
    pub name: String,
    pub value: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct IngestResult {
    pub accepted: usize,
    pub rejected: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct StoreStats {
    pub series: usize,
    pub points: usize,
    pub bytes: usize,
}

pub struct Store {
    shards: [RwLock<HashMap<String, Arc<Series>>>; NUM_SHARDS],
    retention_ms: i64,
    chunk_window_ms: i64,
}

struct Series {
    head: Mutex<HeadChunk>,
    sealed: RwLock<VecDeque<Arc<SealedChunk>>>,
}

struct HeadChunk {
    bucket_start: i64,
    encoder: ChunkEncoder,
}

struct SealedChunk {
    start_ts: i64,
    end_ts: i64,
    last_value: f64,
    bytes: Vec<u8>,
    num_points: usize,
}

impl HeadChunk {
    fn empty() -> Self {
        Self {
            bucket_start: 0,
            encoder: ChunkEncoder::new(),
        }
    }

    fn new(bucket_start: i64) -> Self {
        Self {
            bucket_start,
            encoder: ChunkEncoder::new(),
        }
    }
}

impl Series {
    fn new() -> Self {
        Self {
            head: Mutex::new(HeadChunk::empty()),
            sealed: RwLock::new(VecDeque::new()),
        }
    }
}

impl Store {
    pub fn new(retention: Duration, chunk_window: Duration) -> Self {
        let retention_ms = duration_millis(retention);
        let chunk_window_ms = duration_millis(chunk_window);
        assert!(retention_ms > 0, "retention must be greater than zero");
        assert!(
            chunk_window_ms > 0,
            "chunk window must be greater than zero"
        );

        Self {
            shards: array::from_fn(|_| RwLock::new(HashMap::new())),
            retention_ms,
            chunk_window_ms,
        }
    }

    pub fn ingest(&self, batch: Vec<InputPoint>) -> IngestResult {
        self.ingest_at(batch, current_time_ms())
    }

    pub fn ingest_at(&self, batch: Vec<InputPoint>, now_ms: i64) -> IngestResult {
        let cutoff = now_ms.saturating_sub(self.retention_ms);
        let mut result = IngestResult {
            accepted: 0,
            rejected: 0,
        };

        for point in batch {
            if !point.value.is_finite() || point.timestamp < cutoff {
                result.rejected += 1;
                continue;
            }

            let series = self.get_or_create_series(&point.name);
            if self.append(&series, point.timestamp, point.value, cutoff) {
                result.accepted += 1;
            } else {
                result.rejected += 1;
            }
        }

        result
    }

    pub fn series_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for shard in &self.shards {
            names.extend(
                shard
                    .read()
                    .expect("series shard lock poisoned")
                    .keys()
                    .cloned(),
            );
        }
        names.sort_unstable();
        names
    }

    pub fn expand(&self, pattern: &str) -> Vec<String> {
        if !pattern.contains('*') {
            return self
                .find_series(pattern)
                .map(|_| vec![pattern.to_owned()])
                .unwrap_or_default();
        }

        let pattern_segments: Vec<_> = pattern.split('.').collect();
        let mut names = Vec::new();
        for shard in &self.shards {
            names.extend(
                shard
                    .read()
                    .expect("series shard lock poisoned")
                    .keys()
                    .filter(|name| metric_matches(&pattern_segments, name))
                    .cloned(),
            );
        }
        names.sort_unstable();
        names
    }

    pub fn query(
        &self,
        name: &str,
        from_ms: i64,
        to_ms: i64,
        step_ms: i64,
    ) -> Option<Vec<(i64, Option<f64>)>> {
        let series = self.find_series(name)?;
        if step_ms <= 0 || to_ms <= from_ms {
            return Some(Vec::new());
        }

        let Some(span) = to_ms.checked_sub(from_ms) else {
            return Some(Vec::new());
        };
        let Some(bucket_count) = (span - 1)
            .checked_div(step_ms)
            .and_then(|count| count.checked_add(1))
            .and_then(|count| usize::try_from(count).ok())
        else {
            return Some(Vec::new());
        };
        let mut values = vec![None; bucket_count];
        let (mut last_value, chunks) = Self::snapshot_chunks(&series, from_ms, to_ms);

        for chunk in chunks {
            for (timestamp, value) in ChunkDecoder::new(&chunk.bytes, chunk.num_points) {
                if timestamp < from_ms {
                    last_value = Some(value);
                    continue;
                }
                if timestamp >= to_ms {
                    continue;
                }
                let index = usize::try_from((timestamp - from_ms) / step_ms)
                    .expect("bucket index must be non-negative");
                values[index] = Some(value);
            }
        }

        Some(
            values
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    let offset = i64::try_from(index)
                        .expect("bucket index must fit i64")
                        .saturating_mul(step_ms);
                    let timestamp = from_ms.saturating_add(offset);
                    last_value = value.or(last_value);
                    (timestamp, last_value)
                })
                .collect(),
        )
    }

    pub fn sweep(&self, now_ms: i64) {
        let cutoff = now_ms.saturating_sub(self.retention_ms);
        for shard in &self.shards {
            shard
                .write()
                .expect("series shard lock poisoned")
                .retain(|_, series| {
                    !Self::expire_and_is_empty(series, cutoff) || Arc::strong_count(series) > 1
                });
        }
    }

    pub fn stats(&self) -> StoreStats {
        let mut stats = StoreStats {
            series: 0,
            points: 0,
            bytes: 0,
        };

        for shard in &self.shards {
            let shard = shard.read().expect("series shard lock poisoned");
            stats.series += shard.len();
            for series in shard.values() {
                let head = series.head.lock().expect("series head lock poisoned");
                let sealed = series.sealed.read().expect("sealed chunks lock poisoned");
                stats.points += head.encoder.num_points();
                stats.bytes += head.encoder.byte_size();
                for chunk in sealed.iter() {
                    stats.points += chunk.num_points;
                    stats.bytes += chunk.bytes.len();
                }
            }
        }

        stats
    }

    fn append(&self, series: &Series, timestamp: i64, value: f64, cutoff: i64) -> bool {
        let bucket_start = timestamp.div_euclid(self.chunk_window_ms) * self.chunk_window_ms;
        let mut head = series.head.lock().expect("series head lock poisoned");

        if head
            .encoder
            .last_ts()
            .is_some_and(|last_ts| timestamp <= last_ts)
        {
            return false;
        }

        if head.encoder.num_points() == 0 {
            head.bucket_start = bucket_start;
        } else if bucket_start != head.bucket_start {
            let old_head = std::mem::replace(&mut *head, HeadChunk::new(bucket_start));
            let chunk = Arc::new(SealedChunk {
                start_ts: old_head
                    .encoder
                    .first_ts()
                    .expect("non-empty chunk must have a first timestamp"),
                end_ts: old_head
                    .encoder
                    .last_ts()
                    .expect("non-empty chunk must have a last timestamp"),
                last_value: old_head
                    .encoder
                    .last_value()
                    .expect("non-empty chunk must have a last value"),
                num_points: old_head.encoder.num_points(),
                bytes: old_head.encoder.into_bytes(),
            });
            let mut sealed = series.sealed.write().expect("sealed chunks lock poisoned");
            sealed.push_back(chunk);
            while sealed.front().is_some_and(|chunk| chunk.end_ts < cutoff) {
                sealed.pop_front();
            }
        }

        head.encoder.append(timestamp, value).is_ok()
    }

    fn snapshot_chunks(
        series: &Series,
        from_ms: i64,
        to_ms: i64,
    ) -> (Option<f64>, Vec<ChunkSnapshot>) {
        let head = series.head.lock().expect("series head lock poisoned");
        let sealed = series.sealed.read().expect("sealed chunks lock poisoned");
        let mut chunks = Vec::with_capacity(sealed.len() + 1);
        let mut last_value = sealed
            .iter()
            .rev()
            .find(|chunk| chunk.end_ts < from_ms)
            .map(|chunk| chunk.last_value);

        chunks.extend(
            sealed
                .iter()
                .filter(|chunk| chunk.end_ts >= from_ms && chunk.start_ts < to_ms)
                .map(|chunk| ChunkSnapshot {
                    bytes: chunk.bytes.clone(),
                    num_points: chunk.num_points,
                }),
        );

        if head
            .encoder
            .last_ts()
            .is_some_and(|timestamp| timestamp < from_ms)
        {
            last_value = head.encoder.last_value();
        }

        if head.encoder.num_points() != 0
            && head
                .encoder
                .first_ts()
                .is_some_and(|timestamp| timestamp < to_ms)
        {
            chunks.push(ChunkSnapshot {
                bytes: head.encoder.bytes(),
                num_points: head.encoder.num_points(),
            });
        }

        (last_value, chunks)
    }

    fn expire_and_is_empty(series: &Series, cutoff: i64) -> bool {
        let mut head = series.head.lock().expect("series head lock poisoned");
        let mut sealed = series.sealed.write().expect("sealed chunks lock poisoned");

        while sealed.front().is_some_and(|chunk| chunk.end_ts < cutoff) {
            sealed.pop_front();
        }
        if head
            .encoder
            .last_ts()
            .is_some_and(|timestamp| timestamp < cutoff)
        {
            *head = HeadChunk::empty();
        }

        head.encoder.num_points() == 0 && sealed.is_empty()
    }

    fn get_or_create_series(&self, name: &str) -> Arc<Series> {
        let index = shard_index(name);
        if let Some(series) = self.shards[index]
            .read()
            .expect("series shard lock poisoned")
            .get(name)
            .cloned()
        {
            return series;
        }

        self.shards[index]
            .write()
            .expect("series shard lock poisoned")
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(Series::new()))
            .clone()
    }

    fn find_series(&self, name: &str) -> Option<Arc<Series>> {
        self.shards[shard_index(name)]
            .read()
            .expect("series shard lock poisoned")
            .get(name)
            .cloned()
    }
}

struct ChunkSnapshot {
    bytes: Vec<u8>,
    num_points: usize,
}

fn shard_index(name: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    hasher.finish() as usize % NUM_SHARDS
}

fn metric_matches(pattern_segments: &[&str], name: &str) -> bool {
    let mut name_segments = name.split('.');
    pattern_segments.iter().all(|pattern| {
        name_segments
            .next()
            .is_some_and(|segment| segment_matches(pattern, segment))
    }) && name_segments.next().is_none()
}

pub(crate) fn segment_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut pattern_index, mut value_index) = (0, 0);
    let (mut star_index, mut star_value_index) = (None, 0);

    while value_index < value.len() {
        if pattern_index < pattern.len()
            && pattern[pattern_index] != b'*'
            && pattern[pattern_index] == value[value_index]
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            pattern_index += 1;
            star_value_index = value_index;
        } else if let Some(star) = star_index {
            pattern_index = star + 1;
            star_value_index += 1;
            value_index = star_value_index;
        } else {
            return false;
        }
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

fn duration_millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn current_time_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_millis(duration),
        Err(error) => -duration_millis(error.duration()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn point(name: &str, timestamp: i64, value: f64) -> InputPoint {
        InputPoint {
            name: name.to_owned(),
            value,
            timestamp,
        }
    }

    #[test]
    fn ingest_and_query_across_chunk_boundary() {
        let store = Store::new(Duration::from_secs(10), Duration::from_millis(100));
        let result = store.ingest_at(vec![point("cpu", 95, 1.0), point("cpu", 105, 2.0)], 110);

        assert_eq!(result.accepted, 2);
        assert_eq!(result.rejected, 0);
        let values: Vec<_> = store
            .query("cpu", 95, 110, 10)
            .unwrap()
            .into_iter()
            .filter_map(|(_, value)| value)
            .collect();
        assert_eq!(values, vec![1.0, 2.0]);

        let series = store.find_series("cpu").unwrap();
        assert_eq!(
            series
                .sealed
                .read()
                .expect("sealed chunks lock poisoned")
                .len(),
            1
        );
    }

    #[test]
    fn retention_expires_chunks_on_append_and_sweep() {
        let store = Store::new(Duration::from_millis(50), Duration::from_millis(10));
        assert_eq!(store.ingest_at(vec![point("cpu", 10, 1.0)], 10).accepted, 1);
        assert_eq!(store.ingest_at(vec![point("cpu", 70, 2.0)], 70).accepted, 1);

        let series = store.find_series("cpu").unwrap();
        assert!(
            series
                .sealed
                .read()
                .expect("sealed chunks lock poisoned")
                .is_empty()
        );
        assert_eq!(store.query("cpu", 0, 100, 10).unwrap()[7], (70, Some(2.0)));
        drop(series);

        store.sweep(121);
        assert!(store.query("cpu", 0, 200, 10).is_none());
        assert!(store.series_names().is_empty());
    }

    #[test]
    fn rejects_invalid_points_without_affecting_accepted_points() {
        let store = Store::new(Duration::from_millis(100), Duration::from_millis(50));
        assert_eq!(
            store.ingest_at(vec![point("cpu", 100, 1.0)], 100),
            IngestResult {
                accepted: 1,
                rejected: 0
            }
        );

        let result = store.ingest_at(
            vec![
                point("cpu", -1, 4.0),
                point("cpu", 100, 2.0),
                point("cpu", 101, f64::NAN),
                point("cpu", 102, 3.0),
            ],
            100,
        );
        assert_eq!(
            result,
            IngestResult {
                accepted: 1,
                rejected: 3
            }
        );

        let values: Vec<_> = store
            .query("cpu", 100, 103, 1)
            .unwrap()
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        assert_eq!(values, vec![Some(1.0), Some(1.0), Some(3.0)]);
    }

    #[test]
    fn downsampling_keeps_the_last_value_and_carries_it_forward() {
        let store = Store::new(Duration::from_secs(10), Duration::from_secs(1));
        assert_eq!(
            store
                .ingest_at(
                    vec![
                        point("load", 1_000, 1.0),
                        point("load", 1_100, 3.0),
                        point("load", 1_300, 5.0),
                    ],
                    1_300,
                )
                .accepted,
            3
        );

        assert_eq!(
            store.query("load", 800, 1_500, 200).unwrap(),
            vec![
                (800, None),
                (1_000, Some(3.0)),
                (1_200, Some(5.0)),
                (1_400, Some(5.0)),
            ]
        );

        let head_only = Store::new(Duration::from_secs(10), Duration::from_secs(10));
        assert_eq!(
            head_only
                .ingest_at(vec![point("load", 900, 11.0)], 900)
                .accepted,
            1
        );
        assert_eq!(
            head_only.query("load", 1_000, 1_300, 100).unwrap(),
            vec![
                (1_000, Some(11.0)),
                (1_100, Some(11.0)),
                (1_200, Some(11.0)),
            ]
        );
    }

    #[test]
    fn query_seeds_the_range_with_the_last_retained_value() {
        let store = Store::new(Duration::from_secs(10), Duration::from_secs(1));
        assert_eq!(
            store
                .ingest_at(
                    vec![point("load", 900, 7.0), point("load", 1_300, 5.0)],
                    1_300,
                )
                .accepted,
            2
        );

        assert_eq!(
            store.query("load", 1_000, 1_300, 100).unwrap(),
            vec![(1_000, Some(7.0)), (1_100, Some(7.0)), (1_200, Some(7.0)),]
        );
    }

    #[test]
    fn concurrent_ingest_same_and_different_series_is_accounted_for() {
        const THREADS: usize = 8;
        const POINTS: usize = 100;
        let store = Arc::new(Store::new(
            Duration::from_secs(100),
            Duration::from_millis(250),
        ));
        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::new();

        for worker in 0..THREADS {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let name = if worker < 4 {
                    "shared".to_owned()
                } else {
                    format!("worker.{worker}")
                };
                let base = i64::try_from(worker).unwrap() * 1_000;
                let batch = (0..POINTS)
                    .map(|offset| {
                        point(&name, base + i64::try_from(offset).unwrap(), offset as f64)
                    })
                    .collect();
                barrier.wait();
                store.ingest_at(batch, 10_000)
            }));
        }

        let total = handles
            .into_iter()
            .map(|handle| handle.join().expect("ingest thread panicked"))
            .fold(
                IngestResult {
                    accepted: 0,
                    rejected: 0,
                },
                |mut total, result| {
                    total.accepted += result.accepted;
                    total.rejected += result.rejected;
                    total
                },
            );

        assert_eq!(total.accepted + total.rejected, THREADS * POINTS);
        assert!(total.accepted >= 5 * POINTS);
        assert_eq!(store.stats().points, total.accepted);
    }

    #[test]
    fn names_are_sorted_and_stats_include_head_and_sealed_chunks() {
        let store = Store::new(Duration::from_secs(10), Duration::from_millis(10));
        store.ingest_at(
            vec![
                point("zeta", 10, 1.0),
                point("alpha", 10, 1.0),
                point("zeta", 20, 2.0),
            ],
            20,
        );

        assert_eq!(store.series_names(), vec!["alpha", "zeta"]);
        let stats = store.stats();
        assert_eq!(stats.series, 2);
        assert_eq!(stats.points, 3);
        assert!(stats.bytes > 0);
    }

    fn store_with_names(names: &[&str]) -> Store {
        let store = Store::new(Duration::from_secs(10), Duration::from_secs(1));
        store.ingest_at(
            names.iter().map(|name| point(name, 1_000, 1.0)).collect(),
            1_000,
        );
        store
    }

    #[test]
    fn expand_anchors_segments() {
        let store = store_with_names(&["demo.wave", "demo.wave.sine", "demo.rand"]);

        assert_eq!(store.expand("demo.*"), vec!["demo.rand", "demo.wave"]);
    }

    #[test]
    fn expand_matches_mid_segment_stars() {
        let store =
            store_with_names(&["web.frontend.cpu", "web.front.cpu_total", "web.backend.cpu"]);

        assert_eq!(
            store.expand("web.front*.cpu*"),
            vec!["web.front.cpu_total", "web.frontend.cpu"]
        );
    }

    #[test]
    fn expand_matches_multiple_stars_in_a_segment() {
        let store = store_with_names(&["service.frontend.cpu", "service.feed.cpu"]);

        assert_eq!(
            store.expand("service.f*r*e*d.cpu"),
            vec!["service.frontend.cpu"]
        );
    }

    #[test]
    fn expand_exact_names_and_missing_names() {
        let store = store_with_names(&["demo.sine"]);

        assert_eq!(store.expand("demo.sine"), vec!["demo.sine"]);
        assert!(store.expand("demo.cosine").is_empty());
    }

    #[test]
    fn expand_empty_store() {
        let store = Store::new(Duration::from_secs(10), Duration::from_secs(1));

        assert!(store.expand("*").is_empty());
    }

    #[test]
    fn expand_star_only_patterns_respect_depth() {
        let store = store_with_names(&["cpu", "demo.sine", "demo.wave.sine"]);

        assert_eq!(store.expand("*"), vec!["cpu"]);
        assert_eq!(store.expand("*.*"), vec!["demo.sine"]);
    }

    #[test]
    fn sweep_keeps_a_series_while_an_operation_holds_it() {
        let store = Store::new(Duration::from_millis(10), Duration::from_millis(5));
        store.ingest_at(vec![point("cpu", 10, 1.0)], 10);
        let active = store.find_series("cpu").unwrap();

        store.sweep(100);
        assert_eq!(store.series_names(), vec!["cpu"]);

        drop(active);
        store.sweep(100);
        assert!(store.series_names().is_empty());
    }
}
