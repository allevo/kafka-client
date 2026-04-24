use std::collections::HashMap;
use std::sync::Mutex;

use kafka_protocol::messages::TopicName;
use murmur2::{KAFKA_SEED, murmur2};
use tracing::warn;

use crate::client::{PartitionId, TopicMetadata};
use crate::producer::ProducerRecord;

pub(crate) struct StickyState {
    per_topic: Mutex<HashMap<TopicName, PartitionId>>,
}

impl StickyState {
    pub(crate) fn new() -> Self {
        Self {
            per_topic: Mutex::new(HashMap::new()),
        }
    }

    /// Advance the sticky partition for `topic` to the next id modulo
    /// `num_partitions`. Called by the accumulator when the current sticky
    /// batch freezes (no caller yet in this slice). Deterministic rotation —
    /// v1 trades Java's random reshuffle for simplicity; fine as long as all
    /// producers don't converge on the same partition, which they don't
    /// because each holds its own `StickyState`.
    pub(crate) fn rotate(&self, topic: &TopicName, num_partitions: i32) -> Option<PartitionId> {
        if num_partitions <= 0 {
            return None;
        }
        let mut guard = self.per_topic.lock().unwrap();
        let next = match guard.get(topic) {
            Some(current) => PartitionId((current.0 + 1).rem_euclid(num_partitions)),
            None => PartitionId(0),
        };
        guard.insert(topic.clone(), next);
        Some(next)
    }
}

/// Assign a partition for `record` against `topic`. Returns `None` only when
/// `topic.partitions` is empty — caller treats that as metadata-unknown and
/// triggers a refresh.
pub(crate) fn partition_for(
    record: &ProducerRecord,
    topic: &TopicMetadata,
    sticky: &StickyState,
) -> Option<PartitionId> {
    if topic.partitions.is_empty() {
        return None;
    }

    // Explicit partition wins unconditionally. Range/validity is the
    // caller's problem.
    if let Some(p) = record.partition {
        return Some(p);
    }

    let Ok(num_partitions) = u32::try_from(topic.partitions.len()) else {
        warn!("Number of partitions are too big for u32");
        return None;
    };

    if let Some(key) = record.key.as_ref() {
        // Remove the sign-bit, so be sure i32 casting doesn't return negative value which doesn't not make any sense.
        let h = murmur2(key.as_ref(), KAFKA_SEED) & 0x7fff_ffff;
        return Some(PartitionId((h % num_partitions) as i32));
    }

    let mut guard = sticky.per_topic.lock().unwrap();
    let entry = guard.entry(record.topic.clone()).or_insert(PartitionId(0));
    // If partition count shrank under us between calls, snap back in-range.
    if entry.0 >= num_partitions as i32 || entry.0 < 0 {
        *entry = PartitionId(0);
    }
    Some(*entry)
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use kafka_protocol::ResponseError;
    use kafka_protocol::messages::BrokerId;
    use kafka_protocol::protocol::StrBytes;

    use crate::client::{PartitionId, PartitionMetadata};

    /// Wrap the crate's u32 output as Java's signed int so we can compare
    /// directly to Java's reference values. Same bit pattern, different
    /// interpretation.
    fn murmur2_java(data: &[u8]) -> i32 {
        murmur2(data, KAFKA_SEED) as i32
    }

    fn topic_name(name: &'static str) -> TopicName {
        TopicName(StrBytes::from_static_str(name))
    }

    fn topic_with(n: usize) -> TopicMetadata {
        let partitions = (0..n as i32)
            .map(|i| {
                (
                    PartitionId(i),
                    PartitionMetadata {
                        leader: BrokerId(i),
                        leader_epoch: 0,
                    },
                )
            })
            .collect();
        TopicMetadata {
            partitions,
            error: Option::<ResponseError>::None,
        }
    }

    fn record(topic: TopicName) -> ProducerRecord {
        ProducerRecord::new(topic, Bytes::from_static(b"v"))
    }

    // Reference vectors lifted verbatim from
    // org.apache.kafka.common.utils.UtilsTest.testMurmur2 (Apache Kafka,
    // clients/src/test/java/org/apache/kafka/common/utils/UtilsTest.java).
    // Locks us to Java's partition assignment so a record keyed from a
    // Java producer lands on the same partition as one from us.
    #[test]
    fn murmur2_reference_vectors() {
        assert_eq!(murmur2_java(b"21"), -973_932_308);
        assert_eq!(murmur2_java(b"foobar"), -790_332_482);
        assert_eq!(murmur2_java(b"a-little-bit-long-string"), -985_981_536);
        assert_eq!(murmur2_java(b"a-little-bit-longer-string"), -1_486_304_829);
        assert_eq!(
            murmur2_java(b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"),
            -58_897_971
        );
        assert_eq!(murmur2_java(b"abc"), 479_470_107);
    }

    // Reference vectors lifted verbatim from librdkafka's `unittest_murmur2`
    // (librdkafka/src/rdmurmur2.c). Exercises the aligned path ("kafka",
    // "giberish123456789"), the unaligned variant (offsets 1..=3 into a
    // 4-byte and a long string — the C implementation branches on
    // pointer alignment), and both the empty-slice and NULL-pointer cases
    // (the C code treats NULL as length 0, producing the same hash as "").
    // Values are the i32 interpretation of the u32 bit pattern the C test
    // checks against, so matching them cross-verifies against the
    // C client in addition to the JVM one.
    #[test]
    fn murmur2_librdkafka_reference_vectors() {
        let short_unaligned: &[u8] = b"1234";
        let unaligned: &[u8] = b"PreAmbleWillBeRemoved,ThePrePartThatIs";

        let cases: &[(&[u8], u32)] = &[
            (b"kafka", 0xd067cf64),
            (b"giberish123456789", 0x8f552b0c),
            (short_unaligned, 0x9fc97b14),
            (&short_unaligned[1..], 0xe7c009ca),
            (&short_unaligned[2..], 0x873930da),
            (&short_unaligned[3..], 0x5a4b5ca1),
            (unaligned, 0x78424f1c),
            (&unaligned[1..], 0x4a62b377),
            (&unaligned[2..], 0xe0e4e09e),
            (&unaligned[3..], 0x62b8b43f),
            (b"", 0x106e08d9),
            // librdkafka's test also feeds a NULL pointer; its code path
            // passes len=0, which is indistinguishable from the empty
            // slice above. Kept explicit for parity with the C suite.
            (&[], 0x106e08d9),
        ];

        for (key, expected) in cases {
            assert_eq!(
                murmur2(key, KAFKA_SEED),
                *expected,
                "mismatch for key {:?}",
                std::str::from_utf8(key).unwrap_or("<non-utf8>")
            );
        }
    }

    /// Faithful port of `java.util.SplittableRandom`. We need the exact same
    /// pseudo-random byte stream as the JDK to reproduce Kafka's
    /// `UtilsTest.testMurmur2Checksum` expected sum. Algorithm and
    /// constants from OpenJDK 11u `SplittableRandom.java`:
    /// `GOLDEN_GAMMA = 0x9e3779b97f4a7c15`; `mix64` applies two
    /// multiplicative shifts; `nextBytes` emits each 64-bit word
    /// little-endian, with any trailing bytes taken from the low end of a
    /// final `nextLong()`.
    struct SplittableRandom {
        seed: u64,
    }

    impl SplittableRandom {
        const GAMMA: u64 = 0x9e3779b97f4a7c15;

        fn new(seed: u64) -> Self {
            Self { seed }
        }

        fn mix64(mut z: u64) -> u64 {
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            z ^ (z >> 31)
        }

        fn next_long(&mut self) -> u64 {
            self.seed = self.seed.wrapping_add(Self::GAMMA);
            Self::mix64(self.seed)
        }

        fn next_bytes(&mut self, bytes: &mut [u8]) {
            let len = bytes.len();
            let mut i = 0usize;
            for _ in 0..(len >> 3) {
                let mut rnd = self.next_long();
                for _ in 0..8 {
                    bytes[i] = rnd as u8;
                    rnd >>= 8;
                    i += 1;
                }
            }
            if i < len {
                let mut rnd = self.next_long();
                while i < len {
                    bytes[i] = rnd as u8;
                    rnd >>= 8;
                    i += 1;
                }
            }
        }
    }

    // Ported from UtilsTest.testMurmur2Checksum (same file as the reference
    // vectors above). Fuzzes lengths 0..=1000 with 100 trials each, seeded
    // deterministically, and sums the *unsigned* 32-bit hash values as u64.
    // Any drift in murmur2 semantics — endianness, tail handling, seed —
    // flips the checksum and fails loudly. Rust-side `SplittableRandom`
    // above must stay bit-identical to Java's for this to hold.
    #[test]
    fn murmur2_checksum_matches_java() {
        const NUM_TRIALS: usize = 100;
        const MAX_LEN: usize = 1000;
        const EXPECTED: u64 = 0xc3b8cf7c99fc;

        let mut rng = SplittableRandom::new(0);
        let mut checksum: u64 = 0;

        for len in 0..=MAX_LEN {
            let mut data = vec![0u8; len];
            for _ in 0..NUM_TRIALS {
                rng.next_bytes(&mut data);
                let hash = murmur2(&data, KAFKA_SEED);
                checksum = checksum.wrapping_add(hash as u64);
            }
        }

        assert_eq!(checksum, EXPECTED);
    }

    #[test]
    fn keyed_partitioning_is_deterministic() {
        let topic = topic_with(4);
        let rec = record(topic_name("t")).with_key(Bytes::from_static(b"hello"));
        let sticky = StickyState::new();

        let first = partition_for(&rec, &topic, &sticky).unwrap();
        for _ in 0..10 {
            assert_eq!(partition_for(&rec, &topic, &sticky).unwrap(), first);
        }
        assert!(first.0 >= 0 && first.0 < 4);
    }

    #[test]
    fn keyed_partitioning_changes_with_partition_count() {
        let rec = record(topic_name("t")).with_key(Bytes::from_static(b"foobar"));
        let sticky = StickyState::new();

        // murmur2("foobar") = -790_332_482. Strip sign bit → 1_357_151_166.
        // Mod 4 → 2; mod 7 → 0. Different counts → different partitions,
        // which is the point of the test — also guards against accidental
        // `% num_partitions` omission.
        let p4 = partition_for(&rec, &topic_with(4), &sticky).unwrap();
        let p7 = partition_for(&rec, &topic_with(7), &sticky).unwrap();
        assert_eq!(p4, PartitionId(2));
        assert_eq!(p7, PartitionId(0));
        assert_ne!(p4, p7);
    }

    #[test]
    fn explicit_partition_overrides_key_hashing() {
        let topic = topic_with(4);
        let rec = record(topic_name("t"))
            .with_key(Bytes::from_static(b"anything"))
            .with_partition(PartitionId(99));
        let sticky = StickyState::new();
        // 99 is out of range but the partitioner must not second-guess the
        // caller's explicit choice — validation is the caller's concern.
        assert_eq!(
            partition_for(&rec, &topic, &sticky).unwrap(),
            PartitionId(99)
        );
    }

    #[test]
    fn sticky_without_key_is_stable_until_rotation() {
        let topic = topic_with(4);
        let name = topic_name("t");
        let rec = record(name.clone());
        let sticky = StickyState::new();

        let first = partition_for(&rec, &topic, &sticky).unwrap();
        for _ in 0..5 {
            assert_eq!(partition_for(&rec, &topic, &sticky).unwrap(), first);
        }

        let rotated = sticky.rotate(&name, 4).unwrap();
        assert_ne!(rotated, first);
        assert_eq!(partition_for(&rec, &topic, &sticky).unwrap(), rotated);
    }

    #[test]
    fn sticky_rotation_cycles_through_partitions() {
        let name = topic_name("t");
        let sticky = StickyState::new();

        let seen: Vec<PartitionId> = (0..4).map(|_| sticky.rotate(&name, 4).unwrap()).collect();
        // rotate() from fresh state jumps to 0 first, then 1, 2, 3.
        assert_eq!(
            seen,
            vec![
                PartitionId(0),
                PartitionId(1),
                PartitionId(2),
                PartitionId(3)
            ]
        );
        assert_eq!(sticky.rotate(&name, 4).unwrap(), PartitionId(0));
    }

    #[test]
    fn empty_partitions_returns_none() {
        let topic = TopicMetadata {
            partitions: Vec::new(),
            error: Option::<ResponseError>::None,
        };
        let rec = record(topic_name("t"));
        let sticky = StickyState::new();
        assert!(partition_for(&rec, &topic, &sticky).is_none());
    }
}
