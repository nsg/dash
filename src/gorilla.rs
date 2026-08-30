#[derive(Clone, Debug, Default)]
pub struct BitWriter {
    bytes: Vec<u8>,
    bit_len: usize,
}

impl BitWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn write_bit(&mut self, bit: bool) {
        let byte_index = self.bit_len / 8;
        let bit_index = 7 - self.bit_len % 8;
        if byte_index == self.bytes.len() {
            self.bytes.push(0);
        }
        if bit {
            self.bytes[byte_index] |= 1 << bit_index;
        }
        self.bit_len += 1;
    }

    pub fn write_bits(&mut self, value: u64, count: usize) {
        assert!(count <= 64);
        for shift in (0..count).rev() {
            self.write_bit(((value >> shift) & 1) != 0);
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Clone, Debug)]
pub struct BitReader<'a> {
    bytes: &'a [u8],
    bit_pos: usize,
}

impl<'a> BitReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, bit_pos: 0 }
    }

    pub fn read_bit(&mut self) -> Option<bool> {
        if self.bit_pos >= self.bytes.len() * 8 {
            return None;
        }
        let byte_index = self.bit_pos / 8;
        let bit_index = 7 - self.bit_pos % 8;
        self.bit_pos += 1;
        Some(((self.bytes[byte_index] >> bit_index) & 1) != 0)
    }

    pub fn read_bits(&mut self, count: usize) -> Option<u64> {
        if count > 64 || self.bytes.len() * 8 - self.bit_pos < count {
            return None;
        }
        let mut value = 0;
        for _ in 0..count {
            value = (value << 1) | u64::from(self.read_bit()?);
        }
        Some(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EncodeError {
    TimestampNotIncreasing,
    TimestampDeltaOutOfRange,
}

#[derive(Clone, Debug)]
pub struct ChunkEncoder {
    writer: BitWriter,
    num_points: usize,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
    last_delta: i64,
    last_value_bits: u64,
    value_window: Option<ValueWindow>,
}

#[derive(Clone, Copy, Debug)]
struct ValueWindow {
    leading: u32,
    meaningful: u32,
}

impl Default for ChunkEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkEncoder {
    pub fn new() -> Self {
        Self {
            writer: BitWriter::new(),
            num_points: 0,
            first_ts: None,
            last_ts: None,
            last_delta: 0,
            last_value_bits: 0,
            value_window: None,
        }
    }

    pub fn append(&mut self, timestamp: i64, value: f64) -> Result<(), EncodeError> {
        let value_bits = value.to_bits();
        let Some(previous_ts) = self.last_ts else {
            self.writer.write_bits(timestamp as u64, 64);
            self.writer.write_bits(value_bits, 64);
            self.num_points = 1;
            self.first_ts = Some(timestamp);
            self.last_ts = Some(timestamp);
            self.last_value_bits = value_bits;
            return Ok(());
        };

        if timestamp <= previous_ts {
            return Err(EncodeError::TimestampNotIncreasing);
        }

        let delta = timestamp
            .checked_sub(previous_ts)
            .ok_or(EncodeError::TimestampDeltaOutOfRange)?;
        let delta_of_delta = delta
            .checked_sub(self.last_delta)
            .ok_or(EncodeError::TimestampDeltaOutOfRange)?;
        if i32::try_from(delta_of_delta).is_err() {
            return Err(EncodeError::TimestampDeltaOutOfRange);
        }

        self.write_timestamp_delta(delta_of_delta);
        self.write_value(value_bits);
        self.num_points += 1;
        self.last_ts = Some(timestamp);
        self.last_delta = delta;
        self.last_value_bits = value_bits;
        Ok(())
    }

    fn write_timestamp_delta(&mut self, delta: i64) {
        match delta {
            0 => self.writer.write_bit(false),
            -63..=64 => {
                self.writer.write_bits(0b10, 2);
                self.writer.write_bits((delta + 63) as u64, 7);
            }
            -255..=256 => {
                self.writer.write_bits(0b110, 3);
                self.writer.write_bits((delta + 255) as u64, 9);
            }
            -2047..=2048 => {
                self.writer.write_bits(0b1110, 4);
                self.writer.write_bits((delta + 2047) as u64, 12);
            }
            _ => {
                self.writer.write_bits(0b1111, 4);
                self.writer.write_bits(delta as u32 as u64, 32);
            }
        }
    }

    fn write_value(&mut self, value_bits: u64) {
        let xor = value_bits ^ self.last_value_bits;
        if xor == 0 {
            self.writer.write_bit(false);
            return;
        }

        self.writer.write_bit(true);
        // Five bits cap leading zeros at 31; a zero six-bit length represents 64.
        // The 5-bit field caps leading zeroes at 31; extra zeroes become payload bits.
        let leading = xor.leading_zeros().min(31);
        let trailing = xor.trailing_zeros();
        if let Some(window) = self.value_window {
            let window_trailing = 64 - window.leading - window.meaningful;
            if leading >= window.leading && trailing >= window_trailing {
                self.writer.write_bit(false);
                self.writer
                    .write_bits(xor >> window_trailing, window.meaningful as usize);
                return;
            }
        }

        let meaningful = 64 - leading - trailing;
        self.writer.write_bit(true);
        self.writer.write_bits(u64::from(leading), 5);
        self.writer.write_bits(u64::from(meaningful % 64), 6);
        self.writer.write_bits(xor >> trailing, meaningful as usize);
        self.value_window = Some(ValueWindow {
            leading,
            meaningful,
        });
    }

    pub fn num_points(&self) -> usize {
        self.num_points
    }

    pub fn first_ts(&self) -> Option<i64> {
        self.first_ts
    }

    pub fn last_ts(&self) -> Option<i64> {
        self.last_ts
    }

    #[allow(dead_code)]
    pub fn last_value(&self) -> Option<f64> {
        (self.num_points != 0).then(|| f64::from_bits(self.last_value_bits))
    }

    pub fn byte_size(&self) -> usize {
        self.writer.as_bytes().len()
    }

    #[allow(dead_code)]
    pub fn as_bytes(&self) -> &[u8] {
        self.writer.as_bytes()
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.writer.as_bytes().to_vec()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.writer.into_bytes()
    }
}

#[derive(Clone, Debug)]
pub struct ChunkDecoder<'a> {
    reader: BitReader<'a>,
    remaining: usize,
    initialized: bool,
    last_ts: i64,
    last_delta: i64,
    last_value_bits: u64,
    value_window: Option<ValueWindow>,
}

impl<'a> ChunkDecoder<'a> {
    pub fn new(bytes: &'a [u8], num_points: usize) -> Self {
        Self {
            reader: BitReader::new(bytes),
            remaining: num_points,
            initialized: false,
            last_ts: 0,
            last_delta: 0,
            last_value_bits: 0,
            value_window: None,
        }
    }

    fn decode_timestamp_delta(&mut self) -> Option<i64> {
        if !self.reader.read_bit()? {
            return Some(0);
        }
        if !self.reader.read_bit()? {
            return Some(self.reader.read_bits(7)? as i64 - 63);
        }
        if !self.reader.read_bit()? {
            return Some(self.reader.read_bits(9)? as i64 - 255);
        }
        if !self.reader.read_bit()? {
            return Some(self.reader.read_bits(12)? as i64 - 2047);
        }
        Some(self.reader.read_bits(32)? as u32 as i32 as i64)
    }

    fn decode_value(&mut self) -> Option<u64> {
        if !self.reader.read_bit()? {
            return Some(self.last_value_bits);
        }

        let xor = if !self.reader.read_bit()? {
            let window = self.value_window?;
            let trailing = 64 - window.leading - window.meaningful;
            self.reader.read_bits(window.meaningful as usize)? << trailing
        } else {
            let leading = self.reader.read_bits(5)? as u32;
            let encoded_length = self.reader.read_bits(6)? as u32;
            let meaningful = if encoded_length == 0 {
                64
            } else {
                encoded_length
            };
            if leading + meaningful > 64 {
                return None;
            }
            let trailing = 64 - leading - meaningful;
            let bits = self.reader.read_bits(meaningful as usize)? << trailing;
            self.value_window = Some(ValueWindow {
                leading,
                meaningful,
            });
            bits
        };
        Some(self.last_value_bits ^ xor)
    }

    fn fail(&mut self) -> Option<(i64, f64)> {
        self.remaining = 0;
        None
    }
}

impl Iterator for ChunkDecoder<'_> {
    type Item = (i64, f64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        if !self.initialized {
            let Some(timestamp) = self.reader.read_bits(64).map(|bits| bits as i64) else {
                return self.fail();
            };
            let Some(value_bits) = self.reader.read_bits(64) else {
                return self.fail();
            };
            self.initialized = true;
            self.last_ts = timestamp;
            self.last_value_bits = value_bits;
            self.remaining -= 1;
            return Some((timestamp, f64::from_bits(value_bits)));
        }

        let Some(delta_of_delta) = self.decode_timestamp_delta() else {
            return self.fail();
        };
        let Some(delta) = self.last_delta.checked_add(delta_of_delta) else {
            return self.fail();
        };
        let Some(timestamp) = self.last_ts.checked_add(delta) else {
            return self.fail();
        };
        let Some(value_bits) = self.decode_value() else {
            return self.fail();
        };

        self.last_delta = delta;
        self.last_ts = timestamp;
        self.last_value_bits = value_bits;
        self.remaining -= 1;
        Some((timestamp, f64::from_bits(value_bits)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for ChunkDecoder<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_roundtrip(points: &[(i64, f64)]) -> ChunkEncoder {
        let mut encoder = ChunkEncoder::new();
        for &(timestamp, value) in points {
            encoder.append(timestamp, value).unwrap();
        }
        let decoded: Vec<_> = ChunkDecoder::new(encoder.as_bytes(), encoder.num_points()).collect();
        assert_eq!(decoded.len(), points.len());
        for ((actual_ts, actual_value), (expected_ts, expected_value)) in decoded.iter().zip(points)
        {
            assert_eq!(actual_ts, expected_ts);
            assert_eq!(actual_value.to_bits(), expected_value.to_bits());
        }
        encoder
    }

    #[test]
    fn bit_reader_mirrors_writer() {
        let mut writer = BitWriter::new();
        writer.write_bit(true);
        writer.write_bits(0b0101, 4);
        writer.write_bits(u64::MAX, 64);
        let mut reader = BitReader::new(writer.as_bytes());
        assert_eq!(reader.read_bit(), Some(true));
        assert_eq!(reader.read_bits(4), Some(0b0101));
        assert_eq!(reader.read_bits(64), Some(u64::MAX));
    }

    #[test]
    fn fixed_interval_sine_roundtrip() {
        let points: Vec<_> = (0..10_000)
            .map(|index| {
                let timestamp = 1_700_000_000_000 + index * 10_000;
                let value = f64::from((100.0 + ((index as f64) / 100.0).sin()) as f32);
                (timestamp, value)
            })
            .collect();
        assert_roundtrip(&points);
    }

    #[test]
    fn jittered_random_walk_roundtrip() {
        let mut state = 0x4d59_5df4_d0f3_3173_u64;
        let mut timestamp = 1_700_000_000_000_i64;
        let mut value = 0.0;
        let mut points = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            timestamp += 900 + (state % 201) as i64;
            value += ((state >> 32) as i32 as f64) / i32::MAX as f64;
            points.push((timestamp, value));
        }
        assert_roundtrip(&points);
    }

    #[test]
    fn constant_values_use_zero_xor_path() {
        let points: Vec<_> = (0..1_000)
            .map(|index| (10_000 + index * 1_000, -42.25))
            .collect();
        let encoder = assert_roundtrip(&points);
        assert!(encoder.byte_size() < 300);
    }

    #[test]
    fn alternating_extreme_values_rewrite_windows() {
        let values = [
            f64::from_bits(1),
            f64::MAX,
            -f64::MAX,
            f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE,
            f64::from_bits(0x000f_ffff_ffff_ffff),
        ];
        let points: Vec<_> = (0..1_000)
            .map(|index| (index as i64 + 1, values[index % values.len()]))
            .collect();
        assert_roundtrip(&points);
    }

    #[test]
    fn special_finite_value_bits_roundtrip() {
        let values = [
            0.0,
            -0.0,
            -1.5,
            f64::MAX,
            -f64::MAX,
            f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE,
            f64::from_bits(1),
            f64::from_bits(0x8000_0000_0000_0001),
        ];
        let points: Vec<_> = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| (1_000 + index as i64, value))
            .collect();
        assert_roundtrip(&points);
    }

    #[test]
    fn leading_zero_count_is_clamped_to_five_bits() {
        assert_roundtrip(&[(1, 0.0), (2, f64::from_bits(1))]);
    }

    #[test]
    fn timestamp_delta_bucket_boundaries_roundtrip() {
        let delta_of_deltas = [
            0, -63, 64, -64, 65, -255, 256, -256, 257, -2047, 2048, -2048, 2049, -100_000, 100_000,
        ];
        let mut timestamp = 1_700_000_000_000_i64;
        let mut delta = 200_000_i64;
        let mut points = vec![(timestamp, 1.0)];
        for (index, delta_of_delta) in delta_of_deltas.into_iter().enumerate() {
            delta += delta_of_delta;
            timestamp += delta;
            points.push((timestamp, index as f64 + 2.0));
        }
        assert_roundtrip(&points);
    }

    #[test]
    fn single_point_roundtrip_and_accessors() {
        let encoder = assert_roundtrip(&[(-123, -0.0)]);
        assert_eq!(encoder.num_points(), 1);
        assert_eq!(encoder.first_ts(), Some(-123));
        assert_eq!(encoder.last_ts(), Some(-123));
        assert_eq!(
            encoder.last_value().unwrap().to_bits(),
            (-0.0_f64).to_bits()
        );
        assert_eq!(encoder.byte_size(), 16);
        assert_eq!(encoder.bytes(), encoder.clone().into_bytes());
    }

    #[test]
    fn rejects_non_increasing_timestamps_without_mutation() {
        let mut encoder = ChunkEncoder::new();
        encoder.append(100, 1.0).unwrap();
        let bytes = encoder.bytes();
        assert_eq!(
            encoder.append(100, 2.0),
            Err(EncodeError::TimestampNotIncreasing)
        );
        assert_eq!(
            encoder.append(99, 2.0),
            Err(EncodeError::TimestampNotIncreasing)
        );
        assert_eq!(encoder.num_points(), 1);
        assert_eq!(encoder.as_bytes(), bytes);
    }

    #[test]
    fn fixed_interval_sine_compression_is_under_four_bytes_per_point() {
        let points: Vec<_> = (0..10_000)
            .map(|index| {
                let timestamp = 1_700_000_000_000 + index * 10_000;
                let value = f64::from((100.0 + ((index as f64) / 100.0).sin()) as f32);
                (timestamp, value)
            })
            .collect();
        let encoder = assert_roundtrip(&points);
        assert!(
            encoder.byte_size() < points.len() * 4,
            "{} bytes",
            encoder.byte_size()
        );
    }
}
