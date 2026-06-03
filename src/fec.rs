use std::{
    collections::HashMap,
    fmt,
    io::{self, ErrorKind},
    time::{Duration, Instant},
};

use reed_solomon_erasure::galois_8::ReedSolomon;

pub const FEC_HEADER_SIZE: usize = 6;
pub const FEC_HEADER_SIZE_PLUS_SIZE: usize = FEC_HEADER_SIZE + 2;
pub const TYPE_DATA: u16 = 0xf1;
pub const TYPE_PARITY: u16 = 0xf2;
const MAX_SHARD_SETS: u32 = 3;
const MAX_FEC_ENCODE_LATENCY: Duration = Duration::from_millis(500);

pub struct EncodedPackets {
    pub packets: Vec<Vec<u8>>,
}

pub struct FecEncoder {
    data_shards: usize,
    parity_shards: usize,
    shard_size: usize,
    paws: u32,
    next: u32,
    ts_latest_packet: Option<Instant>,
    shards: Vec<Vec<u8>>,
    codec: ReedSolomon,
}

impl FecEncoder {
    pub fn new(data_shards: usize, parity_shards: usize) -> io::Result<FecEncoder> {
        validate_shards(data_shards, parity_shards)?;

        let shard_size = data_shards + parity_shards;
        let codec = ReedSolomon::new(data_shards, parity_shards).map_err(fec_error)?;

        Ok(FecEncoder {
            data_shards,
            parity_shards,
            shard_size,
            paws: 0xffff_ffff / shard_size as u32 * shard_size as u32,
            next: 0,
            ts_latest_packet: None,
            shards: Vec::with_capacity(data_shards),
            codec,
        })
    }

    pub fn encode(&mut self, payload: &[u8]) -> io::Result<EncodedPackets> {
        if payload.len() + 2 > u16::MAX as usize {
            return Err(io::Error::new(ErrorKind::InvalidInput, "FEC payload too large"));
        }

        let mut data = Vec::with_capacity(FEC_HEADER_SIZE_PLUS_SIZE + payload.len());
        self.seal(&mut data, TYPE_DATA);
        data.extend_from_slice(&((payload.len() + 2) as u16).to_le_bytes());
        data.extend_from_slice(payload);

        self.shards.push(data[FEC_HEADER_SIZE..].to_vec());

        let mut packets = vec![data];
        let now = Instant::now();
        if self.shards.len() == self.data_shards {
            let continuous = self
                .ts_latest_packet
                .is_some_and(|ts_latest_packet| now.duration_since(ts_latest_packet) < MAX_FEC_ENCODE_LATENCY);

            if !continuous {
                self.shards.clear();
                self.skip_parity();
                self.ts_latest_packet = Some(now);
                return Ok(EncodedPackets { packets });
            }

            let mut max_len = 0;
            for shard in &self.shards {
                max_len = max_len.max(shard.len());
            }

            let mut shards = Vec::with_capacity(self.shard_size);
            for shard in self.shards.drain(..) {
                let mut shard = shard;
                shard.resize(max_len, 0);
                shards.push(shard);
            }
            for _ in 0..self.parity_shards {
                shards.push(vec![0; max_len]);
            }

            self.codec.encode(&mut shards).map_err(fec_error)?;

            for parity in &shards[self.data_shards..] {
                let mut packet = Vec::with_capacity(FEC_HEADER_SIZE + parity.len());
                self.seal(&mut packet, TYPE_PARITY);
                packet.extend_from_slice(parity);
                packets.push(packet);
            }
        }

        self.ts_latest_packet = Some(now);
        Ok(EncodedPackets { packets })
    }

    fn seal(&mut self, packet: &mut Vec<u8>, flag: u16) {
        packet.extend_from_slice(&self.next.to_le_bytes());
        packet.extend_from_slice(&flag.to_le_bytes());
        self.next = (self.next + 1) % self.paws;
    }

    fn skip_parity(&mut self) {
        self.next = (self.next + self.parity_shards as u32) % self.paws;
    }
}

pub struct FecDecoder {
    data_shards: usize,
    shard_size: usize,
    paws: u32,
    shard_set: HashMap<u32, ShardSet>,
    newest_shard_id: u32,
    codec: ReedSolomon,
}

struct ShardSet {
    shards: Vec<Option<Vec<u8>>>,
}

impl fmt::Debug for FecDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FecDecoder")
            .field("data_shards", &self.data_shards)
            .field("shard_size", &self.shard_size)
            .field("shard_set.len", &self.shard_set.len())
            .field("newest_shard_id", &self.newest_shard_id)
            .finish()
    }
}

impl FecDecoder {
    pub fn new(data_shards: usize, parity_shards: usize) -> io::Result<FecDecoder> {
        validate_shards(data_shards, parity_shards)?;

        let shard_size = data_shards + parity_shards;
        let codec = ReedSolomon::new(data_shards, parity_shards).map_err(fec_error)?;

        Ok(FecDecoder {
            data_shards,
            shard_size,
            paws: 0xffff_ffff / shard_size as u32 * shard_size as u32,
            shard_set: HashMap::new(),
            newest_shard_id: 0,
            codec,
        })
    }

    pub fn decode(&mut self, packet: &[u8]) -> io::Result<Vec<Vec<u8>>> {
        if packet.len() < FEC_HEADER_SIZE {
            return Err(io::Error::new(ErrorKind::InvalidInput, "FEC packet too short"));
        }

        let seq = seqid(packet);
        if seq >= self.paws {
            return Ok(Vec::new());
        }

        let shard_id = seq / self.shard_size as u32;
        let shard_index = seq as usize % self.shard_size;
        let shard_size = self.shard_size;
        let shard = self.shard_set.entry(shard_id).or_insert_with(|| ShardSet {
            shards: vec![None; shard_size],
        });

        if shard.shards[shard_index].is_some() {
            return Ok(Vec::new());
        }

        shard.shards[shard_index] = Some(packet[FEC_HEADER_SIZE..].to_vec());

        if shard_id.wrapping_sub(self.newest_shard_id) < 0x8000_0000 && shard_id > self.newest_shard_id {
            self.newest_shard_id = shard_id;
        }

        let present = shard.shards.iter().filter(|shard| shard.is_some()).count();
        if present < self.data_shards {
            self.discard_old_shards();
            return Ok(Vec::new());
        }

        let mut shard = self.shard_set.remove(&shard_id).unwrap();
        let mut missing_data = Vec::new();
        let mut max_len = 0;
        for (idx, data) in shard.shards.iter().enumerate() {
            if idx < self.data_shards && data.is_none() {
                missing_data.push(idx);
            }
            if let Some(data) = data {
                max_len = max_len.max(data.len());
            }
        }

        if missing_data.is_empty() {
            self.discard_old_shards();
            return Ok(Vec::new());
        }

        for data in shard.shards.iter_mut().flatten() {
            data.resize(max_len, 0);
        }

        self.codec.reconstruct_data(&mut shard.shards).map_err(fec_error)?;

        let mut recovered = Vec::new();
        for idx in missing_data {
            if let Some(data) = &shard.shards[idx] {
                if data.len() < 2 {
                    continue;
                }

                let size = u16::from_le_bytes([data[0], data[1]]) as usize;
                if size >= 2 && size <= data.len() {
                    recovered.push(data[2..size].to_vec());
                }
            }
        }

        self.discard_old_shards();
        Ok(recovered)
    }

    fn discard_old_shards(&mut self) {
        let newest = self.newest_shard_id;
        let shard_size = self.shard_size as u32;
        self.shard_set
            .retain(|shard_id, _| newest.wrapping_sub(*shard_id) <= MAX_SHARD_SETS * shard_size);
    }
}

pub fn is_fec_packet(packet: &[u8]) -> bool {
    matches!(fec_flag(packet), Some(TYPE_DATA | TYPE_PARITY))
}

pub fn is_data_packet(packet: &[u8]) -> bool {
    fec_flag(packet) == Some(TYPE_DATA)
}

pub fn is_parity_packet(packet: &[u8]) -> bool {
    fec_flag(packet) == Some(TYPE_PARITY)
}

pub fn data_payload(packet: &[u8]) -> Option<&[u8]> {
    if is_data_packet(packet) && packet.len() >= FEC_HEADER_SIZE_PLUS_SIZE {
        Some(&packet[FEC_HEADER_SIZE_PLUS_SIZE..])
    } else {
        None
    }
}

pub fn data_payload_mut(packet: &mut [u8]) -> Option<&mut [u8]> {
    if is_data_packet(packet) && packet.len() >= FEC_HEADER_SIZE_PLUS_SIZE {
        Some(&mut packet[FEC_HEADER_SIZE_PLUS_SIZE..])
    } else {
        None
    }
}

fn fec_flag(packet: &[u8]) -> Option<u16> {
    if packet.len() >= FEC_HEADER_SIZE {
        Some(u16::from_le_bytes([packet[4], packet[5]]))
    } else {
        None
    }
}

fn seqid(packet: &[u8]) -> u32 {
    u32::from_le_bytes([packet[0], packet[1], packet[2], packet[3]])
}

fn validate_shards(data_shards: usize, parity_shards: usize) -> io::Result<()> {
    if data_shards == 0 || parity_shards == 0 || data_shards + parity_shards > 256 {
        return Err(io::Error::new(ErrorKind::InvalidInput, "invalid FEC shards"));
    }
    Ok(())
}

fn fec_error<E>(err: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(ErrorKind::InvalidData, err)
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use super::{FecDecoder, FecEncoder, TYPE_DATA};

    #[test]
    fn data_packet_matches_kcp_go_layout() {
        let payload = b"\x01\x02\x03\x04\x51\x00\x00\x00";
        let mut encoder = FecEncoder::new(2, 1).unwrap();

        let encoded = encoder.encode(payload).unwrap();
        let data = &encoded.packets[0];

        assert_eq!(&data[..4], 0u32.to_le_bytes());
        assert_eq!(&data[4..6], TYPE_DATA.to_le_bytes());
        assert_eq!(&data[6..8], ((payload.len() + 2) as u16).to_le_bytes());
        assert_eq!(&data[8..], payload);
    }

    #[test]
    fn parity_packet_recovers_missing_data_packet() {
        let one = b"\x11\x22\x33\x44\x51\x00\x00\x00";
        let two = b"\x55\x66\x77\x88\x51\x00\x00\x00payload";
        let mut encoder = FecEncoder::new(2, 1).unwrap();
        let mut decoder = FecDecoder::new(2, 1).unwrap();

        let first = encoder.encode(one).unwrap();
        let second = encoder.encode(two).unwrap();

        assert!(first.packets.len() == 1);
        assert!(decoder.decode(&second.packets[0]).unwrap().is_empty());
        let recovered = decoder.decode(&second.packets[1]).unwrap();

        assert_eq!(recovered, vec![one.to_vec()]);
    }

    #[test]
    fn low_frequency_data_skips_parity_like_kcp_go() {
        let mut encoder = FecEncoder::new(2, 1).unwrap();

        let first = encoder.encode(b"\x11\x22\x33\x44\x51\x00\x00\x00").unwrap();
        thread::sleep(Duration::from_millis(510));
        let second = encoder.encode(b"\x55\x66\x77\x88\x51\x00\x00\x00").unwrap();
        let third = encoder.encode(b"\x99\xaa\xbb\xcc\x51\x00\x00\x00").unwrap();

        assert_eq!(first.packets.len(), 1);
        assert_eq!(second.packets.len(), 1);
        assert_eq!(&third.packets[0][..4], 3u32.to_le_bytes());
    }
}
