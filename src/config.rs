use std::{io, io::Write, time::Duration};

use kcp::Kcp;

use crate::fec::FEC_HEADER_SIZE_PLUS_SIZE;

const MIN_KCP_MTU: usize = 50;

/// Kcp Delay Config
#[derive(Debug, Clone, Copy)]
pub struct KcpNoDelayConfig {
    /// Enable nodelay
    pub nodelay: bool,
    /// Internal update interval (ms)
    pub interval: i32,
    /// ACK number to enable fast resend
    pub resend: i32,
    /// Disable congetion control
    pub nc: bool,
}

impl Default for KcpNoDelayConfig {
    fn default() -> KcpNoDelayConfig {
        KcpNoDelayConfig {
            nodelay: false,
            interval: 100,
            resend: 0,
            nc: false,
        }
    }
}

impl KcpNoDelayConfig {
    /// Get a fastest configuration
    ///
    /// 1. Enable NoDelay
    /// 2. Set ticking interval to be 10ms
    /// 3. Set fast resend to be 2
    /// 4. Disable congestion control
    pub const fn fastest() -> KcpNoDelayConfig {
        KcpNoDelayConfig {
            nodelay: true,
            interval: 10,
            resend: 2,
            nc: true,
        }
    }

    /// Get a normal configuration
    ///
    /// 1. Disable NoDelay
    /// 2. Set ticking interval to be 40ms
    /// 3. Disable fast resend
    /// 4. Enable congestion control
    pub const fn normal() -> KcpNoDelayConfig {
        KcpNoDelayConfig {
            nodelay: false,
            interval: 40,
            resend: 0,
            nc: false,
        }
    }
}

/// kcp-go compatible FEC config.
#[derive(Debug, Clone, Copy, Default)]
pub struct KcpFecConfig {
    /// Number of data shards. `0` disables FEC.
    pub data_shards: usize,
    /// Number of parity shards. `0` disables FEC.
    pub parity_shards: usize,
}

impl KcpFecConfig {
    pub const fn new(data_shards: usize, parity_shards: usize) -> KcpFecConfig {
        KcpFecConfig {
            data_shards,
            parity_shards,
        }
    }

    pub const fn disabled() -> KcpFecConfig {
        KcpFecConfig {
            data_shards: 0,
            parity_shards: 0,
        }
    }

    pub const fn enabled(&self) -> bool {
        self.data_shards > 0 && self.parity_shards > 0
    }

    pub fn validate(&self) -> io::Result<()> {
        if self.data_shards == 0 && self.parity_shards == 0 {
            return Ok(());
        }

        if self.data_shards == 0 || self.parity_shards == 0 || self.data_shards + self.parity_shards > 256 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid FEC shards"));
        }

        Ok(())
    }
}

/// Kcp Config
#[derive(Debug, Clone, Copy)]
pub struct KcpConfig {
    /// Max Transmission Unit
    pub mtu: usize,
    /// nodelay
    pub nodelay: KcpNoDelayConfig,
    /// Send window size
    pub wnd_size: (u16, u16),
    /// Session expire duration, default is 90 seconds
    pub session_expire: Option<Duration>,
    /// Flush KCP state immediately after write
    pub flush_write: bool,
    /// Flush ACKs immediately after input
    pub flush_acks_input: bool,
    /// Stream mode
    pub stream: bool,
    /// Allow recv 0 byte packet. KCP Segments with 0 byte data are skipped by default.
    pub allow_recv_empty_packet: bool,
    /// kcp-go compatible FEC config. Disabled by default.
    pub fec: KcpFecConfig,
}

impl Default for KcpConfig {
    fn default() -> KcpConfig {
        KcpConfig {
            mtu: 1400,
            nodelay: KcpNoDelayConfig::normal(),
            wnd_size: (256, 256),
            session_expire: Some(Duration::from_secs(90)),
            flush_write: false,
            flush_acks_input: false,
            stream: false,
            allow_recv_empty_packet: false,
            fec: KcpFecConfig::disabled(),
        }
    }
}

impl KcpConfig {
    pub fn with_fec(mut self, data_shards: usize, parity_shards: usize) -> KcpConfig {
        self.fec = KcpFecConfig::new(data_shards, parity_shards);
        self
    }

    pub fn validate(&self) -> io::Result<()> {
        self.fec.validate()?;

        let mtu = if self.fec.enabled() {
            self.mtu
                .checked_sub(FEC_HEADER_SIZE_PLUS_SIZE)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "MTU too small for FEC header"))?
        } else {
            self.mtu
        };

        if mtu < MIN_KCP_MTU {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid MTU"));
        }

        Ok(())
    }

    /// Applies config onto `Kcp`
    #[doc(hidden)]
    pub fn apply_config<W: Write>(&self, k: &mut Kcp<W>) {
        self.validate().expect("invalid KCP config");

        let mtu = if self.fec.enabled() {
            self.mtu.checked_sub(FEC_HEADER_SIZE_PLUS_SIZE).expect("invalid MTU")
        } else {
            self.mtu
        };
        k.set_mtu(mtu).expect("invalid MTU");

        k.set_nodelay(
            self.nodelay.nodelay,
            self.nodelay.interval,
            self.nodelay.resend,
            self.nodelay.nc,
        );

        k.set_wndsize(self.wnd_size.0, self.wnd_size.1);
    }
}
