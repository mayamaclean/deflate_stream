use flamer::flame;
use std::ops::Deref;

#[derive(Copy, Clone)]
pub struct CrcRs {
    crc: u32,
    len: usize,
}

#[derive(Copy, Clone)]
pub struct CrcCf {
    crc: u64,
    len: usize,
}

impl Deref for CrcRs {
    type Target = u32;

    fn deref(&self) -> &u32 {
        &self.crc
    }
}

impl CrcRs {
    pub fn inner(&self) -> u32 {
        self.crc
    }
}

pub trait Crc32Calculator: Copy + Clone + Send + Sync + 'static {
    fn calculate(&self, data: &[u8]) -> CrcRs;
    fn combine(&self, crc1: CrcRs, crc2: CrcRs) -> CrcRs;
}

impl Deref for CrcCf {
    type Target = u64;

    fn deref(&self) -> &u64 {
        &self.crc
    }
}

impl CrcCf {
    pub fn inner(&self) -> u64 {
        self.crc
    }
}

pub trait Crc64Calculator: Copy + Clone + Send + Sync + 'static {
    fn calculate(&self, data: &[u8]) -> CrcCf;
    fn combine(&self, crc1: CrcCf, crc2: CrcCf) -> CrcCf;
}

#[cfg(feature = "zlib")]
pub mod types {
    use crc32fast::Hasher;

    use super::*;

    #[derive(Copy, Clone)]
    pub struct Crc32();

    impl Crc32Calculator for Crc32 {
        fn calculate(&self, data: &[u8]) -> CrcRs {
            let mut hasher = Hasher::new();
            hasher.update(data);

            CrcRs {
                crc: hasher.finalize(),
                len: data.len(),
            }
        }

        fn combine(&self, crc1: CrcRs, crc2: CrcRs) -> CrcRs {
            let mut crc_one = Hasher::from(crc1);
            let crc_two = Hasher::from(crc2);
            crc_one.combine(&crc_two);

            crc_one.into()
        }
    }

    impl From<CrcRs> for Hasher {
        fn from(other: CrcRs) -> Hasher {
            let mut hasher = Hasher::new();

            let mut ptr = &mut hasher as *mut _;
            let mut byte = ptr as *mut u8;
            let mut crc = unsafe { byte.add(12) } as *mut u32;
            let mut len = ptr as *mut u64;

            unsafe {
                *crc = other.crc;
                *len = other.len as u64;
            }

            hasher
        }
    }

    impl From<Hasher> for CrcRs {
        // thonk
        fn from(other: Hasher) -> CrcRs {
            let ptr = &other as *const _;
            let byte = ptr as *const u8;
            let crc = unsafe { byte.add(12) } as *const u32;
            let len = ptr as *const u64;

            CrcRs {
                crc: unsafe { *crc },
                len: unsafe { *len } as usize,
            }
        }
    }
}

#[cfg(any(feature = "cf-zlib", feature = "miniz", feature = "rustz"))]
pub mod types {
    use cloudflare_zlib::CfCrc;
    use crc32fast::Hasher;

    use super::*;

    #[derive(Copy, Clone)]
    pub struct Crc64();

    impl Crc64Calculator for Crc64 {
        fn calculate(&self, data: &[u8]) -> CrcCf {
            let cf = CfCrc::from(data);

            CrcCf {
                crc: cf.val,
                len: cf.len as usize,
            }
        }

        fn combine(&self, crc1: CrcCf, crc2: CrcCf) -> CrcCf {
            let mut crc_one = CfCrc::from(crc1);
            let crc_two = CfCrc::from(crc2);
            crc_one.combine(&crc_two);

            crc_one.into()
        }
    }

    impl From<CfCrc> for CrcCf {
        fn from(other: CfCrc) -> CrcCf {
            CrcCf {
                crc: other.val,
                len: other.len as usize,
            }
        }
    }

    impl From<CrcCf> for CfCrc {
        fn from(other: CrcCf) -> CfCrc {
            CfCrc {
                val: other.crc,
                len: other.len as isize,
            }
        }
    }

    #[derive(Copy, Clone)]
    pub struct Crc32();

    impl Crc32Calculator for Crc32 {
        fn calculate(&self, data: &[u8]) -> CrcRs {
            let mut hasher = Hasher::new();
            hasher.update(data);

            CrcRs {
                crc: hasher.finalize(),
                len: data.len(),
            }
        }

        fn combine(&self, crc1: CrcRs, crc2: CrcRs) -> CrcRs {
            let mut crc_one = Hasher::from(crc1);
            let crc_two = Hasher::from(crc2);
            crc_one.combine(&crc_two);

            crc_one.into()
        }
    }

    impl From<CrcRs> for Hasher {
        fn from(other: CrcRs) -> Hasher {
            let mut hasher = Hasher::new();

            let mut ptr = &mut hasher as *mut _;
            let mut byte = ptr as *mut u8;
            let mut crc = unsafe { byte.add(12) } as *mut u32;
            let mut len = ptr as *mut u64;

            unsafe {
                *crc = other.crc;
                *len = other.len as u64;
            }

            hasher
        }
    }

    impl From<Hasher> for CrcRs {
        // thonk
        fn from(other: Hasher) -> CrcRs {
            let ptr = &other as *const _;
            let byte = ptr as *const u8;
            let crc = unsafe { byte.add(12) } as *const u32;
            let len = ptr as *const u64;

            CrcRs {
                crc: unsafe { *crc },
                len: unsafe { *len } as usize,
            }
        }
    }
}
