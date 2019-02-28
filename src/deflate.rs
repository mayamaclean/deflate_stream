use std::os::raw::*;

pub const WINDOW_SIZE: usize = 1024*32;

pub trait Deflater: Clone + Send + Sync + 'static {
    fn compress(&self, data: &[u8], dict: Option<Vec<u8>>, level: c_int, strategy: c_int, window_bits: c_int) -> Result<Vec<u8>, String>;
}

pub trait Inflater {
    /* expected_len is a workaround for the flate2 backend and not needed when calling the cf-zlib backend though it can be used
    when used the flate2 backend, it determines the total number of bytes returned
    it will default to data.len() * 5 where expected_len is unspecified, which should cover the majority of cases */
    fn decompress(&mut self, data: &[u8], expected_len: Option<usize>) -> Result<Vec<u8>, String>;
}

#[cfg(feature = "cf-zlib")]
pub mod press {
    use cloudflare_zlib::{CfCrc, Deflate, Inflate};
    use super::*;

    #[derive(Copy, Clone)]
    pub struct Compressor();

    pub struct Decompressor(Inflate);

    impl Deflater for Compressor {
        fn compress(&self, data: &[u8], dict: Option<Vec<u8>>, level: c_int, strategy: c_int, window_bits: c_int) -> Result<Vec<u8>, String> {
            let mut wb = window_bits;

            if wb.is_positive() {
                wb *= -1;
            }

            let mut buf = Vec::with_capacity(0);
            let mut comp = Deflate::new_with_vec(level, strategy, wb, buf).expect("error initiating cf-zlib");

            if let Some(d) = dict {
                comp.set_dict(&d);
            }

            buf = Vec::with_capacity(data.len() * 2);
            match comp.compress_sync(data, &mut buf) {
                Ok(()) => Ok(buf),
                Err(e) => Err(e.to_string()),
            }
        }
    }

    impl Inflater for Decompressor {
        fn decompress(&mut self, data: &[u8], expected_len: Option<usize>) -> Result<Vec<u8>, String> {
            let size = match expected_len {
                Some(i) => i,
                None => data.len() * 5,
            };

            let mut buf = Vec::with_capacity(size);
            match self.0.inflate_sync(&data, &mut buf) {
                Ok(_) => Ok(buf),
                Err(e) => Err(e.to_string()),
            }
        }
    }

    impl Decompressor {
        pub fn new(window_bits: c_int) -> Result<Self, String> {
            let mut wb = window_bits;

            if wb.is_positive() {
                wb *= -1;
            }

            match Inflate::new_with_window(wb) {
                Ok(inner) => Ok(Decompressor(inner)),
                Err(e) => Err(e.to_string()),
            }
        }
    }
}

#[cfg(any(feature = "rustz", feature = "miniz", feature = "zlib"))]
pub mod press {
    use flate2::{Compress, Decompress, Compression, FlushCompress, FlushDecompress};

    use super::*;

    #[derive(Copy, Clone)]
    pub struct Compressor();

    pub struct Decompressor(pub Decompress);

    impl Deflater for Compressor {
        fn compress(&self, data: &[u8], dict: Option<Vec<u8>>, level: c_int, strategy: c_int, window_bits: c_int) -> Result<Vec<u8>, String> {
            let mut wb: u32;

            if window_bits.is_positive() {
                wb = window_bits as u32;
            } else {
                wb = window_bits.checked_neg().unwrap() as u32;
            }

            let mut lvl: u32;

            if level.is_positive() {
                lvl = level as u32;
            } else {
                lvl = level.checked_neg().unwrap() as u32;
            }

            let mut comp = Compress::new(Compression::new(lvl), false);

            if let Some(d) = dict {
                let mut temp = Vec::with_capacity(WINDOW_SIZE);
                comp.compress(&d, &mut temp, FlushCompress::Sync);
            }

            let mut buf = Vec::with_capacity(data.len() * 2);
            match comp.compress_vec(&data, &mut buf, FlushCompress::Sync) {
                Ok(s) => {
                    Ok(buf)
                },
                Err(e) => Err(e.to_string()),
            }
        }
    }

    impl Inflater for Decompressor {
        // the flate2 miniz/rustz backend will only return buf.len() bytes in sync mode
        fn decompress(&mut self, data: &[u8], expected_len: Option<usize>) -> Result<Vec<u8>, String> {
            let size = match expected_len {
                Some(i) => i,
                None => data.len() * 5,
            };

            let mut buf = Vec::with_capacity(size);

            match self.0.decompress_vec(data, &mut buf, FlushDecompress::Sync) {
                Ok(s) => Ok(buf),
                Err(e) => Err(e.to_string()),
            }
        }
    }

    impl Decompressor {
        pub fn new(window_bits: c_int) -> Result<Self, String> {
            Ok(Self(Decompress::new(false)))
        }
    }
}
