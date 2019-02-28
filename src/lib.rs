#![feature(duration_float)]
#![feature(try_from)]
#![feature(stdsimd)]
#![feature(proc_macro_hygiene)]

pub mod crc;
pub mod crc_async;
pub mod crc_stream;
pub mod deflate;
pub mod deflate_stream;

#[cfg(test)]
#[cfg(any(feature = "rustz", feature = "miniz", feature = "cf-zlib", feature = "zlib"))]
mod tests {
    use flamer::flame;
    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};

    use crate::crc::{types::*, *};
    use crate::crc_stream::*;
    use crate::deflate_stream::{self, *};
    use crate::deflate::{self, *, press::{self, *}};

    #[test]
    #[flame]
    fn zwrap_test() {
        let mut blk = vec![0u8; 1024*1024*16];
        for i in (0u16..blk.capacity() as u16).step_by(2) {
            blk[i as usize..(i+2) as usize].copy_from_slice(&(((i + 16529).wrapping_mul(65521)) % 26).to_ne_bytes());
        }
        let mut block = Vec::with_capacity(blk.len() * 16);
        for _ in (0..16) {
            block.extend_from_slice(&blk);
        }

        let timer = std::time::Instant::now();

        let c = press::Compressor();

        println!("buf len: {}", block.len());

        let buf = c.compress(&block, None, 9, 0, 15).expect("error");

        println!("timer: {:#?} ({:.2} MiB/s)", timer.elapsed(), 256f64 / timer.elapsed().as_float_secs());

        println!("buf len: {}", buf.len());

        let mut d = press::Decompressor::new(15).expect("error");

        let decomp: Vec<u8>;
        if cfg!(feature = "cf-zlib") {
            decomp = d.decompress(&buf, None).expect("error");
        } else {
            decomp = d.decompress(&buf, Some(block.len())).expect("error");
        }

        assert_eq!(block, decomp);

        println!("buf len: {}", decomp.len());
        flame::dump_stdout();
        flame::dump_html(std::fs::File::create("zwrap_test.html").expect("error writing flamegraph"));
    }

    #[test]
    #[flame]
    fn stream_compression_test() {
        let mut block = vec![0u8; 1024 * 1024 * 16];
        for i in (0u16..block.capacity() as u16).step_by(2) {
            block[i as usize..(i+2) as usize].copy_from_slice(&(((i + 16529).wrapping_mul(65521)) % 26).to_ne_bytes());
        }
        let l1 = block.len();
        println!("{}", l1);
        let (mut writer, mut reader) = deflate_stream::<Compressor>(9, 0, 15, 4);

        let timer = std::time::Instant::now();
        for chunk in block.chunks(block.len() / 4) {
            writer.write(chunk);
            println!("chunk.len() -> {}", chunk.len());
        }
        writer.flush();
        drop(writer);

        println!("stream len: {}", reader.total_len());

        let mut compressed = vec![0u8; block.len()];
        match reader.read(&mut compressed) {
            Ok(amount) => {
                unsafe { compressed.set_len(amount) };
                println!("timer: {:#?} ({:.2} MiB/s)", timer.elapsed(), 16f64 / timer.elapsed().as_float_secs());
                println!("buf len: {} (compare to: {})", compressed.len(), l1);
            },
            Err(e) => {
                panic!("{:?}", e);
            },
        }

        let mut d = press::Decompressor::new(15).expect("error");

        let decomp = if cfg!(feature = "cf-zlib") {
            d.decompress(&compressed, None).expect("error")
        } else {
            d.decompress(&compressed, Some(block.len())).expect("error")
        };

        //println!("block: {:?}\n\ndecomp: {:?}", block, decomp);

        println!("buf len: {}", decomp.len());
        assert_eq!(decomp.len(), l1);
        println!("buf len: {}", decomp.len());
        assert_eq!(decomp, block);

        flame::dump_stdout();
    }

    #[test]
    fn crc32_to_from() {
        let mut block = vec![0u8; 1024*1024*16];
        for i in (0u16..block.capacity() as u16).step_by(2) {
            block[i as usize..(i+2) as usize].copy_from_slice(&(((i + 16529).wrapping_mul(65521)) % 26).to_ne_bytes());
        }

        let timer = std::time::Instant::now();

        // blocks will be copied into the stream, so we'll simulate that here
        let block1 = block[..block.len()/2].to_vec();
        let block2 = block[block.len()/2..].to_vec();

        let calc = Crc32();
        let crc1 = calc.calculate(&block1);
        let crc2 = calc.calculate(&block2);
        let combined = calc.combine(crc1, crc2);

        println!("timer: {:#?} ({:.2} MiB/s)", timer.elapsed(), 16f64 / timer.elapsed().as_float_secs());

        let mut crc_stock = crc32fast::Hasher::new();
        crc_stock.update(&block);

        assert_eq!(combined.inner(), crc_stock.finalize());
    }

    #[cfg(any(feature = "cf-zlib", feature = "miniz", feature = "rustz"))]
    #[test]
    fn crc64_to_from() {
        let mut block = vec![0u8; 1024*1024*16];
        for i in (0..block.capacity()).step_by(8) {
            block[i..i+8].copy_from_slice(&(i % 64).to_ne_bytes());
        }

        let timer = std::time::Instant::now();

        // blocks will be copied into the stream, so we'll simulate that here
        let block1 = block[..block.len()/2].to_vec();
        let block2 = block[block.len()/2..].to_vec();

        let calc = Crc64();
        let crc1 = calc.calculate(&block1);
        let crc2 = calc.calculate(&block2);
        let combined = calc.combine(crc1, crc2);

        println!("timer: {:#?} ({:.2} MiB/s)", timer.elapsed(), 16f64 / timer.elapsed().as_float_secs());

        let mut crc_stock = cloudflare_zlib::CfCrc::from(&block);

        assert_eq!(combined.inner(), crc_stock.inner());
    }

    #[test]
    fn crc_stream_test() {
        let mut block = vec![0u8; 1024*1024*16];
        for i in (0..block.capacity()).step_by(8) {
            block[i..i+8].copy_from_slice(&(i % 64).to_ne_bytes());
        }

        let timer = std::time::Instant::now();

        let (mut writer, mut reader) = crc_stream::<Crc32>(4);

        reader.crc.calculate_async(std::time::Duration::from_millis(10));
        for chunk in block.chunks(block.len()/4) {
            writer.write(chunk);
        }
        drop(writer);

        let crc1 = reader.crc.wait();

        println!("timer: {:#?} ({:.2} MiB/s)", timer.elapsed(), 16f64 / timer.elapsed().as_float_secs());

        let mut crc_stock = crc32fast::Hasher::new();
        crc_stock.update(&block);

        assert_eq!(crc1, crc_stock.finalize());
    }

    #[test]
    #[flame]
    fn crc_comp_decomp() {
        let mut block = vec![0u8; 1024*1024*16];
        for i in (0u16..block.capacity() as u16).step_by(2) {
            block[i as usize..(i+2) as usize].copy_from_slice(&(((i + 16529).wrapping_mul(65521)) % 26).to_ne_bytes());
        }

        let block_len = block.len();
        let timer = std::time::Instant::now();

        let (mut crc_writer, mut crc_reader) = crc_stream::<Crc32>(1);
        let (mut data_writer, mut data_reader) = deflate_stream::<Compressor>(9, 0, 15, 4);

        crc_reader.crc.calculate_async(std::time::Duration::from_millis(500));

        std::thread::spawn(move || {
            flame::start_guard("writer thread");
            for i in 0..16 {
                crc_writer.write(&block);
            }
            crc_writer.flush();
            drop(crc_writer);
            flame::dump_stdout();
            flame::dump_html(std::fs::File::create("writer_thread.html").unwrap());
        });

        while let Ok(msg) = crc_reader.current() {
            data_writer.push(msg);
        }
        data_writer.flush();

        drop(data_writer);
        println!("timer: {:#?} ({:.2} MiB/s)", timer.elapsed(), 256f64 / timer.elapsed().as_float_secs());

        let mut compressed_data = Vec::with_capacity(block_len);
        while let Ok(buf) = data_reader.current() {
            compressed_data.extend_from_slice(&buf);
        }

        let crc1 = crc_reader.crc.wait();

        drop(data_reader);
        drop(crc_reader);

        let mut d = press::Decompressor::new(15).expect("error");

        let decomp = if cfg!(feature = "cf-zlib") {
            d.decompress(&compressed_data, None).expect("error")
        } else {
            d.decompress(&compressed_data, Some(block_len * 16)).expect("error")
        };

        let calc = Crc32();
        let crc2 = calc.calculate(&decomp);

        println!("compressed_data.len() -> {}", compressed_data.len());
        //assert_eq!(block.len(), decomp.len());
        //assert_eq!(block, decomp);
        assert_eq!(crc1, crc2.inner());
        flame::dump_stdout();
        flame::dump_html(std::fs::File::create("crc_comp_decomp_test.html").unwrap());
    }

    #[flame]
    fn io_comp_test() -> u32 {
        let (mut crc_writer, mut crc_reader) = crc_stream::<Crc32>(1);
        let (mut data_writer, mut data_reader) = deflate_stream::<Compressor>(9, 0, 15, 10);

        let crc1 = Arc::new(Mutex::new(0u32));
        let crc2 = Arc::new(Mutex::new(0u32));

        let arc_crc1 = Arc::clone(&crc1);

        std::thread::spawn(move || {
            let mut input = std::fs::OpenOptions::new().read(true).open("./mudd.avi").expect("error");
            let mut buf = vec![0u8; 1024*1024*16];
            let mut amt = 0;
            let len = input.metadata().expect("err").len() as usize;

            while let Ok(sz) = input.read(&mut buf) {
                unsafe { buf.set_len(sz) };
                crc_writer.write(&buf);
                amt += sz;

                if amt >= len {
                    break;
                }
            }

            crc_writer.flush();
            drop(crc_writer);
        });

        std::thread::spawn(move || {
            while crc_reader.len() == 0 {}
            let mut amt = 0;

            while let Ok(msg) = crc_reader.current() {
                amt += msg.len();
                data_writer.push(msg);
            }

            crc_reader.crc.calculate(std::time::Duration::from_secs(1));

            let mut l = arc_crc1.lock().expect("error");

            *l = crc_reader.crc.wait();

            data_writer.flush();
        });

        let timer = std::time::Instant::now();

        let mut amt = 0;

        let mut output = std::fs::OpenOptions::new().write(true).create(true).open("./mudd.pressed").expect("error");

        while data_reader.len() == 0 {}

        while let Ok(buf) = data_reader.current() {
            amt += buf.len();
            output.write(&buf).expect("write error");
        }
        println!("******took {:#?} to write {} bytes******", timer.elapsed(), amt);

        while *crc1.lock().expect("err") == 0 {}
        let l = crc1.lock().expect("");
        *l
    }

    #[flame]
    fn io_decomp_test(other: u32) -> bool {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (mut crc_writer, mut crc_reader) = crc_stream::<Crc32>(1);

        let result = Arc::new(AtomicUsize::new(0));

        let res = result.clone();

        std::thread::spawn(move || {
            let mut output = std::fs::OpenOptions::new().write(true).create(true).open("./mudd.result").expect("error");

            crc_reader.crc.calculate_async(std::time::Duration::from_secs(1));

            while let Ok(buf) = crc_reader.current() {
                output.write(&buf);
            }
            let crc = crc_reader.crc.wait();
            (*res).store(crc as usize, Ordering::Relaxed);
        });

        let mut input = std::fs::OpenOptions::new().read(true).open("./mudd.pressed").expect("error");
        let mut decomp = Decompressor::new(15).expect("decomp err");
        loop {
            let mut buf = vec![0u8; 1024 * 1024 * 16];
            match input.read(&mut buf) {
                Ok(sz) => {
                    if sz == 0 { break; }
                    unsafe { buf.set_len(sz) };
                },
                Err(_) => break,
            }
            crc_writer.push(decomp.decompress(&buf, None).expect("decomp err"));
        }
        crc_writer.flush();
        drop(crc_writer);

        while (*result).load(Ordering::Relaxed) == 0 {}
        (*result).load(Ordering::Relaxed) == other as usize
    }

    #[test]
    #[flame]
    fn io_test() {
        let crc1 = io_comp_test();
        assert_eq!(true, io_decomp_test(crc1));
        flame::dump_stdout();
        flame::dump_html(std::fs::File::create("io_test.html").expect(""));
    }
}
