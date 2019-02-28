use crossbeam_channel::RecvTimeoutError;
use crossbeam_channel::RecvError;
use crossbeam_channel::{Sender, unbounded};
use flamer::flame;
use ordered_stream::OrderedStream;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::io::{self, ErrorKind, Read, Write};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}, Mutex, RwLock};

use crate::crc::*;
use crate::crc_async::AsyncCrc;

pub fn crc_stream<C: Crc32Calculator>(threads: usize) -> (CrcStreamW<C>, CrcStreamR<C>) {
    let (data_tx, data_rx) = unbounded::<(u64, Vec<u8>)>();
    let (tag_tx, tag_rx) = unbounded::<(u64, Vec<CrcRs>)>();

    let data_stream = OrderedStream::with_recvr(data_rx);
    let tag_stream = OrderedStream::with_recvr(tag_rx);

    let w_cnt = Arc::new(AtomicUsize::new(1));
    let len = Arc::new(AtomicUsize::new(0));
    let pending = Arc::new(AtomicUsize::new(0));
    let buffer = Arc::new(AtomicUsize::new(0));
    let calc: C = unsafe { std::mem::uninitialized() };
    let crc = AsyncCrc::new(tag_stream);

    let reader = CrcStreamR {
        output: Arc::new(RwLock::new(data_stream)),
        crc,
        w_cnt: Arc::clone(&w_cnt),
        len: Arc::clone(&len),
        pending: Arc::clone(&pending),
        buffer,
        calc,
    };

    let pool = ThreadPoolBuilder::new().num_threads(threads).breadth_first().build().unwrap();

    let writer = CrcStreamW {
        idx: 0,
        data_tx,
        tag_tx,
        pool: Arc::new(pool),
        errors: Arc::new(Mutex::new(Vec::new())),
        w_cnt,
        len,
        pending,
        calc,
    };

    (writer, reader)
}

pub struct CrcStreamR<C: Crc32Calculator> {
    output: Arc<RwLock<OrderedStream<u8>>>,
    pub crc: AsyncCrc<C>,
    w_cnt: Arc<AtomicUsize>,
    pending: Arc<AtomicUsize>,
    len: Arc<AtomicUsize>,
    buffer: Arc<AtomicUsize>,
    calc: C,
}

impl<C: Crc32Calculator> CrcStreamR<C> {
    pub fn squeeze(&self, len: usize) -> Result<Vec<u8>, String> {
        self.update_size();

        if self.total_len() == 0 {
            if !self.has_writers() {
                Err(io::Error::new(ErrorKind::UnexpectedEof, "no available data").to_string())
            } else {
                Err(io::Error::new(ErrorKind::Interrupted, "possible unsent data").to_string())
            }
        } else if self.total_len() < len && self.total_len() > 0 {
            let mut l = self.output.write().expect("error locking output stream");
            match l.squeeze(len) {
                Ok(buf) => {
                    self.len.fetch_sub(buf.len(), Ordering::Relaxed);
                    Ok(buf)
                },
                Err(e) => {
                    match e {
                        Some(err) => Err(err.to_string()),
                        None => Err(io::Error::new(ErrorKind::UnexpectedEof, "stream empty").to_string()),
                    }
                }
            }
        } else if self.total_len() >= len {
            if self.len() >= len {
                let mut l = self.output.write().expect("error locking output stream");
                match l.squeeze(len) {
                    Ok(buf) => {
                        self.len.fetch_sub(buf.len(), Ordering::Relaxed);
                        Ok(buf)
                    },
                    Err(e) => {
                        match e {
                            Some(err) => Err(err.to_string()),
                            None => Err(io::Error::new(ErrorKind::UnexpectedEof, "stream empty").to_string()),
                        }
                    }
                }
            } else {
                loop {
                    if self.len() >= len {
                        let mut l = self.output.write().expect("error locking output stream");
                        match l.squeeze(len) {
                            Ok(buf) => {
                                self.len.fetch_sub(buf.len(), Ordering::Relaxed);
                                return Ok(buf)
                            },
                            Err(e) => {
                                match e {
                                    Some(err) => return Err(err.to_string()),
                                    None => return Err(io::Error::new(ErrorKind::UnexpectedEof, "stream empty").to_string()),
                                }
                            }
                        }
                    } else {
                        // is_empty() will compare against total_len()
                        if self.len() > 0 {
                            let mut l = self.output.write().expect("error locking output stream");
                            l.read_msgs(self.len());
                            drop(l);
                            continue
                        } else {
                            continue
                        }
                    }
                }
            }
        } else {
            Err(io::Error::new(ErrorKind::Other, "unknown error").to_string())
        }
    }

    pub fn writer_count(&self) -> usize {
        self.w_cnt.load(Ordering::Relaxed)
    }

    pub fn has_writers(&self) -> bool {
        self.writer_count() != 0
    }

    pub fn buffered_len(&self) -> usize {
        self.buffer.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed) + self.buffered_len()
    }

    pub fn is_empty(&self) -> bool {
        self.update_size();
        self.total_len() == 0
    }

    pub fn pending_len(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    pub fn total_len(&self) -> usize {
        self.pending_len() + self.len()
    }

    pub fn update_size(&self) {
        let l = self.output.read().expect("error locking output stream");
        self.buffer.store(l.size(), Ordering::Relaxed);
    }

    #[flame]
    pub fn current(&mut self) -> Result<Vec<u8>, Option<RecvError>> {
        let mut l = self.output.write().expect("error locking output stream");
        l.current()
    }

    #[flame]
    pub fn current_timeout(&mut self, timeout: std::time::Duration) -> Result<Vec<u8>, Option<RecvTimeoutError>> {
        let mut l = self.output.write().expect("error locking output stream");
        l.current_timeout(timeout)
    }
}

impl<C: Crc32Calculator> Read for CrcStreamR<C> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.update_size();

        if self.total_len() == 0 {
            if !self.has_writers() {
                Err(io::Error::new(ErrorKind::UnexpectedEof, "no available data"))
            } else {
                Err(io::Error::new(ErrorKind::Interrupted, "possible unsent data"))
            }
        } else if self.total_len() < buf.len() && self.total_len() > 0 {
            let mut l = self.output.write().expect("error locking output stream");
            match l.read(buf) {
                Ok(written) => {
                    self.len.fetch_sub(written, Ordering::Relaxed);
                    Ok(written)
                },
                Err(e) => Err(e),
            }
        } else if self.total_len() >= buf.len() {
            if self.len() >= buf.len() {
                let mut l = self.output.write().expect("error locking output stream");
                match l.read(buf) {
                    Ok(written) => {
                        self.len.fetch_sub(written, Ordering::Relaxed);
                        Ok(written)
                    },
                    Err(e) => Err(e),
                }
            } else {
                loop {
                    if self.len() >= buf.len() {
                        let mut l = self.output.write().expect("error locking output stream");
                        match l.read(buf) {
                            Ok(written) => {
                                self.len.fetch_sub(written, Ordering::Relaxed);
                                return Ok(written)
                            },
                            Err(e) => {
                                return Err(e)
                            },
                        }
                    } else {
                        // is_empty() will compare against total_len()
                        if self.len() > 0 {
                            let mut l = self.output.write().expect("error locking output stream");
                            l.read_msgs(self.len());
                            drop(l);
                            continue
                        } else {
                            continue
                        }
                    }
                }
            }
        } else {
            Err(io::Error::new(ErrorKind::Other, "unknown error"))
        }
    }
}

#[derive(Clone)]
pub struct CrcStreamW<C: Crc32Calculator> {
    idx: u64,
    pool: Arc<ThreadPool>,
    errors: Arc<Mutex<Vec<String>>>,
    w_cnt: Arc<AtomicUsize>,
    len: Arc<AtomicUsize>,
    pending: Arc<AtomicUsize>,
    data_tx: Sender<(u64, Vec<u8>)>,
    tag_tx: Sender<(u64, Vec<CrcRs>)>,
    calc: C,
}

impl<C: Crc32Calculator> Drop for CrcStreamW<C> {
    fn drop(&mut self) {
        self.w_cnt.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<C: Crc32Calculator> CrcStreamW<C> {
    #[flame]
    pub fn insert(&self, idx: u64, msg: Vec<u8>) {
        self.pending.fetch_add(msg.len(), Ordering::Relaxed);

        let pnd = Arc::clone(&self.pending);
        let c_tx = self.data_tx.clone();
        let t_tx = self.tag_tx.clone();
        let err = Arc::clone(&self.errors);
        let len = Arc::clone(&self.len);
        let calc = self.calc;

        self.pool.spawn(move || {
            let tag = calc.calculate(&msg);
            let l = msg.len();

            match t_tx.send((idx, vec![tag])) {
                Ok(_) => {
                    //println!("sent tag {}", idx);
                },
                Err(e) => {
                    let mut l = err.lock().unwrap();
                    l.push("tag tx err: ".to_owned() + &e.to_string());
                },
            }

            match c_tx.send((idx, msg)) {
                Ok(_) => {
                    pnd.fetch_sub(l, Ordering::Relaxed);
                    len.fetch_add(l, Ordering::Relaxed);
                    //flame::dump_stdout();
                    //flame::dump_html(std::fs::File::create(format!("crc_thread_{}.html", idx)).unwrap());
                },
                Err(e) => {
                    let mut l = err.lock().unwrap();
                    l.push("data tx err: ".to_owned() + &e.to_string());
                },
            }
        });
    }

    #[flame]
    pub fn push(&mut self, msg: Vec<u8>) {
        let idx = self.idx;
        self.idx += 1;
        self.insert(idx, msg);
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    pub fn total_len(&self) -> usize {
        self.len() + self.pending()
    }

    pub fn is_empty(&self) -> bool {
        self.total_len() == 0
    }
}

impl<C: Crc32Calculator> Write for CrcStreamW<C> {
    #[flame]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let msg = buf.to_vec();

        self.push(msg);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        while self.pending() > 0 {}
        Ok(())
    }
}
