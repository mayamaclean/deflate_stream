use crossbeam_channel::RecvTimeoutError;
use crossbeam_channel::RecvError;
use crossbeam_channel::{Sender, unbounded};
use flamer::flame;
use intmap::IntMap;
use ordered_stream::OrderedStream;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::io::{self, ErrorKind, Read, Write};
use std::os::raw::*;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}, Mutex, RwLock};

use crate::deflate::*;

pub fn deflate_stream<D: Deflater>(level: isize, strategy: isize, window_bits: isize, threads: usize) -> (DeflateStreamW<D>, DeflateStreamR<D>) {
    let (data_tx, data_rx) = unbounded::<(u64, Vec<u8>)>();

    let data_stream = OrderedStream::with_recvr(data_rx);
    let deflate_object: D = unsafe { std::mem::uninitialized() };
    let w_cnt = Arc::new(AtomicUsize::new(1));
    let pending = Arc::new(AtomicUsize::new(0));
    let len = Arc::new(AtomicUsize::new(0));
    let pool = Arc::new(ThreadPoolBuilder::new().num_threads(threads).breadth_first().build().unwrap());

    let reader = DeflateStreamR {
        output: Arc::new(RwLock::new(data_stream)),
        w_cnt: Arc::clone(&w_cnt),
        pending: Arc::clone(&pending),
        len: Arc::clone(&len),
        buffer: 0,
        def: deflate_object.clone(),
    };

    let writer = DeflateStreamW {
        idx: 0,
        data_tx,
        pool,
        errors: Arc::new(Mutex::new(Vec::new())),
        w_cnt,
        pending,
        len,
        def: deflate_object,
        dicts: Arc::new(Mutex::new(IntMap::new())),
        level: level as c_int,
        window_bits: window_bits as c_int,
        strategy: strategy as c_int,
    };

    (writer, reader)
}

pub struct DeflateStreamR<D: Deflater> {
    output: Arc<RwLock<OrderedStream<u8>>>,
    w_cnt: Arc<AtomicUsize>,
    pending: Arc<AtomicUsize>,
    len: Arc<AtomicUsize>,
    buffer: usize,
    def: D,
}

impl<D: Deflater> DeflateStreamR<D> {
    pub fn squeeze(&mut self, len: usize) -> Result<Vec<u8>, String> {
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
                    self.len.fetch_sub(buf.len(), Ordering::Release);
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
                        self.len.fetch_sub(buf.len(), Ordering::Release);
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
                                self.len.fetch_sub(buf.len(), Ordering::Release);
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
        self.w_cnt.load(Ordering::Acquire)
    }

    pub fn has_writers(&self) -> bool {
        self.writer_count() != 0
    }

    pub fn buffered_len(&self) -> usize {
        self.buffer
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire) + self.buffered_len()
    }

    pub fn is_empty(&mut self) -> bool {
        self.update_size();
        self.total_len() == 0
    }

    pub fn pending_len(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    pub fn total_len(&mut self) -> usize {
        self.pending_len() + self.len()
    }

    pub fn update_size(&mut self) {
        let l = self.output.read().expect("error locking output stream");
        self.buffer = l.size();
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


impl<D: Deflater> Read for DeflateStreamR<D> {
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
                    self.len.fetch_sub(written, Ordering::Release);
                    Ok(written)
                },
                Err(e) => Err(e),
            }
        } else if self.total_len() >= buf.len() {
            if self.len() >= buf.len() {
                let mut l = self.output.write().expect("error locking output stream");
                match l.read(buf) {
                    Ok(written) => {
                        self.len.fetch_sub(written, Ordering::Release);
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
                                self.len.fetch_sub(written, Ordering::Release);
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

pub struct DeflateStreamW<D: Deflater> {
    idx: u64,
    data_tx: Sender<(u64, Vec<u8>)>,
    pool: Arc<ThreadPool>,
    errors: Arc<Mutex<Vec<String>>>,
    w_cnt: Arc<AtomicUsize>,
    pending: Arc<AtomicUsize>,
    len: Arc<AtomicUsize>,
    def: D,
    dicts: Arc<Mutex<IntMap<Vec<u8>>>>,
    level: c_int,
    window_bits: c_int,
    strategy: c_int,
}

impl<D: Deflater> DeflateStreamW<D> {
    #[flame]
    pub fn insert(&mut self, idx: u64, msg: Vec<u8>) {
        self.pending.fetch_add(msg.len(), Ordering::Acquire);

        let pnd = Arc::clone(&self.pending);
        let data_tx = self.data_tx.clone();
        let err = Arc::clone(&self.errors);
        let len = Arc::clone(&self.len);
        let def = self.def.clone();
        let dicts = Arc::clone(&self.dicts);
        let level = self.level;
        let window_bits = self.window_bits;
        let strategy = self.strategy;

        self.pool.spawn(move || {
            let dict_to_add = if msg.len() < WINDOW_SIZE {
                msg.to_vec()
            } else {
                msg[msg.len() - WINDOW_SIZE..].to_vec()
            };

            let mut dictionary = dicts.lock().expect("error locking dictionary map for write");
            dictionary.insert(idx + 1, dict_to_add);
            drop(dictionary);

            let mut dict = None;
            if idx > 0 {
                loop {
                    let mut dictionary = dicts.lock().expect("error locking dictionary map for read");

                    if let Some(d) = dictionary.remove(idx) {
                        dict = Some(d);
                        break;
                    }
                }
            }

            let compressed_data = def.compress(&msg, dict, level, strategy, window_bits).expect("error compressing data");

            let l = compressed_data.len();

            match data_tx.send((idx, compressed_data)) {
                Ok(_) => {
                    pnd.fetch_sub(msg.len(), Ordering::Release);
                    len.fetch_add(l, Ordering::Acquire);
                    //flame::dump_stdout();
                    //flame::dump_html(std::fs::File::create(format!("compress_thread_{}.html", idx)).unwrap());
                },
                Err(e) => {
                    let mut l = err.lock().unwrap();
                    l.push("data tx err: ".to_owned() + &e.to_string());
                },
            }
        });
    }

    #[flame]
    pub fn push(&mut self, mut msg: Vec<u8>) {
        let idx = self.idx;
        self.idx += 1;
        self.insert(idx, msg);
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    pub fn total_len(&self) -> usize {
        self.len() + self.pending()
    }

    pub fn is_empty(&self) -> bool {
        self.total_len() == 0
    }
}

impl<D: Deflater> Drop for DeflateStreamW<D> {
    fn drop(&mut self) {
        self.w_cnt.fetch_sub(1, Ordering::Relaxed);
    }
}


impl<D: Deflater> Write for DeflateStreamW<D> {
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
