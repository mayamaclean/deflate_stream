use flamer::flame;
use ordered_stream::OrderedStream;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering}, Mutex};
use std::thread;
use std::time::Duration;

use crate::crc::{*, Crc32Calculator, Crc64Calculator};

pub struct AsyncCrc<C: Crc32Calculator> {
    incoming: Arc<Mutex<OrderedStream<CrcRs>>>,
    ready: Arc<AtomicBool>,
    crc: Arc<AtomicU32>,
    calc: C,
}

impl<C: Crc32Calculator> AsyncCrc<C> {
    pub fn new(i: OrderedStream<CrcRs>) -> AsyncCrc<C> {
        let calc: C = unsafe { std::mem::uninitialized() };

        AsyncCrc {
            incoming: Arc::new(Mutex::new(i)),
            ready: Arc::new(AtomicBool::new(false)),
            crc: Arc::new(AtomicU32::new(0)),
            calc,
        }
    }

    pub fn calculate_async(&self, d: Duration) {
        let incoming = Arc::clone(&self.incoming);
        let red = Arc::clone(&self.ready);
        let crc = Arc::clone(&self.crc);
        let calc = self.calc;

        thread::spawn(move || {
            flame::start_guard("async crc combination thread");
            let mut lock = incoming.lock().expect("could not acquire stream lock");
            let mut init: CrcRs = lock.current_timeout(d).expect("error getting first tag").pop().expect("error popping initial tag");

            for mut tag in lock.iter_mut_timeout(d) {
                init = calc.combine(init, tag.pop().expect("error popping tag"));
            }

            crc.store(init.inner(), Ordering::Relaxed);
            red.store(true, Ordering::Relaxed);
            flame::dump_stdout();
            flame::dump_html(std::fs::File::create("async_crc_thread.html").unwrap());
        });
    }

    pub fn calculate(&self, d: Duration) {
        let mut lock = self.incoming.lock().expect("could not acquire stream lock");
        let mut init: CrcRs = lock.current_timeout(d).expect("error getting first tag").pop().expect("error popping initial tag");

        for mut tag in lock.iter_mut_timeout(d) {
            init = self.calc.combine(init, tag.pop().expect("error popping tag"));
        }

        self.crc.store(init.inner(), Ordering::Relaxed);
        self.ready.store(true, Ordering::Relaxed);
    }

    pub fn is_ready(&self) -> Result<u32, ()> {
        if self.ready.load(Ordering::Relaxed) {
            Ok(self.crc.load(Ordering::Relaxed))
        } else {
            Err(())
        }
    }

    pub fn collect_crcs(&self, timeout: Duration) -> Vec<CrcRs> {
        let mut lock = self.incoming.lock().expect("could not acquire stream lock");
        let mut crcs = Vec::new();
        for mut crc in lock.iter_mut_timeout(timeout) {
            crcs.push(crc.pop().expect("error popping crc"));
        }
        crcs
    }

    #[flame]
    pub fn wait(&self) -> u32 {
        loop {
            if let Ok(tag) = self.is_ready() {
                return tag
            }
        }
    }
}
