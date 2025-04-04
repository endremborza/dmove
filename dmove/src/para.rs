use std::sync::{Arc, Condvar, Mutex};

use crossbeam_channel::{bounded, Receiver};

pub trait Worker<T>
where
    T: Send,
    Self: Sized + Sync,
{
    const CAPACITY_PER_THREAD: usize = 100;

    fn proc(&self, input: T);

    fn post(self) -> Self {
        self
    }

    fn para<I>(self, in_v: I) -> Self
    where
        I: Iterator<Item = T>,
    {
        let n_threads: usize = std::thread::available_parallelism().unwrap().into();
        self.para_n(in_v, n_threads)
    }
    fn para_n<I>(self, in_v: I, n: usize) -> Self
    where
        I: Iterator<Item = T>,
    {
        para_run::<Self, T, _>(in_v, &self, n);
        self.post()
    }
}

pub fn set_and_notify<T>(cvp: Arc<(Mutex<T>, Condvar)>, val: T) {
    let (lock, cvar) = &*cvp;
    let mut data = lock.lock().unwrap();
    *data = val;
    cvar.notify_all();
}

fn para_run<W, T, I>(in_v: I, setup: &W, n_threads: usize)
where
    W: Worker<T> + Sync,
    I: Iterator<Item = T>,
    T: Send,
{
    let capacity = n_threads * W::CAPACITY_PER_THREAD;
    let (sender, r) = bounded(capacity);

    std::thread::scope(|s| {
        let mut threads_v = Vec::new();
        for _ in 0..(n_threads) {
            let in_clone = r.clone();
            threads_v.push(s.spawn(move || subf::<W, _>(in_clone, setup)));
        }

        for e in in_v {
            sender.send(Some(e)).unwrap();
        }
        for _ in 0..(n_threads) {
            sender.send(None).unwrap();
        }
        for t in threads_v.into_iter() {
            t.join().expect("thread failed");
        }
    });
}

fn subf<W, T>(r: Receiver<Option<T>>, s: &W)
where
    W: Worker<T>,
    T: Send,
{
    loop {
        if let Some(qc_in) = r.recv().unwrap() {
            s.proc(qc_in);
        } else {
            break;
        };
    }
}
