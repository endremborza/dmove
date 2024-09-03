use std::sync::Arc;

use crossbeam_channel::{bounded, Receiver};

#[macro_export]
macro_rules! clone_thread_push {
    ($thread_vec: ident, $para_fun: ident, $($arg: ident),*) => {
        {
        $(let $arg = Arc::clone(&$arg);)*

        $thread_vec.push(thread::spawn(move || {
            $para_fun(
                $($arg,)*
        )
        }))
        }
    };
}

pub trait Worker<T>
where
    T: Send,
    Self: Sized,
    Arc<Self>: Send,
{
    fn proc(&self, input: T);

    fn para<I>(self, in_v: I) -> Arc<Self>
    where
        I: Iterator<Item = T>,
    {
        let n_threads: usize = std::thread::available_parallelism().unwrap().into();
        self.para_n(in_v, n_threads)
    }
    fn para_n<I>(self, in_v: I, n: usize) -> Arc<Self>
    where
        I: Iterator<Item = T>,
    {
        let arced_self = Arc::new(self);
        para_run::<Self, T, _>(in_v, arced_self.clone(), n);
        arced_self
    }
}

pub fn para_run<W, T, I>(in_v: I, setup: Arc<W>, n_threads: usize)
where
    W: Worker<T>,
    I: Iterator<Item = T>,
    T: Send,
    Arc<W>: Send,
{
    let capacity = n_threads * 100;

    let (sender, r) = bounded(capacity);

    std::thread::scope(|s| {
        for _ in 0..(n_threads) {
            let in_clone = r.clone();
            let s_clone = setup.clone();
            s.spawn(move || subf::<W, _>(in_clone, s_clone));
        }

        for e in in_v {
            sender.send(Some(e)).unwrap();
        }
        for _ in 0..(n_threads) {
            sender.send(None).unwrap();
        }
    });
}

fn subf<W, T>(r: Receiver<Option<T>>, s: Arc<W>)
where
    W: Worker<T>,
    T: Send,
    Arc<W>: Send,
{
    loop {
        if let Some(qc_in) = r.recv().unwrap() {
            s.proc(qc_in);
        } else {
            break;
        };
    }
}
