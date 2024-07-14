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
        let arced_self = Arc::new(self);
        para_run::<Self, T, _>(in_v, arced_self.clone());
        arced_self
    }
}

pub enum QueIn<T> {
    Go(T),
    Poison,
}

pub fn para_run<W, T, I>(in_v: I, setup: Arc<W>)
where
    W: Worker<T>,
    I: Iterator<Item = T>,
    T: Send,
    Arc<W>: Send,
{
    let n_threads: usize = std::thread::available_parallelism().unwrap().into();
    let capacity = n_threads * 100;

    let (sender, r) = bounded(capacity);

    std::thread::scope(|s| {
        for _ in 0..(n_threads) {
            let in_clone = r.clone();
            let s_clone = setup.clone();
            s.spawn(move || subf::<W, _>(in_clone, s_clone));
        }

        for e in in_v {
            sender.send(QueIn::Go(e)).unwrap();
        }
        for _ in 0..(n_threads) {
            sender.send(QueIn::Poison).unwrap();
        }
    });
}

fn subf<W, T>(r: Receiver<QueIn<T>>, s: Arc<W>)
where
    W: Worker<T>,
    T: Send,
    Arc<W>: Send,
{
    loop {
        if let QueIn::Go(qc_in) = r.recv().unwrap() {
            s.proc(qc_in);
        } else {
            break;
        };
    }
}
