use std::{
    collections::VecDeque,
    io,
    sync::{Arc, Mutex},
    thread,
};

use tqdm::pbar;

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

pub trait Worker<S, T> {
    fn new(setup: Arc<S>) -> Self;

    fn proc(&self, input: T);
}

enum QueIn<T> {
    Go(T),
    Poison,
}

pub fn para_run<W, T, S, I>(in_v: I, setup: Arc<S>) -> io::Result<()>
where
    W: Worker<S, T>,
    I: Iterator<Item = T>,
    T: Send + 'static,
    S: Send + Sync + 'static,
{
    let n_threads: usize = std::thread::available_parallelism().unwrap().into();

    let in_q = Arc::new(Mutex::new(VecDeque::new()));

    let mut spawned_threads = Vec::new();
    for _ in 0..(n_threads) {
        let in_clone = Arc::clone(&in_q);
        let s_clone = setup.clone();
        spawned_threads.push(thread::spawn(move || subf::<W, _, _>(in_clone, s_clone)));
    }

    for e in in_v {
        in_q.lock().unwrap().push_front(QueIn::Go(e))
    }
    for _ in &spawned_threads {
        in_q.lock().unwrap().push_front(QueIn::Poison);
    }

    for done_thread in spawned_threads {
        done_thread.join().unwrap();
    }

    Ok(())
}

fn subf<W, S, T>(in_queue: Arc<Mutex<VecDeque<QueIn<T>>>>, s: Arc<S>)
where
    W: Worker<S, T>,
{
    let mut pbar = pbar(None);
    let w = W::new(s);

    loop {
        let queue_in = match in_queue.lock().unwrap().pop_back() {
            Some(q) => q,
            None => continue,
        };
        if let QueIn::Go(qc_in) = queue_in {
            pbar.update(1).unwrap();
            w.proc(qc_in);
        } else {
            break;
        };
    }
}
