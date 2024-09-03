use csv::Writer;
use flate2::{bufread::GzEncoder, Compression};
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use std::fs::{read_dir, DirEntry, File};
use std::io::{self, prelude::*, Cursor};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tqdm::Iter;

use crate::{para::Worker, quercus::Quercus};

pub type QP4 = QuercusPacket<Arc<[QP3]>>;
type QP3 = QuercusPacket<Arc<[QP2]>>;
type QP2 = QuercusPacket<Arc<[QP1]>>;
type QP1 = QuercusPacket<Arc<[QP0]>>;
type QP0 = QuercusPacket<()>;

type InputType = (String, DirEntry);

#[derive(Serialize, Deserialize, Debug)]
pub struct QuercusPacket<T> {
    pub id: u16,
    pub weight: u32,
    pub source_count: u32,
    pub top_source: (u64, u32),
    pub children: T,
}

#[derive(Serialize, Debug)]
struct FStat {
    name: String,
    old: usize,
    new: usize,
    new_compressed: usize,
}

pub trait FromQcChildren {
    fn from_qcc(qcc: &HashMap<u16, Quercus>) -> Self;
    fn to_qcc(&self) -> HashMap<u16, Quercus>;
}

impl FromQcChildren for () {
    fn from_qcc(_: &HashMap<u16, Quercus>) -> Self {
        ()
    }
    fn to_qcc(&self) -> HashMap<u16, Quercus> {
        HashMap::new()
    }
}

impl<T: FromQcChildren + std::fmt::Debug> FromQcChildren for Arc<[QuercusPacket<T>]> {
    fn from_qcc(qcc: &HashMap<u16, Quercus>) -> Self {
        let mut out = Vec::new();
        for (k, qc) in qcc.into_iter() {
            out.push(QuercusPacket::<T>::from_qc(qc, *k));
        }
        out.try_into().unwrap()
    }

    fn to_qcc(&self) -> HashMap<u16, Quercus> {
        let mut out = HashMap::new();
        for qcp in self.into_iter() {
            let k = qcp.id.clone();
            let qc = qcp.to_qc();
            out.insert(k, qc);
        }
        out.try_into().unwrap()
    }
}

impl<T: FromQcChildren> QuercusPacket<T> {
    pub fn from_qc(qc: &Quercus, id: u16) -> Self {
        Self {
            id,
            weight: qc.weight,
            source_count: qc.source_count as u32,
            top_source: qc.top_source,
            children: T::from_qcc(&qc.children),
        }
    }

    pub fn to_qc(&self) -> Quercus {
        Quercus {
            weight: self.weight,
            source_count: self.source_count as usize,
            top_source: self.top_source,
            sources: HashMap::new(),
            children: self.children.to_qcc(),
        }
    }
}

pub fn dump_packets(stowage: Stowage) -> io::Result<()> {
    let a: Arc<[u16]> = (1..5).collect();
    let buf = bincode::serialize(&a).unwrap();

    println!("{:?}", buf);

    let ocsv = csv::Writer::from_path(Path::new("/home/borza/tmp/sizes.csv")).unwrap();
    let csv_mex = Mutex::new(ocsv);

    Fstatter(csv_mex).para(stowage.iter_pruned_qc_locs());

    Ok(())
}

struct Fstatter(Mutex<Writer<File>>);

impl Worker<InputType> for Fstatter {
    fn proc(&self, input: InputType) {
        let (filter_name, dirpath) = input;
        let idname = format!(
            "{}/{}",
            dirpath.file_name().to_str().to_owned().unwrap(),
            filter_name
        );
        for de in read_dir(dirpath.path()).unwrap().tqdm().desc(Some(idname)) {
            let de = de.unwrap();
            let old = std::fs::metadata(de.path()).unwrap().len() as usize;
            let qc = read_js_path::<Quercus>(de.path().to_str().unwrap()).unwrap();
            let qp = QP3::from_qc(&qc, 0);
            let buf = bincode::serialize(&qp).unwrap();
            let new = buf.len();
            let mut cvec = Vec::new();
            let mut gz = GzEncoder::new(Cursor::new(buf), Compression::fast());
            gz.read_to_end(&mut cvec).unwrap();
            let new_compressed = cvec.len();
            let fstat = FStat {
                name: format!(
                    "{:?}/{}/{:?}",
                    dirpath.file_name(),
                    filter_name,
                    de.file_name(),
                ),
                old,
                new,
                new_compressed,
            };
            self.0.lock().unwrap().serialize(fstat).unwrap();
        }
    }
}
