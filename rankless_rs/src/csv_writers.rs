use csv::Writer;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::{de::DeserializeOwned, Deserialize};
use std::fs::{create_dir_all, read_dir, File};
use std::io::{self, BufRead, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use tqdm::Iter;

use crate::common::Stowage;
use crate::oa_structs::{
    Ancestor, AssociatedInstitution, Author, Authorship, Biblio, Concept, FieldLike, Geo,
    IdCountDecorated, IdTrait, Institution, Location, OpenAccess, Publisher, RelatedConcept,
    Source, SubField, SummaryStats, Topic, Work, WorkTopic,
};

type GzInner = GzEncoder<BufWriter<File>>;
type GzWriter = Writer<GzInner>;

macro_rules! sub_write {
    ($parent:ident, $writer_name:ident, $field_name: ident) => {
        let parent_id = $parent.get_id();
        if let Some(ref mut child) = $parent.$field_name {
            child.parent_id = Some(parent_id.clone());
            $writer_name.serialize(child).unwrap();
        }
    };
}
macro_rules! sub_multi_write {
    ($parent:ident, $writer_name:ident, $field_name: ident) => {
        let parent_id = $parent.get_id();
        if let Some(children) = &mut $parent.$field_name {
            for record in children {
                record.parent_id = Some(parent_id.clone());
                $writer_name.serialize(record).unwrap();
            }
        }
    };
}
macro_rules! create_csv_struct {
    ($struct_name:ident, $($rest:ident),*) => {
        pub struct $struct_name {
            $($rest: GzWriter),*
        }

        impl $struct_name {
            pub fn new(root_path: &Path) -> Self {
                Self {
                    $($rest: get_writer(root_path, stringify!($rest)).unwrap()),*
                }
            }
        }

    };
}
macro_rules! create_complex_writers {
    ($($t_name: ident - $mod_name: ident; $($rest_key:ident => $rest_value: ident)&*; $($rest_single_key:ident -> $rest_single_value: ident)&*; $($rest_inner_key: ident)&*),*) => {

        $(pub mod $mod_name {

            use super::*;

            #[allow(dead_code)]
            pub const C: &str = stringify!($mod_name);

            #[allow(dead_code, non_upper_case_globals)]
            pub mod atts {
                $(pub const $rest_key: &str = stringify!($rest_key);)*
                $(pub const $rest_single_key: &str = stringify!($rest_single_key);)*
                $(pub const $rest_inner_key: &str = stringify!($rest_inner_key);)*
            }

            #[derive(Deserialize, Debug)]
            pub struct Decorated {
                #[serde(flatten)]
                child: IdCountDecorated<$t_name>,
                $( $rest_key: Option<Vec<$rest_value>>, )*
                $( $rest_single_key: Option<$rest_single_value>, )*
            }

            impl IdTrait for Decorated
            {
                fn get_id(&self) -> String {
                    self.child.get_id()
                }
            }

            create_csv_struct!(ModWriter, main, ids, counts $(, $rest_key)* $(, $rest_single_key)* $(, $rest_inner_key)*);


            impl ModWriter {

                fn write_line(&mut self, line: &str) {
                    #[allow(unused_mut)]
                    let mut outer: Decorated = deserialize_verbose(line);

                    $(let $rest_key = &mut self.$rest_key;)*
                    $(sub_multi_write!(outer, $rest_key, $rest_key);)*


                    $(let $rest_single_key = &mut self.$rest_single_key;)*
                    $(sub_write!(outer, $rest_single_key, $rest_single_key);)*

                    let mut parent: IdCountDecorated<$t_name> = outer.child;

                    $(let $rest_inner_key = &mut self.$rest_inner_key;)*
                    let _child = &mut parent.child;
                    $(sub_multi_write!(_child, $rest_inner_key, $rest_inner_key);)*

                    let id_writer = &mut self.ids;
                    let cb_writer = &mut self.counts;
                    sub_write!(parent, id_writer, ids);
                    sub_multi_write!(parent, cb_writer, counts_by_year);
                    self.main.serialize(parent.child).unwrap();
                }
            }

            pub fn write(
                in_root_str: &str,
                out_root_str: &str,
            ) -> io::Result<()> {
                    let slug = stringify!($mod_name);
                    let mut gz_files: Vec<PathBuf> = vec![];
                    let in_dir = Path::new(&in_root_str).join(slug);
                    fill_with_files(&in_dir, &mut gz_files, "gz").unwrap();

                    let out_dir = Path::new(&out_root_str).join(slug);
                    create_dir_all(&out_dir)?;
                    let mut writer = ModWriter::new(&out_dir);
                    for gz_path in gz_files.iter().tqdm().desc(Some(slug)) {
                        let file_gz = File::open(gz_path)?;
                        let gz_decoder = GzDecoder::new(file_gz);
                        let reader = BufReader::new(gz_decoder);
                        for line in reader.lines() {
                            writer.write_line(&line.unwrap());
                        }
                    }
                    Ok(())
            }
        })*


    };
}
macro_rules! macwrite {
    ($inp:ident, $outp:ident, $($modname:ident),*) => {
        $(
            $modname::write($inp, $outp)?;
        )*
    };
}

create_complex_writers!(
    Source - sources;;;,
    Publisher - publishers;;;,
    Author - authors;; summary_stats -> SummaryStats;,
    Topic - topics;;;,
    FieldLike - fields;;;,
    FieldLike - domains;;;,
    SubField - subfields;;;,
    Concept - concepts; ancestors => Ancestor & related_concepts => RelatedConcept;;,
    Institution - institutions; associated_institution => AssociatedInstitution; geo -> Geo;,
    Work - works;
    topics => WorkTopic &
    locations => Location &
    authorships => Authorship;
    biblio -> Biblio &
    open_access -> OpenAccess;
    referenced_works
);

fn get_writer(root: &Path, fname: &str) -> io::Result<GzWriter> {
    let file_csv = File::create(root.join(fname).with_extension("csv.gz"))?;
    let gz_encoder = GzEncoder::new(BufWriter::new(file_csv), Compression::default());
    return Ok(Writer::from_writer(gz_encoder));
}

fn fill_with_files(path: &Path, v: &mut Vec<PathBuf>, extension: &str) -> io::Result<()> {
    if path.is_dir() {
        for entry in read_dir(path)? {
            let sub_path = entry?.path();
            fill_with_files(&sub_path, v, extension)?;
        }
    } else if let Some(ext) = path.extension() {
        if ext == extension {
            v.push(path.to_path_buf());
        }
    }
    Ok(())
}

fn deserialize_verbose<T: DeserializeOwned>(s: &str) -> T {
    let deserializer = &mut serde_json::Deserializer::from_str(s);

    let result: Result<T, _> = serde_path_to_error::deserialize(deserializer);

    match result {
        Ok(r) => return r,
        Err(err) => {
            println!("err: {:?}", err);
            println!("s: {}", s);
            panic!("verbose err: {}", err);
        }
    }
}

pub fn write_csvs(in_root_str: &str, stowage: &Stowage) -> io::Result<()> {
    let out_root_str = &stowage.get_out_csv_path();
    macwrite!(
        in_root_str,
        out_root_str,
        fields,
        domains,
        subfields,
        topics,
        institutions,
        concepts,
        works,
        authors,
        publishers,
        sources
    );
    Ok(())
}
