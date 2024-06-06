use csv::Writer;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, read_dir, File};
use std::io::{self, BufRead, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use tqdm::Iter;

use crate::oa_structs::{
    Ancestor, AssociatedInstitution, Author, Authorship, Biblio, Concept, FieldLike, Geo,
    IdCountDecorated, IdTrait, Institution, Location, OpenAccess, Publisher, RelatedConcept,
    Source, SubField, Topic, Work, WorkTopic,
};

use crate::common::{
    AUTHORS, CONCEPTS, DOMAINS, FIELDS, INSTS, PUBLISHERS, SOURCES, SUB_FIELDS, TOPICS, WORKS,
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
            fn new(root_path: &Path) -> Self {
                Self {
                    $($rest: get_writer(root_path, stringify!($rest)).unwrap()),*
                }
            }
        }

    };
}
macro_rules! create_writer {
    ($struct_name:ident, $t_name: ident) => {
        struct $struct_name {}

        impl FullWriter for $struct_name {
            fn new(_: &Path) -> Self {
                $struct_name {}
            }

            fn write_line(&mut self, line: &str, writer: &mut BaseCsvWriter) {
                let parent: IdCountDecorated<$t_name> = deserialize_verbose(line);
                write_decorated::<$t_name>(parent, writer).expect("couldn't write");
            }
        }
    };
}

create_csv_struct!(BaseCsvWriter, main, ids, counts);

trait FullWriter {
    fn new(out_dir: &Path) -> Self;

    fn write_line(&mut self, line: &str, writer: &mut BaseCsvWriter);
}

create_writer!(SourceWriter, Source);
create_writer!(PubWriter, Publisher);
create_writer!(AuthorWriter, Author);
create_writer!(TopicWriter, Topic);
create_writer!(FieldWriter, FieldLike);
create_writer!(SubFieldWriter, SubField);

macro_rules! create_decorated_struct {
    ($writer_name: ident, $csv_writer: ident, $decor_name:ident, $t_name: ident $(,V $rest_key:ident => $rest_value: ident)* $(,S $rest_single_key:ident -> $rest_single_value: ident)* $(,I $rest_inner_key: ident)*) => {

        #[derive(Deserialize, Debug)]
        pub struct $decor_name {
            #[serde(flatten)]
            child: IdCountDecorated<$t_name>,
            $( $rest_key: Option<Vec<$rest_value>> ),*,
            $( $rest_single_key: Option<$rest_single_value> ),*
        }

        impl IdTrait for $decor_name
        {
            fn get_id(&self) -> String {
                self.child.get_id()
            }
        }

        create_csv_struct!($csv_writer, $($rest_key),* $(, $rest_single_key)* $(, $rest_inner_key)* );

        struct $writer_name {
            extras: $csv_writer
        }

        impl FullWriter for $writer_name {
            fn new(out_dir: &Path) -> Self {
                Self {
                    extras: $csv_writer::new(out_dir),
                }
            }

            fn write_line(&mut self, line: &str, writer: &mut BaseCsvWriter) {
                let mut outer: $decor_name = deserialize_verbose(line);

                $(let $rest_key = &mut self.extras.$rest_key;)*
                $(sub_multi_write!(outer, $rest_key, $rest_key);)*


                $(let $rest_single_key = &mut self.extras.$rest_single_key;)*
                $(sub_write!(outer, $rest_single_key, $rest_single_key);)*

                let mut parent: IdCountDecorated<$t_name> = outer.child;


                $(let $rest_inner_key = &mut self.extras.$rest_inner_key;)*
                let _child = &mut parent.child;
                $(sub_multi_write!(_child, $rest_inner_key, $rest_inner_key);)*

                let _ = write_decorated::<$t_name>(parent, writer);
            }
        }


    };
}

create_decorated_struct!(ConceptWriter, ConceptCsvWriter, DecoratedConcept, Concept,V ancestors => Ancestor,V related_concepts => RelatedConcept);
create_decorated_struct!(InstitutionWriter, InstitutionCsvWriter, DecoratedInstitution, Institution,V  associated_institution => AssociatedInstitution,S geo -> Geo);
create_decorated_struct!(
    WorkWriter,
    WorkCsvWriter,
    DecoratedWork,
    Work,
    V topics => WorkTopic,
    V locations => Location,
    V authorships => Authorship,
    S biblio -> Biblio,
    S open_access -> OpenAccess,
    I referenced_works
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
            println!("{:?}", err);
            println!("{}", s);
            panic!("{}", err);
        }
    }
}

fn write_decorated<T>(mut parent: IdCountDecorated<T>, writer: &mut BaseCsvWriter) -> io::Result<()>
where
    T: DeserializeOwned + IdTrait + Serialize,
{
    let id_writer = &mut writer.ids;
    let cb_writer = &mut writer.counts;
    sub_write!(parent, id_writer, ids);
    sub_multi_write!(parent, cb_writer, counts_by_year);
    writer.main.serialize(parent.child)?;
    Ok(())
}

fn write_main_struct<T>(
    in_root_str: &str,
    out_root_str: &str,
    slug: &str,
    take_n: Option<usize>,
) -> io::Result<()>
where
    T: FullWriter,
{
    let mut gz_files: Vec<PathBuf> = vec![];
    let in_dir = Path::new(&in_root_str).join(slug);
    fill_with_files(&in_dir, &mut gz_files, "gz").unwrap();

    let out_dir = Path::new(&out_root_str).join(slug);
    fs::create_dir_all(&out_dir)?;
    let mut base_writer = BaseCsvWriter::new(&out_dir);
    let mut full_writer = T::new(&out_dir);
    for gz_path in gz_files.iter().tqdm().desc(Some(slug)) {
        let file_gz = File::open(gz_path)?;
        let gz_decoder = GzDecoder::new(file_gz);
        let reader = BufReader::new(gz_decoder);
        let mut liter: Box<dyn Iterator<Item = Result<String, std::io::Error>>> =
            Box::new(reader.lines());
        if let Some(n) = take_n {
            liter = Box::new(liter.take(n));
        }

        for line in liter {
            full_writer.write_line(&line.unwrap(), &mut base_writer);
        }
    }
    Ok(())
}

macro_rules! macwrite {
    ($a1:ident, $a2:ident, $a3:ident, $($entity:ident, $fname:ident),*) => {
        $(
            write_main_struct::<$entity>($a1, $a2, $fname, $a3)?;
        )*
    };
}

pub fn write_csvs(in_root_str: &str, out_root_str: &str, n: Option<usize>) -> io::Result<()> {
    macwrite!(
        in_root_str,
        out_root_str,
        n,
        FieldWriter,
        FIELDS,
        FieldWriter,
        DOMAINS,
        SubFieldWriter,
        SUB_FIELDS,
        TopicWriter,
        TOPICS,
        InstitutionWriter,
        INSTS,
        ConceptWriter,
        CONCEPTS,
        WorkWriter,
        WORKS,
        AuthorWriter,
        AUTHORS,
        PubWriter,
        PUBLISHERS,
        SourceWriter,
        SOURCES
    );
    Ok(())
}
