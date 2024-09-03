use std::{
    fs::{create_dir_all, File},
    io::{self, Write},
};

use crate::{
    common::{meta_att_struct_get, ByteArrayInterface, MainBuilder},
    prefix_maker, Entity,
};

prefix_maker!(VariableSizeAtt);

pub struct FilePointer<O, S>
where
    O: Sized + ByteArrayInterface,
    S: Sized + ByteArrayInterface,
{
    offset: O,
    size: S,
}

struct OuterFilePointer {
    offset: usize,
    size: usize,
}

pub trait VarAttBuilder {
    fn extend_with_var_att<S: Entity, T, I: Iterator<Item = T>>(&mut self, iter: I, name: &str)
    where
        T: VarAttributeElement;
    fn write_var_atts(&self, path: &str) -> std::io::Result<usize>;
}

pub trait VarAttributeElement: ByteArrayInterface {
    const DIVISOR: usize = 1;
}

impl Clone for OuterFilePointer {
    fn clone(&self) -> Self {
        Self {
            offset: self.offset.clone(),
            size: self.size.clone(),
        }
    }
}

impl VarAttBuilder for MainBuilder {
    fn extend_with_var_att<S, T, I: Iterator<Item = T>>(&mut self, iter: I, name: &str)
    where
        S: Entity,
        T: VarAttributeElement,
    {
        //NOTE: all pointers need to fit into memory
        //if that's infeasable, pointers need to be usize/usize
        let att_dir = self.parent_root.join(S::NAME);
        create_dir_all(&att_dir).unwrap();

        let mut ptr = OuterFilePointer { offset: 0, size: 0 };
        let mut pointers: Vec<OuterFilePointer> = Vec::new();
        let mut max_size: usize = 0;
        let mut targets_file = File::create(&att_dir.join("targets")).unwrap();
        for ts in iter {
            let barr = ts.to_bytes();
            targets_file.write(&barr).expect("target writing");
            ptr.size = barr.len();
            if ptr.size > max_size {
                max_size = ptr.size
            }
            pointers.push(ptr.clone());
            ptr.offset += ptr.size;
        }
        let n = S::N;
        assert_eq!(pointers.len(), n);
        let mut counts_file = File::create(&att_dir.join("sizes")).unwrap();
        for optr in pointers {
            // TODO todo!();
            // u8-u64 based on max value / DIVISOR
            type OT = u64;
            let ptr = FilePointer {
                offset: optr.offset as OT,
                size: ptr.size as u32,
            };
            write_to_sizes(&mut counts_file, &ptr).expect("size writing");
        }

        let type_name = std::any::type_name::<T>();
        self.struct_defs.push(get_struct_def(name, type_name, n));
    }

    fn write_var_atts(&self, path: &str) -> std::io::Result<usize> {
        self.write_structs(path, PREFIX_STR)
    }
}

fn write_to_sizes<T: Write, O, S>(writer: &mut T, ptr: &FilePointer<O, S>) -> io::Result<usize>
where
    O: ByteArrayInterface,
    S: ByteArrayInterface,
{
    writer.write(&ptr.offset.to_bytes())?;
    writer.write(&ptr.size.to_bytes())
}
