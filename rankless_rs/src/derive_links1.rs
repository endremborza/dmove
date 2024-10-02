use std::{fmt::Debug, io};

use crate::{
    common::{init_empty_slice, BackendSelector, QuickestBox, QuickestVBox, ReadIter, Stowage},
    gen_entity_mapping::{Authors, Authorships, Institutions, Subfields, Topics, Works},
    gen_init_links::{
        AuthorshipAuthor, AuthorshipInstitutions, TopicSubfields, WorkAuthorships, WorkReferences,
        WorkTopics,
    },
};

use dmove::{
    BackendLoading, ByteArrayInterface, Entity, EntityImmutableRefMapperBackend,
    FixedAttributeElement, FixedSizeAttribute, Link, MappableEntity, NamespacedEntity,
    UnsignedNumber, VarAttBuilder, VarSizedAttributeElement, VariableSizeAttribute,
};

//TODO: all this is just a draft
//the structs annd iter impls
pub struct Unit<E: Entity> {
    e: E::T,
    done: bool,
}

pub struct Plural<E: Entity> {
    e: Box<[E::T]>,
    idx: usize,
}

impl<E: Entity> Unit<E> {
    fn new(e: E::T) -> Self {
        Self { e, done: false }
    }
}

impl<E: Entity> Plural<E> {
    fn new(e: Box<[E::T]>) -> Self {
        Self { e, idx: 0 }
    }
}

impl<E: Entity> Iterator for Unit<E>
where
    <E as Entity>::T: UnsignedNumber,
{
    type Item = E::T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            None
        } else {
            self.done = true;
            Some(self.e.lift())
        }
    }
}

impl<E: Entity> Iterator for Plural<E>
where
    <E as Entity>::T: UnsignedNumber,
{
    type Item = E::T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.e.len() {
            None
        } else {
            self.idx += 1;
            Some(self.e[self.idx - 1].lift())
        }
    }
}

pub fn invert_read_multi_link<L, S, T>(stowage: &mut Stowage, name: &str)
where
    S: Entity,
    T: Entity,
    L: Entity<T = Box<[T::T]>>
        + VariableSizeAttribute
        + MappableEntity<L, KeyType = usize>
        + NamespacedEntity,
    <S as Entity>::T: FixedAttributeElement + UnsignedNumber,
    <T as Entity>::T: UnsignedNumber + Debug + ByteArrayInterface,
{
    let interface = stowage.get_entity_interface::<L, ReadIter>();
    invert_multi_link::<L, S, T, _>(stowage, interface, name);
}

pub fn invert_multi_link<L, S, T, LIF>(stowage: &mut Stowage, interface: LIF, name: &str)
where
    S: Entity,
    T: Entity,
    L: Entity<T = Box<[T::T]>>, //TODO - very specific and needs pre-reading
    <S as Entity>::T: FixedAttributeElement + UnsignedNumber,
    <T as Entity>::T: UnsignedNumber + Debug,
    LIF: Iterator<Item = L::T>,
{
    let mut inverted = init_empty_slice::<T, Vec<S::T>>();
    for (source_id, target_slice) in interface.enumerate() {
        //TODO: if target is nullable and target id is 0, ignore
        for target_id in target_slice.iter() {
            inverted[target_id.to_usize()].push(S::T::from_usize(source_id))
        }
    }
    stowage.add_iter_owned::<VarAttBuilder, _, _>(
        inverted
            .into_vec()
            .into_iter()
            .map(|e| e.into_boxed_slice()),
        Some(name),
    );
    stowage.declare_link::<S, T>(name);
}

pub fn collapse_links<Link1, Link2>(stowage: &mut Stowage, name: &str)
where
    Link1: Link + NamespacedEntity + MappableEntity<Link1, KeyType = usize> + VariableSizeAttribute,
    Link2: Link
        + Entity<T = <<Link2 as Link>::Target as Entity>::T>
        + NamespacedEntity
        + MappableEntity<Link2, KeyType = usize>
        + FixedSizeAttribute,
    <Link2 as Link>::Target: Entity + MappableEntity<Link2::Target, KeyType = usize>,
    <<Link2 as Link>::Target as Entity>::T: PartialEq + ByteArrayInterface + UnsignedNumber,
    <<Link1 as Link>::Target as Entity>::T: UnsignedNumber,
    <Link1 as Entity>::T: ByteArrayInterface + VarSizedAttributeElement,
    <ReadIter as BackendSelector<Link1>>::BE:
        Iterator<Item = Box<[<<Link1 as Link>::Target as Entity>::T]>>,
    <QuickestBox as BackendSelector<Link2>>::BE: EntityImmutableRefMapperBackend<Link2, Link2>,
{
    let mut collapsed = Vec::new();
    let l1_interface = stowage.get_entity_interface::<Link1, ReadIter>();
    let l2_interface = stowage.get_entity_interface::<Link2, QuickestBox>();

    for mid_targets in l1_interface {
        let mut ends = Vec::new();
        for mt in mid_targets {
            let fw_target = l2_interface.get_ref_via_immut(&mt.to_usize()).unwrap();
            if !ends.contains(fw_target) {
                ends.push(fw_target.lift());
            }
        }
        collapsed.push(ends.into_boxed_slice());
    }
    stowage.add_iter_owned::<VarAttBuilder, _, _>(collapsed.into_iter(), Some(name));
    stowage.declare_link::<Link1::Source, Link2::Target>(name);
}

//TODO: dry it up!
pub fn collapse_links_mtarget<Link1, Link2, IfMarker, M, T>(stowage: &mut Stowage, name: &str)
where
    T: Entity,
    M: Entity,
    <T as Entity>::T: PartialEq + ByteArrayInterface + UnsignedNumber,
    <M as Entity>::T: UnsignedNumber + ByteArrayInterface,
    Link1: Entity<T = Box<[M::T]>>
        + NamespacedEntity
        + MappableEntity<Link1, KeyType = usize>
        + VariableSizeAttribute,
    Link2: Entity<T = Box<[T::T]>> + NamespacedEntity + MappableEntity<Link2, KeyType = usize>,
    <ReadIter as BackendSelector<Link1>>::BE: Iterator<Item = Link1::T>,
    <IfMarker as BackendSelector<Link2>>::BE:
        EntityImmutableRefMapperBackend<Link2, Link2> + BackendLoading<Link2>,
    IfMarker: BackendSelector<Link2>,
{
    let mut collapsed = Vec::new();
    let l1_interface = stowage.get_entity_interface::<Link1, ReadIter>();
    let l2_interface = stowage.get_entity_interface::<Link2, IfMarker>();

    for mid_targets in l1_interface {
        let mut ends = Vec::new();
        for mt in mid_targets {
            let fw_targets = l2_interface.get_ref_via_immut(&mt.to_usize()).unwrap();
            for fw_target in fw_targets {
                if !ends.contains(fw_target) {
                    //maybe too slow?
                    ends.push(fw_target.lift());
                }
            }
        }
        collapsed.push(ends.into_boxed_slice());
    }
    stowage.add_iter_owned::<VarAttBuilder, _, _>(collapsed.into_iter(), Some(name))
}

pub fn main(mut stowage: Stowage) -> io::Result<()> {
    invert_read_multi_link::<WorkReferences, Works, Works>(&mut stowage, "works-citing");

    collapse_links::<WorkTopics, TopicSubfields>(&mut stowage, "work-subfields");
    collapse_links::<WorkAuthorships, AuthorshipAuthor>(&mut stowage, "work-authors");
    collapse_links_mtarget::<
        WorkAuthorships,
        AuthorshipInstitutions,
        QuickestVBox,
        Authorships,
        Institutions,
    >(&mut stowage, "work-institutions");

    //invert_link::<WorkAuthors>("author-works");
    //invert_link::<WorkInstitutions>("institution-works"); // weighted?!

    // build_hierarchy::<WorkInstitutions, InstCountries>("country-hier");
    // build_hierarchy::<WorkSources + Year,SourceQ>("work-qd-sources")
    // build_hierarchy::<WorkSubfields, SubfieldAncestors>("topic-hier"

    //create filtered ones!
    //only _best_ q
    //derived multiple: Institution+Paper -> Coauthor institutions

    stowage.write_code()?;
    Ok(())
}
