use std::io;

use crate::{
    common::{
        init_empty_slice, BackendSelector, MainWorkMarker, MarkedBackendLoader, QuickAttPair,
        QuickestBox, QuickestVBox, ReadIter, Stowage,
    },
    gen::{
        a1_entity_mapping::{Countries, Institutions, Works},
        a2_init_atts::{
            AuthorshipAuthor, AuthorshipInstitutions, InstCountries, TopicSubfields,
            WorkAuthorships, WorkReferences, WorkSources, WorkTopics, WorkYears,
        },
    },
    ReadFixIter,
};

use dmove::{
    BackendLoading, ByteArrayInterface, ByteFixArrayInterface, CompactEntity, Entity,
    EntityImmutableRefMapperBackend, Link, MappableEntity, NamespacedEntity, UnsignedNumber,
    VarAttBuilder, VarSizedAttributeElement, VariableSizeAttribute, VattArrPair, ET,
};

use super::a1_entity_mapping::{YearInterface, N_PERS, POSSIBLE_YEAR_FILTERS};

pub struct WorkPeriods {}
pub struct CountryInsts {}

impl WorkPeriods {
    pub fn from_year(year: u16) -> ET<Self> {
        for i in (0..N_PERS).rev() {
            if year >= POSSIBLE_YEAR_FILTERS[i] {
                return i as u8;
            }
        }
        0
    }
}

impl MarkedBackendLoader<QuickestBox> for WorkPeriods {
    type BE = <QuickestBox as BackendSelector<Self>>::BE;
    fn load(stowage: &Stowage) -> Self::BE {
        let wys = stowage.get_entity_interface::<WorkYears, ReadFixIter>();
        wys.map(|y_id| {
            let y = YearInterface::reverse(y_id);
            Self::from_year(y)
        })
        .collect()
    }
}

impl MarkedBackendLoader<QuickestVBox> for CountryInsts {
    type BE = <QuickestVBox as BackendSelector<Self>>::BE;
    fn load(stowage: &Stowage) -> Self::BE {
        let inst_c = stowage.get_entity_interface::<InstCountries, ReadFixIter>();
        let mut c_insts = init_empty_slice::<Countries, Vec<ET<Institutions>>>();
        inst_c.enumerate().for_each(|(iid, cid)| {
            c_insts[cid.to_usize()].push(<Institutions as Entity>::T::from_usize(iid));
        });
        c_insts
            .to_vec()
            .into_iter()
            .map(|e| e.into_boxed_slice())
            .collect()
    }
}

impl MarkedBackendLoader<QuickAttPair> for CountryInsts {
    type BE = VattArrPair<Self, u32>;
    fn load(stowage: &Stowage) -> Self::BE {
        let boxes = <Self as MarkedBackendLoader<QuickestVBox>>::load(stowage);
        Self::BE::from_boxes(boxes)
    }
}

impl Entity for WorkPeriods {
    type T = u8;
    const N: usize = Works::N;
    const NAME: &'static str = "work-periods";
}

impl Entity for CountryInsts {
    type T = Box<[ET<Institutions>]>;
    const N: usize = Countries::N;
    const NAME: &'static str = "country-insts";
}

impl MappableEntity for WorkPeriods {
    type KeyType = usize;
}

impl MappableEntity for CountryInsts {
    type KeyType = usize;
}

impl VariableSizeAttribute for CountryInsts {
    type SizeType = u32;
}

pub fn invert_read_multi_link_to_work<L>(stowage: &mut Stowage, name: &str)
where
    L: Entity<T = Box<[<<L as Link>::Target as Entity>::T]>>
        + Link<Source = Works>
        + NamespacedEntity
        + CompactEntity
        + VariableSizeAttribute,
    <<L as Link>::Source as Entity>::T: UnsignedNumber,
    <<L as Link>::Target as Entity>::T: UnsignedNumber,
{
    let interface = stowage.get_entity_interface::<L, ReadIter>();
    invert_multi_link::<L, _>(stowage, interface, name);
    stowage.declare::<L::Target, MainWorkMarker>(name);
}

pub fn invert_multi_link<L, LIF>(stowage: &mut Stowage, interface: LIF, name: &str)
where
    L: Entity<T = Box<[<<L as Link>::Target as Entity>::T]>> + Link,
    <<L as Link>::Source as Entity>::T: UnsignedNumber,
    <<L as Link>::Target as Entity>::T: UnsignedNumber,
    LIF: Iterator<Item = L::T>,
{
    let mut inverted = init_empty_slice::<L::Target, Vec<<L::Source as Entity>::T>>();
    for (source_id, target_slice) in interface.enumerate() {
        //TODO: if target is nullable and target id is 0, ignore
        for target_id in target_slice.iter() {
            inverted[target_id.to_usize()].push(<L::Source as Entity>::T::from_usize(source_id))
        }
    }
    stowage.add_iter_owned::<VarAttBuilder, _, _>(
        inverted
            .into_vec()
            .into_iter()
            .map(|e| e.into_boxed_slice()),
        Some(name),
    );
    stowage.declare_link::<L::Target, L::Source>(name);
}

pub fn collapse_links<Link1, Link2>(stowage: &mut Stowage, name: &str)
where
    Link1: Link + NamespacedEntity + VariableSizeAttribute,
    Link2: Link
        + Entity<T = <<Link2 as Link>::Target as Entity>::T>
        + NamespacedEntity
        + CompactEntity,
    <Link1 as Entity>::T: ByteArrayInterface + VarSizedAttributeElement,
    <<Link1 as Link>::Target as Entity>::T: UnsignedNumber,
    <<Link2 as Link>::Target as Entity>::T: PartialEq + UnsignedNumber,
    <ReadIter as BackendSelector<Link1>>::BE:
        Iterator<Item = Box<[<<Link1 as Link>::Target as Entity>::T]>>,
{
    let cloj = |ends: &mut Vec<Link2::T>, fw_target: &Link2::T| {
        if !ends.contains(fw_target) {
            ends.push(fw_target.lift());
        }
    };
    collapse_links_meta::<Link1, Link2, QuickestBox, _>(stowage, name, cloj)
}

pub fn collapse_links_mtarget<Link1, Link2>(stowage: &mut Stowage, name: &str)
where
    Link1: Link + NamespacedEntity + VariableSizeAttribute,
    Link2: Entity<T = Box<[<Link2::Target as Entity>::T]>>
        + NamespacedEntity
        + CompactEntity
        + VariableSizeAttribute
        + Link,
    <Link1 as Entity>::T: ByteArrayInterface + VarSizedAttributeElement,
    <<Link1 as Link>::Target as Entity>::T: UnsignedNumber,
    <<Link2 as Link>::Target as Entity>::T: PartialEq + UnsignedNumber,
    <ReadIter as BackendSelector<Link1>>::BE:
        Iterator<Item = Box<[<<Link1 as Link>::Target as Entity>::T]>>,
{
    let cloj = |ends: &mut Vec<<Link2::Target as Entity>::T>, fw_targets: &Link2::T| {
        for fw_target in fw_targets {
            if !ends.contains(fw_target) {
                ends.push(fw_target.lift());
            }
        }
    };
    collapse_links_meta::<Link1, Link2, QuickestVBox, _>(stowage, name, cloj)
}

pub fn main(mut stowage: Stowage) -> io::Result<()> {
    invert_read_multi_link_to_work::<WorkReferences>(&mut stowage, "works-citing");
    invert_read_multi_link_to_work::<WorkTopics>(&mut stowage, "topic-works");
    invert_read_multi_link_to_work::<WorkSources>(&mut stowage, "source-works");

    collapse_links::<WorkTopics, TopicSubfields>(&mut stowage, "work-subfields");
    collapse_links::<WorkAuthorships, AuthorshipAuthor>(&mut stowage, "work-authors");
    collapse_links_mtarget::<WorkAuthorships, AuthorshipInstitutions>(
        &mut stowage,
        "work-institutions",
    );
    stowage.write_code()?;
    Ok(())
}

fn collapse_links_meta<Link1, Link2, IfMarker, F>(stowage: &mut Stowage, name: &str, fun: F)
where
    Link1: Link + NamespacedEntity + VariableSizeAttribute,
    Link2: Link + NamespacedEntity + CompactEntity,
    IfMarker: BackendSelector<Link2>,
    F: Fn(&mut Vec<<<Link2 as Link>::Target as Entity>::T>, &<Link2 as Entity>::T),
    Link1::T: ByteArrayInterface + VarSizedAttributeElement,
    <Link1::Target as Entity>::T: UnsignedNumber,
    <<Link2 as Link>::Target as Entity>::T: ByteFixArrayInterface,
    <ReadIter as BackendSelector<Link1>>::BE:
        Iterator<Item = Box<[<<Link1 as Link>::Target as Entity>::T]>>,
    <IfMarker as BackendSelector<Link2>>::BE:
        EntityImmutableRefMapperBackend<Link2> + BackendLoading<Link2>,
{
    let mut collapsed = Vec::new();
    let l1_interface = stowage.get_entity_interface::<Link1, ReadIter>();
    let l2_interface = stowage.get_entity_interface::<Link2, IfMarker>();
    for mid_targets in l1_interface {
        let mut ends = Vec::new();
        for mt in mid_targets {
            let fw_target = l2_interface.get_ref_via_immut(&mt.to_usize()).unwrap();
            fun(&mut ends, fw_target);
        }
        collapsed.push(ends.into_boxed_slice());
    }
    stowage.add_iter_owned::<VarAttBuilder, _, _>(collapsed.into_iter(), Some(name));
    stowage.declare_link::<Link1::Source, Link2::Target>(name);
}
