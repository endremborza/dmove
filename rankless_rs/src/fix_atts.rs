use crate::{
    common::{field_id_parse, oa_id_parse, short_string_to_u64, BigId, ParsedId, Stowage},
    csv_writers::works,
    filters::{SWork, START_YEAR},
    gen_types::{
        Authors, Authorships, Countries, Fields, Institutions, Sources, Subfields, Topics, Works,
    },
    oa_structs::{
        post::{Authorship, SubField, Topic},
        Institution,
    },
};
use dmove::{
    para::Worker, Entity, FixAttBuilder, FixedAttributeElement, IdMappedEntity, LoadedIdMap,
    MainBuilder, MetaIntegrator,
};
use hashbrown::HashMap;
use serde::de::DeserializeOwned;
use std::{io, sync::Mutex, usize};

struct FixStow {
    stowage: Stowage,
    builder: MainBuilder,
}

struct Fatter<T> {
    id_map: LoadedIdMap,
    att_id_map: LoadedIdMap,
    attribute_arr: Mutex<Box<[T]>>,
}

trait IdToIntInterface {
    fn parse_id(id: &BigId) -> Self;
    fn null_value() -> Self;
}

trait FatGetter {
    fn get_fatt(&self, att_id_map: &LoadedIdMap) -> Option<BigId>;
}

macro_rules! uints {
    ($($u:ident),*) => {
        $(impl IdToIntInterface for $u {
            fn parse_id(id: &BigId) -> Self {
                *id as Self
            }
            fn null_value() -> Self {0}
        })*
    };
}

uints!(u8, u16, u32, u64);

impl FixStow {
    fn new(stowage: Stowage) -> Self {
        Self {
            builder: MainBuilder::new(&stowage.fix_atts),
            stowage,
        }
    }

    fn write_all(mut self) -> io::Result<usize> {
        self.object_property::<Institution, Institutions, Countries>("inst-countries");
        self.object_property::<SubField, Subfields, Fields>("subfield-ancestors");
        self.object_property::<Topic, Topics, Subfields>("topic-subfields");
        self.run_fatt::<Works, u8, SWork>("work-years", &HashMap::new());
        //
        // let source_map = self.stowage.get_idmap::<Sources>().to_map();
        let author_map = self.stowage.get_idmap::<Authors>().to_map();
        let mut a2ship = vec![0; Authorships::N];
        self.stowage
            .read_csv_objs::<Authorship>(works::C, works::atts::authorships)
            .enumerate()
            .for_each(|(i, ship)| {
                if let Some(aid) = author_map.get(&oa_id_parse(&ship.author_id.unwrap())) {
                    a2ship[i] = *aid;
                };
                ship.institutions;
            });
        self.builder
            .write_code("rankless_rs/src/gen_fix_att_structs.rs")
    }

    fn object_property<T, P, C>(&mut self, fatt_name: &str)
    where
        T: FatGetter + ParsedId + DeserializeOwned + Send,
        P: Entity + IdMappedEntity,
        C: Entity + IdMappedEntity,
        <C as Entity>::T: Send + Clone + IdToIntInterface + FixedAttributeElement,
    {
        let id_map = self.stowage.get_idmap::<C>().to_map();
        self.builder.declare_link::<P, C>(fatt_name);
        self.run_fatt::<P, C::T, T>(fatt_name, &id_map)
    }

    fn run_fatt<P, T, R>(&mut self, fatt_name: &str, att_id_map: &LoadedIdMap)
    where
        P: Entity + IdMappedEntity,
        T: IdToIntInterface + Clone + Send + FixedAttributeElement,
        R: DeserializeOwned + ParsedId + FatGetter + Send,
    {
        let obj_id_map = self.stowage.get_idmap::<P>();
        let attribute_arr: Vec<T> = vec![T::null_value(); P::N + 1];

        let setup = Fatter {
            id_map: obj_id_map.to_map(),
            att_id_map: att_id_map.clone(),
            attribute_arr: Mutex::new(attribute_arr.into_boxed_slice()),
        };

        let att_arr = setup
            .para(self.stowage.read_csv_objs::<R>(P::NAME, "main"))
            .attribute_arr
            .lock()
            .unwrap()
            .to_owned()
            .to_vec();

        FixAttBuilder::add_iter(&mut self.builder, att_arr.iter(), fatt_name)
    }
}

impl FatGetter for SubField {
    fn get_fatt(&self, field_id_map: &LoadedIdMap) -> Option<BigId> {
        Some(*field_id_map.get(&field_id_parse(&self.field))?)
    }
}

impl FatGetter for Topic {
    fn get_fatt(&self, subfield_id_map: &LoadedIdMap) -> Option<BigId> {
        Some(*subfield_id_map.get(&field_id_parse(&self.subfield))?)
    }
}

impl FatGetter for SWork {
    fn get_fatt(&self, _: &LoadedIdMap) -> Option<BigId> {
        if let Some(year) = &self.publication_year {
            return Some((year - START_YEAR) as BigId); //TODO: this is not really an id
        };
        None
    }
}

impl FatGetter for Institution {
    fn get_fatt(&self, att_id_map: &LoadedIdMap) -> Option<BigId> {
        if let Some(cc_id) = &self.country_code {
            return Some(*att_id_map.get(&short_string_to_u64(&cc_id)).unwrap());
        }
        return None;
    }
}

impl<I, O> Worker<I> for Fatter<O>
where
    I: ParsedId + Send + FatGetter,
    O: IdToIntInterface + Send + Sized,
{
    fn proc(&self, input: I) {
        let in_id = input.get_parsed_id();
        if let (Some(oid), Some(inid)) = (self.id_map.get(&in_id), input.get_fatt(&self.att_id_map))
        {
            self.attribute_arr.lock().unwrap()[*oid as usize] = O::parse_id(&inid);
        }
    }
}

pub fn write_fix_atts(stowage: Stowage) -> io::Result<usize> {
    FixStow::new(stowage).write_all()
}
