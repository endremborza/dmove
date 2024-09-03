impl MappedAttributes {
    fn from_hedges<T1, T2>(hedges: Vec<HierEdge<T1, T2>>) -> Self
    where
        SmolId: From<T1> + From<T2>,
        T1: Copy,
        T2: Copy,
    {
        type InnerType = SmolId;
        //TODO: spare some space with repetitions
        let mut outer = Vec::new();
        for hedge in hedges {
            let hedge_main = InnerType::try_from(hedge.id).unwrap();
            let mut inner = Vec::new();
            for subsubid in &hedge.subs {
                inner.push(InnerType::try_from(*subsubid).unwrap());
            }
            outer.push((hedge_main, MappedAttributes::List(inner.into_boxed_slice())))
        }
        Self::Map(outer.into_boxed_slice())
    }

    pub fn iter_inner(&self) -> std::slice::Iter<'_, (SmolId, MappedAttributes)> {
        match self {
            Self::List(_) => panic!("no more levels"),
            Self::Map(vhs) => vhs.iter(),
        }
    }
}

pub type AttributeResolverMap = HashMap<String, MappContainer>;
pub struct MappContainer {
    mapps: Box<[MappedAttributes]>,
}

pub enum MappedAttributes {
    List(Box<[SmolId]>),
    Map(Box<[(SmolId, MappedAttributes)]>),
}

impl MappContainer {
    pub fn get(&self, id: &MidId) -> Option<&MappedAttributes> {
        Some(&self.mapps[*id as usize])
    }

    pub fn from_name<T1, T2>(stowage: &Stowage, var_att_name: &str) -> Self
    where
        SmolId: From<T1> + From<T2>,
        T1: Copy + ByteConvert,
        T2: Copy + ByteConvert,
    {
        let base = VarReader::<Vec<HierEdge<T1, T2>>>::new(stowage, var_att_name);
        let mut mapp = Vec::new();
        for hedges in base
            .tqdm()
            .desc(Some(format!("ares from {}", var_att_name)))
        {
            mapp.push(MappedAttributes::from_hedges(hedges));
        }
        Self {
            mapps: mapp.into_boxed_slice(),
        }
    }
}

pub fn get_mapped_atts(resolver_id: &str) -> Vec<String> {
    let mut hm = HashMap::new();
    hm.insert(
        vnames::COUNTRY_H,
        vec![COUNTRIES.to_string(), institutions::C.to_string()],
    );
    hm.insert(
        vnames::CONCEPT_H,
        vec![fields::C.to_string(), subfields::C.to_string()],
    );
    // hm.insert(vnames::W2S, vec![sources::C.to_string()]);
    hm.insert(vnames::W2QS, vec![QS.to_string(), sources::C.to_string()]);
    hm.get(resolver_id).unwrap().to_vec()
}

pub fn get_attribute_resolver_map(stowage: &Stowage) -> AttributeResolverMap {
    let mut ares_map = HashMap::new();
    build_ar_map::<CountryId, InstId>(stowage, vnames::COUNTRY_H, &mut ares_map);
    build_ar_map::<FieldId, SubFieldId>(stowage, vnames::CONCEPT_H, &mut ares_map);
    build_ar_map::<QId, SourceId>(stowage, vnames::W2QS, &mut ares_map);
    ares_map
}

fn build_ar_map<T1, T2>(stowage: &Stowage, var_att_name: &str, ares_map: &mut AttributeResolverMap)
where
    SmolId: From<T1> + From<T2>,
    T1: Copy + ByteConvert,
    T2: Copy + ByteConvert,
{
    ares_map.insert(
        var_att_name.to_string(),
        MappContainer::from_name::<T1, T2>(stowage, var_att_name),
    );
    println!("built, inserted");
}
