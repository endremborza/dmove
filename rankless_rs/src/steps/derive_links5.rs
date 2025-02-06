use std::io;

use dmove::{
    DowncastingBuilder, Entity, FixAttBuilder, MarkedAttribute, NamespacedEntity, UnsignedNumber,
    VariableSizeAttribute, ET, MAA,
};
use hashbrown::HashMap;

use crate::{
    common::{
        init_empty_slice, BeS, CitSubfieldsArrayMarker, MainWorkMarker, QuickAttPair, QuickMap,
        RefSubfieldsArrayMarker,
    },
    gen::{
        a1_entity_mapping::{
            Authors, Countries, Institutions, Qs, Sources, Subfields, Topics, Works,
        },
        a2_init_atts::{SourceYearQs, WorkYears},
        derive_links1::{WorkSubfields, WorksCiting},
    },
    CiteCountMarker, QuickestBox, ReadIter, Stowage,
};

struct CiteDeriver {
    pub stowage: Stowage,
    wciting: BeS<QuickAttPair, WorksCiting>,
    w_sfs: BeS<QuickAttPair, WorkSubfields>,
}

impl CiteDeriver {
    pub fn new(stowage: Stowage) -> Self {
        let wciting = stowage.get_entity_interface::<WorksCiting, QuickAttPair>();
        let w_sfs = stowage.get_entity_interface::<WorkSubfields, QuickAttPair>();
        Self {
            stowage,
            wciting,
            w_sfs,
        }
    }

    pub fn cite_count<E>(&mut self)
    where
        E: MarkedAttribute<MainWorkMarker>,
        MAA<E, MainWorkMarker>:
            Entity<T = Box<[ET<Works>]>> + NamespacedEntity + VariableSizeAttribute,
    {
        let wc_interface = self
            .stowage
            .get_entity_interface::<MAA<E, MainWorkMarker>, ReadIter>();

        let mut ref_subfields: Vec<[u32; Subfields::N]> = Vec::new();
        let mut cit_subfields: Vec<[u32; Subfields::N]> = Vec::new();

        let iter = wc_interface.map(|ws| {
            let mut ref_rec = [0; Subfields::N];
            let mut cit_rec = [0; Subfields::N];
            let sum = ws
                .iter()
                .map(|wid| {
                    extend_sfs_get_ccount(
                        wid,
                        &mut ref_rec,
                        &mut cit_rec,
                        &self.w_sfs,
                        &self.wciting,
                    )
                })
                .sum();
            ref_subfields.push(ref_rec);
            cit_subfields.push(cit_rec);
            sum
        });

        add_iter::<E, _>(&mut self.stowage, iter);
        add_sf_iters::<E>(&mut self.stowage, ref_subfields, cit_subfields);
    }

    fn q_ccs(&mut self) {
        let mut q_maps = init_empty_slice::<Qs, HashMap<ET<Works>, usize>>();
        let wc_interface = self
            .stowage
            .get_entity_interface::<MAA<Sources, MainWorkMarker>, ReadIter>();
        let qy_map = self
            .stowage
            .get_entity_interface::<SourceYearQs, QuickMap>();
        let wyears = self
            .stowage
            .get_entity_interface::<WorkYears, QuickestBox>();
        let qc_name = format!("qs-cite-count");

        let mut ref_subfields: Vec<[u32; Subfields::N]> = Vec::new();
        let mut cit_subfields: Vec<[u32; Subfields::N]> = Vec::new();

        let iter = wc_interface.enumerate().map(|(i, ws)| {
            let sid = ET::<Sources>::from_usize(i);
            let mut ref_rec = [0; Subfields::N];
            let mut cit_rec = [0; Subfields::N];
            let sum = ws
                .iter()
                .map(|e| {
                    let wind = *e as usize;
                    let year = wyears[wind];
                    let q = qy_map.get(&(sid, year)).unwrap_or(&0);
                    let wcount = extend_sfs_get_ccount(
                        e,
                        &mut ref_rec,
                        &mut cit_rec,
                        &self.w_sfs,
                        &self.wciting,
                    );
                    q_maps[*q as usize].insert(*e, wcount);
                    wcount
                })
                .sum();

            ref_subfields.push(ref_rec);
            cit_subfields.push(cit_rec);
            sum
        });

        add_iter::<Sources, _>(&mut self.stowage, iter);
        add_sf_iters::<Sources>(&mut self.stowage, ref_subfields, cit_subfields);

        self.stowage.add_iter_owned::<DowncastingBuilder, _, _>(
            q_maps.iter().map(|e| e.values().sum()),
            Some(&qc_name),
        );
        self.stowage.declare::<Qs, CiteCountMarker>(&qc_name);
    }
}

pub fn main(stowage: Stowage) -> io::Result<()> {
    let mut deriver = CiteDeriver::new(stowage);
    deriver.cite_count::<Institutions>();
    deriver.cite_count::<Authors>();
    deriver.cite_count::<Countries>();
    deriver.cite_count::<Subfields>();
    deriver.cite_count::<Topics>();
    deriver.q_ccs();
    deriver.stowage.write_code()?;
    Ok(())
}

fn extend_sfs_get_ccount(
    wid: &ET<Works>,
    ref_subfields: &mut [u32],
    cit_subfields: &mut [u32],
    w_sfs: &BeS<QuickAttPair, WorkSubfields>,
    wciting: &BeS<QuickAttPair, WorksCiting>,
) -> usize {
    let wu = wid.to_usize();
    let mut ccount = 0;
    for sf_id in w_sfs.get(&wu).unwrap() {
        ref_subfields[*sf_id as usize] += 1;
    }
    for c_wid in wciting.get(&wu).unwrap() {
        ccount += 1;
        for sf_id in w_sfs.get(&c_wid.to_usize()).unwrap() {
            cit_subfields[*sf_id as usize] += 1;
        }
    }
    ccount
}

fn add_iter<E, I>(stowage: &mut Stowage, iter: I)
where
    E: Entity,
    I: Iterator<Item = usize>,
{
    let cc_name = format!("{}-cite-count", E::NAME);
    stowage.add_iter_owned::<DowncastingBuilder, _, _>(iter, Some(&cc_name));
    stowage.declare::<E, CiteCountMarker>(&cc_name);
}

fn add_sf_iters<E>(
    stowage: &mut Stowage,
    ref_subfields: Vec<[u32; Subfields::N]>,
    cit_subfields: Vec<[u32; Subfields::N]>,
) where
    E: Entity,
{
    let rsf_name = format!("{}-ref-subfields", E::NAME);
    stowage.add_iter_owned::<FixAttBuilder, _, _>(ref_subfields.into_iter(), Some(&rsf_name));
    stowage.declare::<E, RefSubfieldsArrayMarker>(&rsf_name);

    let cit_name = format!("{}-cit-subfields", E::NAME);
    stowage.add_iter_owned::<FixAttBuilder, _, _>(cit_subfields.into_iter(), Some(&cit_name));
    stowage.declare::<E, CitSubfieldsArrayMarker>(&cit_name);
}
