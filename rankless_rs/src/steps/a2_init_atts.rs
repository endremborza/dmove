use crate::{
    common::{
        field_id_parse, init_empty_slice, oa_id_parse, short_string_to_u64, BeS, DoiMarker,
        MainEntity, NameExtensionMarker, NameMarker, ParsedId, QuickestNumbered, Stowage,
        MAIN_NAME, NET,
    },
    csv_writers::{institutions, works},
    gen::a1_entity_mapping::{
        AreaFields, Authors, Authorships, Countries, Fields, Institutions, Sources, Subfields,
        Topics, Works,
    },
    oa_structs::{
        post::{read_post_str_arr, Authorship, Institution, Location, Source, SubField, Topic},
        FieldLike, Geo, Named, NamedEntity, ReferencedWork, Work, WorkTopic,
    },
    steps::a1_entity_mapping::{iter_authorships, Qs, SourceArea, YearInterface, Years},
};
use dmove::{
    para::Worker, BigId, DiscoMapEntityBuilder, Entity, EntityImmutableMapperBackend,
    FixAttBuilder, InitEmpty, LoadedIdMap, MappableEntity, MetaIntegrator, NamespacedEntity,
    UnsignedNumber, VarAttBuilder, ET,
};
use levenshtein::levenshtein;
use serde::{de::DeserializeOwned, Deserialize};
use std::{
    cmp::min,
    io,
    marker::PhantomData,
    sync::{Arc, Mutex},
    usize,
};
use tqdm::Iter;

const MIN_TOPIC_SCORE: f64 = 0.7;
const MIN_RATE: f64 = 0.8;
const MIN_LEN: usize = 10;

#[derive(Deserialize)]
struct SourceQ {
    publication_year: u16,
    id: BigId,
    best_q: u8,
}

struct ShipRelWriter {
    ship2a: Mutex<Box<[ET<Authors>]>>,
    ship2is: Mutex<Box<[Vec<ET<Institutions>>]>>,
    w2ships: Mutex<Box<[Vec<ET<Authorships>>]>>,
    winf: Arc<LoadedIdMap<ET<Works>>>,
    ainf: Arc<LoadedIdMap<ET<Authors>>>,
    iinf: Arc<LoadedIdMap<ET<Institutions>>>,
}

struct WorkAttWriter {
    wyears: Mutex<Box<[ET<Years>]>>,
    wnames: Mutex<Box<[String]>>,
    wdois: Mutex<Box<[String]>>,
    winf: Arc<LoadedIdMap<ET<Works>>>,
}

struct BoxRoller<T, E> {
    arr: std::vec::IntoIter<T>,
    phantom: PhantomData<fn() -> E>,
}

struct StrWriter<'a> {
    stowage: &'a Stowage,
    main: &'static str,
    sub: &'static str,
}

struct GenObjAttWorker<'a, Source, Target, StoredOfTarget, SourceIF, TargetIF>
where
    Source: MainEntity,
    Target: MappableEntity,
    StoredOfTarget: Sync + Send,
    TargetIF: EntityImmutableMapperBackend<Target>,
    SourceIF: EntityImmutableMapperBackend<Source>,
{
    data_worker: DataAttWorker<'a, Source, StoredOfTarget, SourceIF>,
    att_interface: &'a TargetIF,
    p: PhantomData<fn() -> Target>,
}

struct DataAttWorker<'a, Source, TargetType, SourceIF>
where
    Source: MainEntity,
    TargetType: Sync + Send,
    SourceIF: EntityImmutableMapperBackend<Source>,
{
    self_interface: &'a SourceIF,
    attribute_arr: Mutex<Box<[TargetType]>>,
    p: PhantomData<fn() -> Source>,
}

struct GenWorker<W, PreParseTargetType, PostParseTargetType, IngestableAttType, Source, AGMarker, I>
{
    worker: W,
    phantom_prep: PhantomData<fn() -> PreParseTargetType>,
    phantom_post: PhantomData<fn() -> PostParseTargetType>,
    phantom_ing: PhantomData<fn() -> IngestableAttType>,
    phantom_m: PhantomData<fn() -> AGMarker>,
    phantom_s: PhantomData<fn() -> Source>,
    phantom_i: PhantomData<fn() -> I>,
}

trait AttGetter<T, Marker> {
    fn get_att(&self) -> Option<T>;
}

trait ObjAttGetter<T: Entity + MappableEntity> {
    fn get_obj_att(&self) -> Option<T::KeyType>;
}

trait GotAttParser<RawAtt, ParsedAtt, IngestableAttType, Source, Marker, I>
where
    Source: MainEntity,
    I: Iterator<Item = IngestableAttType>,
{
    fn parse(&self, att: Option<RawAtt>) -> Option<ParsedAtt>;
    fn ingest(&self, res: ParsedAtt, ind: NET<Source>);
    fn map_ind(&self, ind: Source::KeyType) -> Option<NET<Source>>;
    fn ingest_result<F>(self, f: F)
    where
        F: Fn(I);
}

trait StorableMarker<T>
where
    Self: Sized,
{
    type FinalType;
    fn update(&mut self, other: T);
    fn finalize(self) -> Self::FinalType;
}

impl Stowage {
    fn add_source_qs<SIF, YIF>(&mut self, sources_interface: &SIF, years_interface: &YIF)
    where
        YIF: EntityImmutableMapperBackend<Years>,
        SIF: EntityImmutableMapperBackend<Sources>,
    {
        let source_q_kv_iter = self
            .read_csv_objs::<SourceQ>(Sources::NAME, Qs::NAME)
            .filter_map(|yq| {
                let source_oa_id = yq.get_parsed_id();
                if let Some(sid) = sources_interface.get_via_immut(&source_oa_id) {
                    let year = years_interface.get_via_immut(&yq.publication_year).unwrap();
                    let key = (sid, year);
                    let v = yq.best_q;
                    Some((key, v))
                } else {
                    None
                }
            });

        self.add_iter_owned::<DiscoMapEntityBuilder<
            (<Sources as Entity>::T, <Years as Entity>::T),
            <Qs as Entity>::T,
        >, _, _>(source_q_kv_iter, Some("source-year-qs"));
    }

    fn add_work_atts(&self, winf: Arc<LoadedIdMap<ET<Works>>>) -> LoadedIdMap<ET<Works>> {
        WorkAttWriter::new(winf.clone())
            .para(self.read_csv_objs(Works::NAME, MAIN_NAME))
            .post(self);
        Arc::into_inner(winf).unwrap()
    }

    fn add_ship_relations(&self) -> LoadedIdMap<ET<Works>> {
        let winf: Arc<LoadedIdMap<ET<Works>>> = self
            .get_entity_interface::<Works, QuickestNumbered>()
            .into();
        {
            ShipRelWriter::new(winf.clone(), self)
                .para(iter_authorships(self).enumerate())
                .post(self);
        }
        Arc::into_inner(winf).unwrap()
    }

    fn property_writer<
        AttWorker,
        Builder,
        CsvObj,
        PreParseTargetType,
        PostParseTargetType,
        IngestableAtt,
        Source,
        AGMarker,
        I,
    >(
        &self,
        w: GenWorker<
            AttWorker,
            PreParseTargetType,
            PostParseTargetType,
            IngestableAtt,
            Source,
            AGMarker,
            I,
        >,
        name: &str,
        main: &str,
        sub: &str,
    ) where
        CsvObj: DeserializeOwned + Send + AttGetter<PreParseTargetType, AGMarker> + ParsedId,
        AttWorker: GotAttParser<
                PreParseTargetType,
                PostParseTargetType,
                IngestableAtt,
                Source,
                AGMarker,
                I,
            > + Sync,
        Source: MainEntity,
        PostParseTargetType: Sync,
        Builder: MetaIntegrator<IngestableAtt>,
        I: Iterator<Item = IngestableAtt>,
    {
        w.para(self.read_csv_objs::<CsvObj>(main, sub))
            .worker
            .ingest_result(|atts| {
                self.add_iter_owned::<Builder, _, IngestableAtt>(atts, Some(name));
            });
    }

    fn object_property<CsvObj, Source, Target, SIF, TIF>(
        &mut self,
        source_interface: &SIF,
        target_interface: &TIF,
        fatt_name: &str,
    ) -> io::Result<usize>
    where
        CsvObj: ObjAttGetter<Target> + ParsedId + DeserializeOwned + Send,
        Source: MainEntity,
        Target: Entity + MappableEntity,
        ET<Target>: UnsignedNumber,
        SIF: EntityImmutableMapperBackend<Source> + Sync,
        TIF: EntityImmutableMapperBackend<Target> + Sync,
    {
        let obj_worker = GenObjAttWorker::<'_, Source, Target, ET<Target>, SIF, TIF>::new(
            source_interface,
            target_interface,
        );
        let winit = GenWorker::new(obj_worker);
        self.property_writer::<_, FixAttBuilder, CsvObj, _, _, _, _, _, _>(
            winit,
            fatt_name,
            Source::NAME,
            MAIN_NAME,
        );
        self.declare_link::<Source, Target>(fatt_name);
        Ok(0)
    }

    fn multi_object_property<CsvObj, Source, Target, SIF, TIF>(
        &mut self,
        source_interface: &SIF,
        target_interface: &TIF,
        fatt_name: &str,
        sub: &str,
    ) -> io::Result<usize>
    where
        CsvObj: ObjAttGetter<Target> + ParsedId + DeserializeOwned + Send,
        Source: MainEntity,
        Target: MainEntity,
        SIF: EntityImmutableMapperBackend<Source> + Sync,
        TIF: EntityImmutableMapperBackend<Target> + Sync,
    {
        let obj_worker = GenObjAttWorker::<'_, Source, Target, Vec<NET<Target>>, SIF, TIF>::new(
            source_interface,
            target_interface,
        );
        let winit = GenWorker::new(obj_worker);
        self.property_writer::<_, VarAttBuilder, CsvObj, _, _, _, _, _, _>(
            winit,
            fatt_name,
            Source::NAME,
            sub,
        );
        self.declare_link::<Source, Target>(fatt_name);
        Ok(0)
    }

    fn add_empty_name_ext<T: Entity>(&mut self) {
        let name = get_name_ext_name::<T>();
        self.add_empty_something::<T, NameExtensionMarker>(&name);
    }

    fn add_empty_something<E: Entity, Marker>(&mut self, name: &str) {
        //TODO: this takes memory (and some space) for no fucking reason
        let iter = (0..E::N).map(|_| "".to_string());
        self.declare_iter::<VarAttBuilder, _, _, E, Marker>(iter, name)
    }
}

impl WorkAttWriter {
    fn new(winf: Arc<LoadedIdMap<ET<Works>>>) -> Self {
        Self {
            wdois: init_empty_slice::<Works, _>().into(),
            wyears: init_empty_slice::<Works, _>().into(),
            wnames: init_empty_slice::<Works, _>().into(),
            winf,
        }
    }

    fn post(self, stowage: &Stowage) {
        let wyname = "work-years";
        stowage.add_iter_owned::<FixAttBuilder, _, _>(iter_mboxa(self.wyears), Some(wyname));
        stowage.declare_link::<Works, Years>(wyname);
        stowage.declare_iter::<VarAttBuilder, _, _, Works, NameMarker>(
            iter_mboxa(self.wnames),
            &get_name_name::<Works>(),
        );
        stowage.declare_iter::<VarAttBuilder, _, _, Works, DoiMarker>(
            iter_mboxa(self.wdois),
            "work-dois",
        );
    }
}

impl ShipRelWriter {
    fn new(winf: Arc<LoadedIdMap<ET<Works>>>, stowage: &Stowage) -> Self {
        Self {
            ship2a: init_empty_slice::<Authorships, _>().into(),
            ship2is: init_empty_slice::<Authorships, _>().into(),
            w2ships: init_empty_slice::<Works, _>().into(),
            winf,
            ainf: stowage
                .get_entity_interface::<Authors, QuickestNumbered>()
                .into(),
            iinf: stowage
                .get_entity_interface::<Institutions, QuickestNumbered>()
                .into(),
        }
    }

    fn post(self, stowage: &Stowage) {
        let aa_name = "authorship-author";
        let ai_name = "authorship-institutions";
        let w2s_name = "work-authorships";
        stowage
            .add_iter_owned::<FixAttBuilder, _, _>(iter_mboxa(self.ship2a).tqdm(), Some(aa_name));
        stowage.add_iter_owned::<VarAttBuilder, _, _>(
            iter_mboxa(self.ship2is)
                .map(|v| v.into_boxed_slice())
                .tqdm(),
            Some(ai_name),
        );
        stowage.add_iter_owned::<VarAttBuilder, _, _>(
            iter_mboxa(self.w2ships)
                .map(|v| v.into_boxed_slice())
                .tqdm(),
            Some(w2s_name),
        );
        stowage.declare_link::<Authorships, Authors>(aa_name);
        stowage.declare_link::<Authorships, Institutions>(ai_name); //TODO: OneToMany
        stowage.declare_link::<Works, Authorships>(w2s_name); //TODO: OneToMany
    }
}

impl Worker<Work> for WorkAttWriter {
    fn proc(&self, input: Work) {
        let w_ind = match self.winf.0.get(&input.get_parsed_id()) {
            Some(wi) => wi.to_usize(),
            None => return,
        };
        if let Some(doi) = input.doi {
            self.wdois.lock().unwrap()[w_ind] = doi;
        }

        if let Some(name) = input.display_name {
            self.wnames.lock().unwrap()[w_ind] = name;
        }

        if let Some(year) = input.publication_year {
            self.wyears.lock().unwrap()[w_ind] = YearInterface::parse(year);
        }
    }
}

impl Worker<(usize, Authorship)> for ShipRelWriter {
    fn proc(&self, input: (usize, Authorship)) {
        let (i, ship) = input;
        let w_ind = match self.winf.0.get(&ship.get_parsed_id()) {
            Some(wi) => wi.to_usize(),
            None => return,
        };
        self.w2ships.lock().unwrap()[w_ind].push(ET::<Authorships>::from_usize(i));

        let aid_o = self.ainf.0.get(&oa_id_parse(&ship.author_id.unwrap()));
        if let Some(aid) = aid_o {
            self.ship2a.lock().unwrap()[i] = *aid;
        }
        for iid in ship
            .institutions
            .unwrap_or("".to_string())
            .trim()
            .split(";")
            .filter(|e| e.len() > 1)
        {
            if let Some(piid) = self.iinf.0.get(&oa_id_parse(iid)) {
                self.ship2is.lock().unwrap()[i].push(*piid);
            }
        }
    }
}

impl<'a> StrWriter<'a> {
    fn new(stowage: &'a Stowage) -> Self {
        Self {
            stowage,
            main: "",
            sub: "",
        }
    }

    fn set_path(&mut self, main: &'static str, sub: &'static str) -> &mut Self {
        self.main = main;
        self.sub = sub;
        self
    }

    fn write_name<CsvObj, Source>(&mut self) -> BeS<QuickestNumbered, Source>
    where
        CsvObj: DeserializeOwned + ParsedId + AttGetter<String, NameMarker> + Send,
        Source: MainEntity + NamespacedEntity,
    {
        let interface = self
            .stowage
            .get_entity_interface::<Source, QuickestNumbered>();
        let prop_name = &get_name_name::<Source>();
        self.write_meta::<Source, CsvObj, NameMarker>(&interface, prop_name);
        interface
    }

    fn write_name_ext<CsvObj, E>(&mut self, interface: &BeS<QuickestNumbered, E>)
    where
        CsvObj: DeserializeOwned + ParsedId + AttGetter<String, NameExtensionMarker> + Send,
        E: MainEntity + NamespacedEntity,
    {
        let prop_name = &get_name_ext_name::<E>();
        self.write_meta::<E, CsvObj, NameExtensionMarker>(interface, prop_name);
    }

    fn write_meta<E, CsvObj, Marker>(
        &mut self,
        interface: &BeS<QuickestNumbered, E>,
        prop_name: &str,
    ) where
        CsvObj: DeserializeOwned + ParsedId + AttGetter<String, Marker> + Send,
        E: MainEntity + NamespacedEntity,
    {
        if self.main == "" {
            self.set_path(E::NAME, MAIN_NAME);
        }
        let winit = GenWorker::new(DataAttWorker::<E, String, _>::new(interface));
        self.stowage
            .property_writer::<_, VarAttBuilder, CsvObj, _, _, _, _, Marker, _>(
                winit, prop_name, self.main, self.sub,
            );
        self.set_path("", "");
        self.stowage.declare::<E, Marker>(&prop_name);
    }
}

impl<'a, S, T, SIF> DataAttWorker<'a, S, T, SIF>
where
    S: MainEntity,
    T: InitEmpty + Sync + Send,
    SIF: EntityImmutableMapperBackend<S>,
{
    fn new(self_interface: &'a SIF) -> Self {
        let init_slice = init_empty_slice::<S, T>();
        Self {
            self_interface,
            attribute_arr: init_slice.into(),
            p: PhantomData,
        }
    }
}

impl<'a, S, T, TT, SIF, TIF> GenObjAttWorker<'a, S, T, TT, SIF, TIF>
where
    S: MainEntity,
    T: MappableEntity,
    TT: InitEmpty + Sync + Send,
    SIF: EntityImmutableMapperBackend<S>,
    TIF: EntityImmutableMapperBackend<T>,
{
    fn new(source_interface: &'a SIF, att_interface: &'a TIF) -> Self {
        Self {
            data_worker: DataAttWorker::<'a, S, TT, SIF>::new(source_interface),
            att_interface,
            p: PhantomData,
        }
    }
}

impl<W, T1, T2, T3, T4, T5, T6> GenWorker<W, T1, T2, T3, T4, T5, T6> {
    fn new(worker: W) -> Self {
        Self {
            worker,
            phantom_prep: PhantomData,
            phantom_post: PhantomData,
            phantom_ing: PhantomData,
            phantom_m: PhantomData,
            phantom_s: PhantomData,
            phantom_i: PhantomData,
        }
    }
}

impl<T, E> BoxRoller<T, E> {
    fn new(arr: Box<[T]>) -> Self {
        Self {
            arr: arr.into_vec().into_iter(),
            phantom: PhantomData,
        }
    }
}

impl<T> StorableMarker<Self> for T {
    type FinalType = Self;
    default fn update(&mut self, other: Self) {
        *self = other;
    }
    default fn finalize(self) -> Self::FinalType {
        self
    }
}

impl<T> StorableMarker<T> for Vec<T> {
    type FinalType = Box<[T]>;
    fn update(&mut self, other: T) {
        self.push(other);
    }
    fn finalize(self) -> Self::FinalType {
        self.into_boxed_slice()
    }
}

impl ParsedId for SourceQ {
    fn get_parsed_id(&self) -> BigId {
        self.id
    }
}

impl<E, CsvObj> AttGetter<E::KeyType, E> for CsvObj
where
    E: Entity + MappableEntity,
    CsvObj: ObjAttGetter<E>,
{
    default fn get_att(&self) -> Option<E::KeyType> {
        self.get_obj_att()
    }
}

impl AttGetter<String, NameExtensionMarker> for Source {
    fn get_att(&self) -> Option<String> {
        post_ext_name(&self.alternate_titles)
    }
}

impl AttGetter<String, NameExtensionMarker> for Institution {
    fn get_att(&self) -> Option<String> {
        post_ext_name(&self.display_name_acronyms)
    }
}

impl AttGetter<String, DoiMarker> for Work {
    fn get_att(&self) -> Option<String> {
        const DL: usize = 16;
        if let Some(doi) = &self.doi {
            if doi.len() > DL {
                return Some(doi[DL..].to_string());
            }
        }
        None
    }
}

impl<T> AttGetter<String, NameMarker> for T
where
    T: Named,
{
    fn get_att(&self) -> Option<String> {
        Some(self.get_name())
    }
}

impl Named for Source {
    fn get_name(&self) -> String {
        let dn = self.display_name.clone();
        let parts: Vec<&str> = dn.split("/").collect();
        if parts.len() == 2 {
            let lmin = min(parts[0].len(), parts[1].len());
            if lmin >= MIN_LEN {
                let edist = levenshtein(parts[0], parts[1]);
                let rate = 1.0 - f64::from(edist as u32) / f64::from(lmin as u32);
                if rate >= MIN_RATE {
                    return parts[0].to_string();
                }
            }
        }
        dn
    }
}

impl ObjAttGetter<Fields> for SubField {
    fn get_obj_att(&self) -> Option<<Fields as MappableEntity>::KeyType> {
        Some(field_id_parse(&self.field))
    }
}

impl ObjAttGetter<Subfields> for Topic {
    fn get_obj_att(&self) -> Option<<Subfields as MappableEntity>::KeyType> {
        Some(field_id_parse(&self.subfield))
    }
}

impl ObjAttGetter<Topics> for WorkTopic {
    fn get_obj_att(&self) -> Option<<Topics as MappableEntity>::KeyType> {
        if self.score.unwrap_or(0.0) > MIN_TOPIC_SCORE {
            return Some(oa_id_parse(self.topic_id.as_ref().unwrap()));
        }
        None
    }
}

impl ObjAttGetter<Countries> for Institution {
    fn get_obj_att(&self) -> Option<<Countries as MappableEntity>::KeyType> {
        if let Some(cc_id) = &self.country_code {
            return Some(short_string_to_u64(&cc_id));
        }
        return None;
    }
}

impl ObjAttGetter<Works> for ReferencedWork {
    fn get_obj_att(&self) -> Option<<Works as MappableEntity>::KeyType> {
        Some(oa_id_parse(&self.referenced_work_id))
    }
}

impl ObjAttGetter<AreaFields> for SourceArea {
    fn get_obj_att(&self) -> Option<<AreaFields as MappableEntity>::KeyType> {
        Some(self.raw_area_id())
    }
}

impl ObjAttGetter<Sources> for Location {
    fn get_obj_att(&self) -> Option<<Works as MappableEntity>::KeyType> {
        if let Some(sid) = &self.source_id {
            Some(oa_id_parse(&sid))
        } else {
            None
        }
    }
}

impl<T, E> Iterator for BoxRoller<T, E>
where
    T: StorableMarker<E>,
{
    type Item = T::FinalType;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(v) = self.arr.next() {
            Some(v.finalize())
        } else {
            None
        }
    }
}

impl<'a, Source, TargetType, Marker, SIF>
    GotAttParser<TargetType, TargetType, TargetType, Source, Marker, std::vec::IntoIter<TargetType>>
    for DataAttWorker<'a, Source, TargetType, SIF>
where
    Source: MainEntity,
    TargetType: Sync + Send,
    SIF: EntityImmutableMapperBackend<Source>,
{
    fn parse(&self, att: Option<TargetType>) -> Option<TargetType> {
        att
    }

    fn ingest(&self, res: TargetType, ind: NET<Source>) {
        self.attribute_arr.lock().unwrap()[ind.to_usize()] = res;
    }

    fn map_ind(&self, ind: Source::KeyType) -> Option<NET<Source>> {
        self.self_interface.get_via_immut(&ind)
    }
    fn ingest_result<F>(self, f: F)
    where
        F: Fn(std::vec::IntoIter<TargetType>),
    {
        f(self
            .attribute_arr
            .into_inner()
            .unwrap()
            .into_vec()
            .into_iter())
    }
}

impl<Source, Target, Marker, StoredOfTarget, SIF, TIF>
    GotAttParser<
        Target::KeyType,
        Target::T,
        StoredOfTarget::FinalType,
        Source,
        Marker,
        BoxRoller<StoredOfTarget, Target::T>,
    > for GenObjAttWorker<'_, Source, Target, StoredOfTarget, SIF, TIF>
where
    Source: MainEntity,
    Target: Entity + MappableEntity,
    <Source as Entity>::T: UnsignedNumber,
    TIF: EntityImmutableMapperBackend<Target>,
    SIF: EntityImmutableMapperBackend<Source>,
    StoredOfTarget: StorableMarker<Target::T> + Send + Sync,
{
    fn parse(&self, att_o: Option<Target::KeyType>) -> Option<Target::T> {
        if let Some(att) = att_o {
            self.att_interface.get_via_immut(&att)
        } else {
            None
        }
    }

    fn ingest(&self, res: Target::T, ind: NET<Source>) {
        StoredOfTarget::update(
            &mut self.data_worker.attribute_arr.lock().unwrap()[ind.to_usize()],
            res,
        )
    }

    fn map_ind(&self, ind: Source::KeyType) -> Option<NET<Source>> {
        self.data_worker.self_interface.get_via_immut(&ind)
    }

    fn ingest_result<F>(self, f: F)
    where
        F: Fn(BoxRoller<StoredOfTarget, Target::T>),
    {
        let arr = self.data_worker.attribute_arr.into_inner().unwrap();
        f(BoxRoller::new(arr))
    }
}

impl<
        CsvObj,
        W,
        PreParseTargetType,
        PostParseTargetType,
        IngestableAttType,
        Source,
        AGMarker,
        I,
    > Worker<CsvObj>
    for GenWorker<
        W,
        PreParseTargetType,
        PostParseTargetType,
        IngestableAttType,
        Source,
        AGMarker,
        I,
    >
where
    W: GotAttParser<
            PreParseTargetType,
            PostParseTargetType,
            IngestableAttType,
            Source,
            AGMarker,
            I,
        > + Sync,
    CsvObj: ParsedId + Send + AttGetter<PreParseTargetType, AGMarker>,
    Source: MainEntity,
    PostParseTargetType: Sync,
    I: Iterator<Item = IngestableAttType>,
{
    fn proc(&self, input: CsvObj) {
        let in_id = input.get_parsed_id();
        if let (Some(att), Some(ind)) = (
            self.worker.parse(input.get_att()),
            self.worker.map_ind(in_id),
        ) {
            self.worker.ingest(att, ind);
        }
    }
}

pub fn main(mut stowage: Stowage) -> io::Result<()> {
    let works_interface = {
        let winf = stowage.add_ship_relations();
        stowage.add_work_atts(winf.into())
    };

    let mut str_writer = StrWriter::new(&stowage);
    let fields_interface = str_writer.write_name::<FieldLike, Fields>();
    let countries_interface = str_writer
        .set_path(Institutions::NAME, institutions::atts::geo)
        .write_name::<Geo, Countries>();
    let subfields_interface = str_writer.write_name::<FieldLike, Subfields>();
    let insts_interface = write_inst_names(&stowage);
    let sources_interface = str_writer.write_name::<Source, Sources>();
    str_writer.write_name::<NamedEntity, Authors>();
    let topics_interface = str_writer.write_name::<NamedEntity, Topics>();
    str_writer.write_name_ext::<Institution, Institutions>(&insts_interface);
    str_writer.write_name_ext::<Source, Sources>(&sources_interface);

    stowage.add_empty_name_ext::<Authors>();
    stowage.add_empty_name_ext::<Countries>();
    stowage.add_empty_name_ext::<Subfields>();

    stowage.add_source_qs(&sources_interface, &YearInterface {});
    stowage.object_property::<Institution, Institutions, _, _, _>(
        &insts_interface,
        &countries_interface,
        "inst-countries",
    )?;
    stowage.object_property::<SubField, Subfields, _, _, _>(
        &subfields_interface,
        &fields_interface,
        "subfield-ancestors",
    )?;
    stowage.object_property::<Topic, Topics, _, _, _>(
        &topics_interface,
        &subfields_interface,
        "topic-subfields",
    )?;
    let area_fields_interface = stowage.get_entity_interface::<AreaFields, QuickestNumbered>();
    stowage.multi_object_property::<SourceArea, Sources, _, _, _>(
        &sources_interface,
        &area_fields_interface,
        "source-area-fields",
        AreaFields::NAME,
    )?;
    stowage.multi_object_property::<ReferencedWork, Works, _, _, _>(
        &works_interface,
        &works_interface,
        "work-references",
        works::atts::referenced_works,
    )?;
    stowage.multi_object_property::<Location, Works, _, _, _>(
        &works_interface,
        &sources_interface,
        "work-sources",
        works::atts::locations,
    )?;
    stowage.multi_object_property::<WorkTopic, Works, _, _, _>(
        &works_interface,
        &topics_interface,
        "work-topics",
        works::atts::topics,
    )?;

    stowage.write_code()?;
    Ok(())
}

fn write_inst_names(stowage: &Stowage) -> LoadedIdMap<NET<Institutions>> {
    type E = Institutions;
    let interface = stowage.get_entity_interface::<E, QuickestNumbered>();
    let mut cities = init_empty_slice::<E, String>();
    let mut ccs = init_empty_slice::<E, String>();
    stowage
        .read_csv_objs::<Geo>(E::NAME, institutions::atts::geo)
        .for_each(|e| {
            let iu = interface
                .0
                .get(&oa_id_parse(&e.parent_id.unwrap()))
                .unwrap_or(&0)
                .to_usize();
            if let Some(city) = e.city {
                cities[iu] = city.clone();
            }
            if let Some(cc) = e.country_code {
                ccs[iu] = cc.clone();
            }
        });

    let winit: GenWorker<_, _, _, _, _, NameMarker, _> =
        GenWorker::new(DataAttWorker::<E, String, _>::new(&interface));
    let raw_names = winit
        .para(stowage.read_csv_objs::<NamedEntity>(E::NAME, MAIN_NAME))
        .worker
        .attribute_arr
        .into_inner()
        .unwrap();
    let countried_names: Vec<String> = raw_names
        .iter()
        .enumerate()
        .map(|(i, e)| {
            for (j, rn) in raw_names.iter().enumerate() {
                if (i != j) & (rn == e) & (ccs[i].len() > 0) {
                    return format!("{e} ({})", ccs[i]);
                }
            }
            e.clone()
        })
        .collect();
    let citied_names: Vec<String> = countried_names
        .iter()
        .enumerate()
        .map(|(i, e)| {
            for (j, cn) in countried_names.iter().enumerate() {
                if (i != j) & (cn == e) & (cities[i].len() > 0) {
                    return format!("{} ({})", raw_names[i], cities[i]);
                }
            }
            e.clone()
        })
        .collect();
    let prop_name = &get_name_name::<E>();
    stowage.declare_iter::<VarAttBuilder, _, _, E, NameMarker>(citied_names.into_iter(), prop_name);

    interface
}

fn get_name_name<E: Entity>() -> String {
    format!("{}-names", E::NAME)
}

fn get_name_ext_name<E: Entity>() -> String {
    format!("{}-name-exts", E::NAME)
}

fn iter_mboxa<T>(ba: Mutex<Box<[T]>>) -> std::vec::IntoIter<T> {
    ba.into_inner().unwrap().into_vec().into_iter()
}

fn post_ext_name(in_str: &Option<String>) -> Option<String> {
    Some(read_post_str_arr(in_str).join(" "))
}
