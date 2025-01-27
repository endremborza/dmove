use crate::{
    common::{
        field_id_parse, init_empty_slice, oa_id_parse, short_string_to_u64, BackendSelector,
        NameExtensionMarker, NameMarker, ParsedId, Quickest, SemanticIdMarker, Stowage, MAIN_NAME,
    },
    csv_writers::{institutions, works},
    gen::a1_entity_mapping::{
        AreaFields, Authors, Authorships, Countries, Fields, Institutions, Qs, Sources, Subfields,
        Topics, Works,
    },
    oa_structs::{
        post::{Institution, Location, Source, SubField, Topic},
        FieldLike, Geo, Named, NamedEntity, ReferencedWork, Work, WorkTopic,
    },
    steps::a1_entity_mapping::{iter_authorships, SourceArea, YearInterface, Years},
};
use dmove::{
    para::Worker, BigId, ByteFixArrayInterface, DiscoMapEntityBuilder, Entity,
    EntityImmutableMapperBackend, FixAttBuilder, InitEmpty, MappableEntity, MetaIntegrator,
    NamespacedEntity, UnsignedNumber, VarAttBuilder,
};
use serde::{de::DeserializeOwned, Deserialize};
use std::{io, marker::PhantomData, sync::Mutex, usize};

#[derive(Deserialize)]
struct SourceQ {
    publication_year: u16,
    id: BigId,
    best_q: u8,
}

struct BoxRoller<T, E> {
    arr: std::vec::IntoIter<T>,
    phantom: PhantomData<fn() -> E>,
}

struct StrWriter<'a> {
    stowage: &'a mut Stowage,
    main: &'static str,
    sub: &'static str,
}

struct GenObjAttWorker<'a, Source, Target, StoredOfTarget, SourceIF, TargetIF>
where
    Source: MappableEntity<KeyType = BigId>,
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
    Source: Entity + MappableEntity,
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
    Source: Entity + MappableEntity,
    I: Iterator<Item = IngestableAttType>,
{
    fn parse(&self, att: Option<RawAtt>) -> Option<ParsedAtt>;
    fn ingest(&self, res: ParsedAtt, ind: Source::T);
    fn map_ind(&self, ind: Source::KeyType) -> Option<Source::T>;
    fn ingest_result<F>(self, f: F)
    where
        F: FnMut(I);
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

    fn add_ship_relations<WIF, AIF, IIF>(
        &mut self,
        works_interface: &WIF,
        authors_interface: &AIF,
        institutions_interface: &IIF,
    ) where
        WIF: EntityImmutableMapperBackend<Works>,
        AIF: EntityImmutableMapperBackend<Authors>,
        IIF: EntityImmutableMapperBackend<Institutions>,
    {
        type AuthorshipId = <Authorships as Entity>::T;
        let mut ship2a: Vec<<Authors as Entity>::T> = vec![0; Authorships::N];
        let mut ship2is: Vec<Vec<<Institutions as Entity>::T>> = Vec::new();
        let mut w2ships = init_empty_slice::<Works, Vec<AuthorshipId>>();
        iter_authorships(&self).enumerate().for_each(|(i, ship)| {
            let w_ind = match works_interface.get_via_immut(&ship.get_parsed_id()) {
                Some(i) => i.to_usize(),
                None => return,
            };
            w2ships[w_ind].push(AuthorshipId::from_usize(i));

            let aid_o = authors_interface.get_via_immut(&oa_id_parse(&ship.author_id.unwrap()));
            if let Some(aid) = aid_o {
                ship2a[i] = aid;
            }
            let mut inst_v = Vec::new();
            for iid in ship
                .institutions
                .unwrap_or("".to_string())
                .trim()
                .split(";")
                .filter(|e| e.len() > 1)
            {
                let iid_o = institutions_interface.get_via_immut(&oa_id_parse(iid));
                if let Some(piid) = iid_o {
                    inst_v.push(piid);
                }
            }
            ship2is.push(inst_v);
        });
        let aa_name = "authorship-author";
        let ai_name = "authorship-institutions";
        let w2s_name = "work-authorships";
        self.add_iter_owned::<FixAttBuilder, _, _>(ship2a.into_iter(), Some(aa_name));
        self.add_iter_owned::<VarAttBuilder, _, _>(
            ship2is.into_iter().map(|v| v.into_boxed_slice()),
            Some(ai_name),
        );
        self.add_iter_owned::<VarAttBuilder, _, _>(
            w2ships.into_vec().into_iter().map(|v| v.into_boxed_slice()),
            Some(w2s_name),
        );
        self.declare_link::<Authorships, Authors>(aa_name);
        self.declare_link::<Authorships, Institutions>(ai_name); //TODO: OneToMany
        self.declare_link::<Works, Authorships>(w2s_name); //TODO: OneToMany
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
        &mut self,
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
        Source: Entity + MappableEntity<KeyType = BigId>,
        PostParseTargetType: Sync,
        Builder: MetaIntegrator<IngestableAtt>,
        <Source as Entity>::T: UnsignedNumber,
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
        Source: MappableEntity<KeyType = BigId>,
        Target: MappableEntity,
        <Target as Entity>::T: ByteFixArrayInterface + InitEmpty + Sync + Send,
        <Source as Entity>::T: UnsignedNumber,
        SIF: EntityImmutableMapperBackend<Source> + Sync,
        TIF: EntityImmutableMapperBackend<Target> + Sync,
    {
        let obj_worker = GenObjAttWorker::<'_, Source, Target, Target::T, SIF, TIF>::new(
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
        Source: Entity + MappableEntity<KeyType = BigId>,
        Target: Entity + MappableEntity,
        <Source as Entity>::T: UnsignedNumber,
        <Target as Entity>::T: Sync + Send + ByteFixArrayInterface,
        SIF: EntityImmutableMapperBackend<Source> + Sync,
        TIF: EntityImmutableMapperBackend<Target> + Sync,
    {
        let obj_worker = GenObjAttWorker::<'_, Source, Target, Vec<Target::T>, SIF, TIF>::new(
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

    fn add_empty_something<T: Entity, Marker>(&mut self, name: &str) {
        //TODO: this takes memory (and some space) for no fucking reason
        self.add_iter_owned::<VarAttBuilder, _, _>((0..T::N).map(|_| "".to_string()), Some(&name));
        self.declare::<T, Marker>(&name);
    }

    fn write_q_names(&mut self) -> io::Result<usize> {
        //TODO: this could/should be a compiled _get_ like with years
        let mut q_names: Vec<String> = vec!["Uncategorized".to_owned()];
        q_names.extend((1..5).map(|i| format!("Q{}", i)));
        let q_name = get_name_name::<Qs>();
        self.add_iter_owned::<VarAttBuilder, _, _>(q_names.into_iter(), Some(&q_name));
        self.declare::<Qs, NameMarker>(&q_name);
        Ok(0)
    }

    fn write_semantic_id<E>(&mut self)
    where
        E: NamespacedEntity + MappableEntity<KeyType = BigId>,
        <E as Entity>::T: UnsignedNumber,
    {
        let mut ids = init_empty_slice::<E, String>();
        let interface = self.get_entity_interface::<E, Quickest>();
        for sem in self.read_sem_ids::<E>() {
            if let Some(eid) = interface.0.get(&sem.id) {
                ids[eid.to_usize()] = sem.semantic_id;
            }
        }
        let name = format!("{}-semantic-ids", E::NAME);
        self.add_iter_owned::<VarAttBuilder, _, _>(ids.iter().map(|e| e.to_owned()), Some(&name));
        self.declare::<E, SemanticIdMarker>(&name);
    }
}

impl<'a> StrWriter<'a> {
    fn new(stowage: &'a mut Stowage) -> Self {
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

    fn write_name<CsvObj, Source>(&mut self) -> <Quickest as BackendSelector<Source>>::BE
    where
        CsvObj: DeserializeOwned + ParsedId + AttGetter<String, NameMarker> + Send,
        Source: MappableEntity<KeyType = BigId> + NamespacedEntity,
        <Source as Entity>::T: UnsignedNumber + Sync,
    {
        let interface = self.stowage.get_entity_interface::<Source, Quickest>();
        let prop_name = &get_name_name::<Source>();
        self.write_meta::<Source, CsvObj, NameMarker>(&interface, prop_name);
        interface
    }

    fn write_name_ext<CsvObj, E>(&mut self, interface: &<Quickest as BackendSelector<E>>::BE)
    where
        CsvObj: DeserializeOwned + ParsedId + AttGetter<String, NameExtensionMarker> + Send,
        E: Entity + MappableEntity<KeyType = BigId> + NamespacedEntity,
        <E as Entity>::T: UnsignedNumber + Sync,
    {
        let prop_name = &get_name_ext_name::<E>();
        self.write_meta::<E, CsvObj, NameExtensionMarker>(interface, prop_name);
    }

    fn write_meta<E, CsvObj, Marker>(
        &mut self,
        interface: &<Quickest as BackendSelector<E>>::BE,
        prop_name: &str,
    ) where
        CsvObj: DeserializeOwned + ParsedId + AttGetter<String, Marker> + Send,
        E: Entity + MappableEntity<KeyType = BigId> + NamespacedEntity,
        <E as Entity>::T: UnsignedNumber + Sync,
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
    S: Entity + MappableEntity,
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
    S: MappableEntity<KeyType = BigId>,
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

impl<T> AttGetter<String, NameMarker> for T
where
    T: Named,
{
    fn get_att(&self) -> Option<String> {
        Some(self.get_name())
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
        if self.score.unwrap_or(0.0) > 0.7 {
            //TODO: specific, hard-coded
            return Some(oa_id_parse(self.topic_id.as_ref().unwrap()));
        }
        None
    }
}

impl ObjAttGetter<Years> for Work {
    fn get_obj_att(&self) -> Option<<Years as MappableEntity>::KeyType> {
        self.publication_year
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
    Source: Entity + MappableEntity,
    <Source as Entity>::T: UnsignedNumber,
    TargetType: Sync + Send,
    SIF: EntityImmutableMapperBackend<Source>,
{
    fn parse(&self, att: Option<TargetType>) -> Option<TargetType> {
        att
    }

    fn ingest(&self, res: TargetType, ind: Source::T) {
        self.attribute_arr.lock().unwrap()[ind.to_usize()] = res;
    }

    fn map_ind(&self, ind: Source::KeyType) -> Option<Source::T> {
        self.self_interface.get_via_immut(&ind)
    }
    fn ingest_result<F>(self, mut f: F)
    where
        F: FnMut(std::vec::IntoIter<TargetType>),
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
    Source: Entity + MappableEntity<KeyType = BigId>,
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

    fn ingest(&self, res: Target::T, ind: Source::T) {
        StoredOfTarget::update(
            &mut self.data_worker.attribute_arr.lock().unwrap()[ind.to_usize()],
            res,
        )
    }

    fn map_ind(&self, ind: Source::KeyType) -> Option<Source::T> {
        self.data_worker.self_interface.get_via_immut(&ind)
    }

    fn ingest_result<F>(self, mut f: F)
    where
        F: FnMut(BoxRoller<StoredOfTarget, Target::T>),
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
    Source: Entity + MappableEntity<KeyType = BigId>,
    PostParseTargetType: Sync,
    <Source as Entity>::T: UnsignedNumber,
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
    stowage.write_q_names()?;
    let mut str_writer = StrWriter::new(&mut stowage);

    let fields_interface = str_writer.write_name::<FieldLike, Fields>();
    let countries_interface = str_writer
        .set_path(Institutions::NAME, institutions::atts::geo)
        .write_name::<Geo, Countries>();
    let subfields_interface = str_writer.write_name::<FieldLike, Subfields>();
    let insts_interface = str_writer.write_name::<NamedEntity, Institutions>();
    let sources_interface = str_writer.write_name::<NamedEntity, Sources>();
    let authors_interface = str_writer.write_name::<NamedEntity, Authors>();
    let topics_interface = str_writer.write_name::<NamedEntity, Topics>();
    str_writer.write_name::<NamedEntity, Works>();

    str_writer.write_name_ext::<Institution, Institutions>(&insts_interface);
    str_writer.write_name_ext::<Source, Sources>(&sources_interface);

    stowage.add_empty_name_ext::<Authors>();
    stowage.add_empty_name_ext::<Countries>();
    stowage.add_empty_name_ext::<Subfields>();

    stowage.write_semantic_id::<Institutions>();
    stowage.write_semantic_id::<Sources>();
    stowage.write_semantic_id::<Authors>();
    stowage.write_semantic_id::<Countries>();
    stowage.write_semantic_id::<Subfields>();

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
    // let topics_interface = stowage.get_entity_interface::<Topics, Quickest>();
    stowage.object_property::<Topic, Topics, _, _, _>(
        &topics_interface,
        &subfields_interface,
        "topic-subfields",
    )?;
    let works_interface = stowage.get_entity_interface::<Works, Quickest>();
    let year_interface = YearInterface {};
    stowage.object_property::<Work, Works, _, _, _>(
        &works_interface,
        &year_interface,
        "work-years",
    )?;
    let area_fields_interface = stowage.get_entity_interface::<AreaFields, Quickest>();
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

    stowage.add_ship_relations(&works_interface, &authors_interface, &insts_interface);
    stowage.add_source_qs(&sources_interface, &year_interface);
    stowage.write_code()?;
    Ok(())
}

fn get_name_name<E: Entity>() -> String {
    format!("{}-names", E::NAME)
}

fn get_name_ext_name<E: Entity>() -> String {
    format!("{}-name-exts", E::NAME)
}

fn post_ext_name(in_str: &Option<String>) -> Option<String> {
    Some(
        serde_json::from_str::<Vec<String>>(&in_str.clone().unwrap_or("[]".to_string()))
            .unwrap()
            .join(" "),
    )
}
