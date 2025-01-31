use dmove::{Entity, MappableEntity, NamespacedEntity};

pub struct Fields { }

pub struct Authors { }

pub struct Works { }

pub struct Qs { }

pub struct Authorships { }

pub struct Countries { }

pub struct Sources { }

pub struct AreaFields { }

pub struct Institutions { }

pub struct Subfields { }

pub struct Topics { }

impl Entity for AreaFields { type T = u8; const N: usize =  2; const NAME: & str =  "area-fields"; }

impl MappableEntity for AreaFields { type KeyType = u64; }

impl NamespacedEntity for AreaFields { const NS: & str =  "a1_entity_mapping"; }

impl Entity for Countries { type T = u8; const N: usize =  229; const NAME: & str =  "countries"; }

impl MappableEntity for Countries { type KeyType = u64; }

impl NamespacedEntity for Countries { const NS: & str =  "a1_entity_mapping"; }

impl Entity for Authorships { type T = u32; const N: usize =  192512777; const NAME: & str =  "authorships"; }

impl Entity for Qs { type T = u8; const N: usize =  5; const NAME: & str =  "qs"; }

impl Entity for Fields { type T = u8; const N: usize =  27; const NAME: & str =  "fields"; }

impl MappableEntity for Fields { type KeyType = u64; }

impl NamespacedEntity for Fields { const NS: & str =  "a1_entity_mapping"; }

impl Entity for Subfields { type T = u8; const N: usize =  253; const NAME: & str =  "subfields"; }

impl MappableEntity for Subfields { type KeyType = u64; }

impl NamespacedEntity for Subfields { const NS: & str =  "a1_entity_mapping"; }

impl Entity for Works { type T = u32; const N: usize =  44973352; const NAME: & str =  "works"; }

impl MappableEntity for Works { type KeyType = u64; }

impl NamespacedEntity for Works { const NS: & str =  "a1_entity_mapping"; }

impl Entity for Institutions { type T = u16; const N: usize =  17782; const NAME: & str =  "institutions"; }

impl MappableEntity for Institutions { type KeyType = u64; }

impl NamespacedEntity for Institutions { const NS: & str =  "a1_entity_mapping"; }

impl Entity for Sources { type T = u16; const N: usize =  28670; const NAME: & str =  "sources"; }

impl MappableEntity for Sources { type KeyType = u64; }

impl NamespacedEntity for Sources { const NS: & str =  "a1_entity_mapping"; }

impl Entity for Topics { type T = u16; const N: usize =  4517; const NAME: & str =  "topics"; }

impl MappableEntity for Topics { type KeyType = u64; }

impl NamespacedEntity for Topics { const NS: & str =  "a1_entity_mapping"; }

impl Entity for Authors { type T = u32; const N: usize =  2294554; const NAME: & str =  "authors"; }

impl MappableEntity for Authors { type KeyType = u64; }

impl NamespacedEntity for Authors { const NS: & str =  "a1_entity_mapping"; }