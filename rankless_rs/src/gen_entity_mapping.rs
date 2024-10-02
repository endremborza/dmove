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

impl Entity for AreaFields { type T = u8; const N : usize = 19 ; const NAME : & 'static str = "area-fields" ; }

impl MappableEntity<Self> for AreaFields { type KeyType = usize; }

impl MappableEntity<u64> for AreaFields { type KeyType = u64; }

impl NamespacedEntity for AreaFields { const NS : & str = "entity_mapping" ; }

impl Entity for Authorships { type T = u32; const N : usize = 73953 ; const NAME : & 'static str = "authorships" ; }

impl MappableEntity<Self> for Authorships { type KeyType = usize; }

impl Entity for Qs { type T = u8; const N : usize = 5 ; const NAME : & 'static str = "qs" ; }

impl MappableEntity<Self> for Qs { type KeyType = usize; }

impl Entity for Countries { type T = u8; const N : usize = 91 ; const NAME : & 'static str = "countries" ; }

impl MappableEntity<Self> for Countries { type KeyType = usize; }

impl MappableEntity<u64> for Countries { type KeyType = u64; }

impl NamespacedEntity for Countries { const NS : & str = "entity_mapping" ; }

impl Entity for Fields { type T = u8; const N : usize = 27 ; const NAME : & 'static str = "fields" ; }

impl MappableEntity<Self> for Fields { type KeyType = usize; }

impl MappableEntity<u64> for Fields { type KeyType = u64; }

impl NamespacedEntity for Fields { const NS : & str = "entity_mapping" ; }

impl Entity for Subfields { type T = u8; const N : usize = 253 ; const NAME : & 'static str = "subfields" ; }

impl MappableEntity<Self> for Subfields { type KeyType = usize; }

impl MappableEntity<u64> for Subfields { type KeyType = u64; }

impl NamespacedEntity for Subfields { const NS : & str = "entity_mapping" ; }

impl Entity for Works { type T = u16; const N : usize = 10815 ; const NAME : & 'static str = "works" ; }

impl MappableEntity<Self> for Works { type KeyType = usize; }

impl MappableEntity<u64> for Works { type KeyType = u64; }

impl NamespacedEntity for Works { const NS : & str = "entity_mapping" ; }

impl Entity for Institutions { type T = u16; const N : usize = 305 ; const NAME : & 'static str = "institutions" ; }

impl MappableEntity<Self> for Institutions { type KeyType = usize; }

impl MappableEntity<u64> for Institutions { type KeyType = u64; }

impl NamespacedEntity for Institutions { const NS : & str = "entity_mapping" ; }

impl Entity for Sources { type T = u8; const N : usize = 177 ; const NAME : & 'static str = "sources" ; }

impl MappableEntity<Self> for Sources { type KeyType = usize; }

impl MappableEntity<u64> for Sources { type KeyType = u64; }

impl NamespacedEntity for Sources { const NS : & str = "entity_mapping" ; }

impl Entity for Topics { type T = u16; const N : usize = 4517 ; const NAME : & 'static str = "topics" ; }

impl MappableEntity<Self> for Topics { type KeyType = usize; }

impl MappableEntity<u64> for Topics { type KeyType = u64; }

impl NamespacedEntity for Topics { const NS : & str = "entity_mapping" ; }

impl Entity for Authors { type T = u8; const N : usize = 253 ; const NAME : & 'static str = "authors" ; }

impl MappableEntity<Self> for Authors { type KeyType = usize; }

impl MappableEntity<u64> for Authors { type KeyType = u64; }

impl NamespacedEntity for Authors { const NS : & str = "entity_mapping" ; }