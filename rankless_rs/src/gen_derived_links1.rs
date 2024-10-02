use dmove::{Entity, Link, MappableEntity, NamespacedEntity, VariableSizeAttribute};

pub struct WorkAuthors {}

pub struct WorkInstitutions {}

pub struct WorkSubfields {}

pub struct WorksCiting {}

impl Entity for WorksCiting {
    type T = Box<[u16]>;
    const N: usize = 10816;
    const NAME: &'static str = "works-citing";
}

impl MappableEntity<Self> for WorksCiting {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorksCiting {
    type SizeType = u8;
}

impl NamespacedEntity for WorksCiting {
    const NS: &str = "derived-links1";
}

impl Link for WorksCiting {
    type Source = crate::gen_entity_mapping::Works;
    type Target = crate::gen_entity_mapping::Works;
}

impl Entity for WorkSubfields {
    type T = Box<[u8]>;
    const N: usize = 10816;
    const NAME: &'static str = "work-subfields";
}

impl MappableEntity<Self> for WorkSubfields {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorkSubfields {
    type SizeType = u8;
}

impl NamespacedEntity for WorkSubfields {
    const NS: &str = "derived-links1";
}

impl Link for WorkSubfields {
    type Source = crate::gen_entity_mapping::Works;
    type Target = crate::gen_entity_mapping::Subfields;
}

impl Entity for WorkAuthors {
    type T = Box<[u8]>;
    const N: usize = 10816;
    const NAME: &'static str = "work-authors";
}

impl MappableEntity<Self> for WorkAuthors {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorkAuthors {
    type SizeType = u8;
}

impl NamespacedEntity for WorkAuthors {
    const NS: &str = "derived-links1";
}

impl Link for WorkAuthors {
    type Source = crate::gen_entity_mapping::Works;
    type Target = crate::gen_entity_mapping::Authors;
}

impl Entity for WorkInstitutions {
    type T = Box<[u16]>;
    const N: usize = 10816;
    const NAME: &'static str = "work-institutions";
}

impl MappableEntity<Self> for WorkInstitutions {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorkInstitutions {
    type SizeType = u8;
}

impl NamespacedEntity for WorkInstitutions {
    const NS: &str = "derived-links1";
}

