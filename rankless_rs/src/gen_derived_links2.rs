use dmove::{Entity, Link, MappableEntity, NamespacedEntity, VariableSizeAttribute};

pub struct InstitutionWorks {}

pub struct AuthorWorks {}

impl Entity for AuthorWorks {
    type T = Box<[u16]>;
    const N: usize = 254;
    const NAME: &'static str = "author-works";
}

impl MappableEntity<Self> for AuthorWorks {
    type KeyType = usize;
}

impl VariableSizeAttribute for AuthorWorks {
    type SizeType = u8;
}

impl NamespacedEntity for AuthorWorks {
    const NS: &str = "derived-links2";
}

impl Link for AuthorWorks {
    type Source = crate::gen_entity_mapping::Works;
    type Target = crate::gen_entity_mapping::Authors;
}

impl Entity for InstitutionWorks {
    type T = Box<[u16]>;
    const N: usize = 306;
    const NAME: &'static str = "institution-works";
}

impl MappableEntity<Self> for InstitutionWorks {
    type KeyType = usize;
}

impl VariableSizeAttribute for InstitutionWorks {
    type SizeType = u8;
}

impl NamespacedEntity for InstitutionWorks {
    const NS: &str = "derived-links2";
}

impl Link for InstitutionWorks {
    type Source = crate::gen_entity_mapping::Works;
    type Target = crate::gen_entity_mapping::Institutions;
}

