use dmove::{Entity, IdMappedEntity};

pub struct AreaFields;

impl Entity for AreaFields {
    type T = u8;
    const N: usize = 6;
    type FullT = [u8; 6];
    const NAME: &'static str = "area-fields";
}

impl IdMappedEntity for AreaFields {}

pub struct Authorships;

impl Entity for Authorships {
    type T = u32;
    const N: usize = 838782;
    type FullT = [u32; 838782];
    const NAME: &'static str = "authorships";
}

pub struct Qs;

impl Entity for Qs {
    type T = u8;
    const N: usize = 5;
    type FullT = [u8; 5];
    const NAME: &'static str = "qs";
}

pub struct Countries;

impl Entity for Countries {
    type T = u8;
    const N: usize = 91;
    type FullT = [u8; 91];
    const NAME: &'static str = "countries";
}

impl IdMappedEntity for Countries {}

pub struct Fields;

impl Entity for Fields {
    type T = u8;
    const N: usize = 27;
    type FullT = [u8; 27];
    const NAME: &'static str = "fields";
}

impl IdMappedEntity for Fields {}

pub struct Subfields;

impl Entity for Subfields {
    type T = u8;
    const N: usize = 253;
    type FullT = [u8; 253];
    const NAME: &'static str = "subfields";
}

impl IdMappedEntity for Subfields {}

pub struct Works;

impl Entity for Works {
    type T = u16;
    const N: usize = 10254;
    type FullT = [u16; 10254];
    const NAME: &'static str = "works";
}

impl IdMappedEntity for Works {}

pub struct Institutions;

impl Entity for Institutions {
    type T = u8;
    const N: usize = 5;
    type FullT = [u8; 5];
    const NAME: &'static str = "institutions";
}

impl IdMappedEntity for Institutions {}

pub struct Sources;

impl Entity for Sources {
    type T = u8;
    const N: usize = 20;
    type FullT = [u8; 20];
    const NAME: &'static str = "sources";
}

impl IdMappedEntity for Sources {}

pub struct Concepts;

impl Entity for Concepts {
    type T = u16;
    const N: usize = 65074;
    type FullT = [u16; 65074];
    const NAME: &'static str = "concepts";
}

impl IdMappedEntity for Concepts {}

pub struct Topics;

impl Entity for Topics {
    type T = u16;
    const N: usize = 4517;
    type FullT = [u16; 4517];
    const NAME: &'static str = "topics";
}

impl IdMappedEntity for Topics {}

pub struct Authors;

impl Entity for Authors {
    type T = u8;
    const N: usize = 253;
    type FullT = [u8; 253];
    const NAME: &'static str = "authors";
}

impl IdMappedEntity for Authors {}