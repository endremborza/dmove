use dmove::{FixedSizeAttribute, Link, Entity};

impl Link<crate::gen_types::Institutions,crate::gen_types::Countries> for InstCountries {}

pub struct InstCountries;

impl Entity for InstCountries {
    type T = u8;
    const N: usize = 6;
    type FullT = [u8; 6];
    const NAME: &'static str = "inst-countries";
}

impl FixedSizeAttribute for InstCountries {}

impl Link<crate::gen_types::Subfields,crate::gen_types::Fields> for SubfieldAncestors {}

pub struct SubfieldAncestors;

impl Entity for SubfieldAncestors {
    type T = u8;
    const N: usize = 254;
    type FullT = [u8; 254];
    const NAME: &'static str = "subfield-ancestors";
}

impl FixedSizeAttribute for SubfieldAncestors {}

impl Link<crate::gen_types::Topics,crate::gen_types::Subfields> for TopicSubfields {}

pub struct TopicSubfields;

impl Entity for TopicSubfields {
    type T = u8;
    const N: usize = 4518;
    type FullT = [u8; 4518];
    const NAME: &'static str = "topic-subfields";
}

impl FixedSizeAttribute for TopicSubfields {}

pub struct WorkYears;

impl Entity for WorkYears {
    type T = u8;
    const N: usize = 10255;
    type FullT = [u8; 10255];
    const NAME: &'static str = "work-years";
}

impl FixedSizeAttribute for WorkYears {}