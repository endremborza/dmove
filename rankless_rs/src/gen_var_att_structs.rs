use dmove::{Entity, VariableSizeInMemoryAttribute};

pub struct FieldsNames;

impl Entity for FieldsNames {
    type T = String;
    const N: usize = 27;
    type FullT = [String; 27];
    const NAME: &'static str = "fields-names";
}

impl VariableSizeInMemoryAttribute for FieldsNames {    type SizeType = u8;}

pub struct SubfieldsNames;

impl Entity for SubfieldsNames {
    type T = String;
    const N: usize = 253;
    type FullT = [String; 253];
    const NAME: &'static str = "subfields-names";
}

impl VariableSizeInMemoryAttribute for SubfieldsNames {    type SizeType = u8;}

pub struct InstitutionsNames;

impl Entity for InstitutionsNames {
    type T = String;
    const N: usize = 5;
    type FullT = [String; 5];
    const NAME: &'static str = "institutions-names";
}

impl VariableSizeInMemoryAttribute for InstitutionsNames {    type SizeType = u8;}

pub struct SourcesNames;

impl Entity for SourcesNames {
    type T = String;
    const N: usize = 20;
    type FullT = [String; 20];
    const NAME: &'static str = "sources-names";
}

impl VariableSizeInMemoryAttribute for SourcesNames {    type SizeType = u8;}

pub struct AuthorsNames;

impl Entity for AuthorsNames {
    type T = String;
    const N: usize = 253;
    type FullT = [String; 253];
    const NAME: &'static str = "authors-names";
}

impl VariableSizeInMemoryAttribute for AuthorsNames {    type SizeType = u8;}

pub struct CountriesNames;

impl Entity for CountriesNames {
    type T = String;
    const N: usize = 91;
    type FullT = [String; 91];
    const NAME: &'static str = "countries-names";
}

impl VariableSizeInMemoryAttribute for CountriesNames {    type SizeType = u8;}

pub struct QsNames;

impl Entity for QsNames {
    type T = String;
    const N: usize = 5;
    type FullT = [String; 5];
    const NAME: &'static str = "qs-names";
}

impl VariableSizeInMemoryAttribute for QsNames {    type SizeType = u8;}

pub struct WTopicHier;

impl Entity for WTopicHier {
    type T = crate::var_atts::HEdgeSet<u8, u8>;
    const N: usize = 10254;
    type FullT = [crate::var_atts::HEdgeSet<u8, u8>; 10254];
    const NAME: &'static str = "w2topic-hier";
}

impl VariableSizeInMemoryAttribute for WTopicHier {    type SizeType = u8;}

pub struct WQs;

impl Entity for WQs {
    type T = crate::var_atts::HEdgeSet<u8, u8>;
    const N: usize = 10254;
    type FullT = [crate::var_atts::HEdgeSet<u8, u8>; 10254];
    const NAME: &'static str = "w2qs";
}

impl VariableSizeInMemoryAttribute for WQs {    type SizeType = u8;}

pub struct WLoc;

impl Entity for WLoc {
    type T = Vec<u8>;
    const N: usize = 10254;
    type FullT = [Vec<u8>; 10254];
    const NAME: &'static str = "w2loc";
}

impl VariableSizeInMemoryAttribute for WLoc {    type SizeType = u8;}

pub struct WCiting;

impl Entity for WCiting {
    type T = Vec<u16>;
    const N: usize = 10254;
    type FullT = [Vec<u16>; 10254];
    const NAME: &'static str = "w2citing";
}

impl VariableSizeInMemoryAttribute for WCiting {    type SizeType = u8;}

pub struct SAf;

impl Entity for SAf {
    type T = Vec<u8>;
    const N: usize = 20;
    type FullT = [Vec<u8>; 20];
    const NAME: &'static str = "s2af";
}

impl VariableSizeInMemoryAttribute for SAf {    type SizeType = u8;}

pub struct WCountryHier;

impl Entity for WCountryHier {
    type T = crate::var_atts::HEdgeSet<u8, u8>;
    const N: usize = 10254;
    type FullT = [crate::var_atts::HEdgeSet<u8, u8>; 10254];
    const NAME: &'static str = "w2country-hier";
}

impl VariableSizeInMemoryAttribute for WCountryHier {    type SizeType = u8;}

pub struct AW;

impl Entity for AW {
    type T = Vec<u16>;
    const N: usize = 253;
    type FullT = [Vec<u16>; 253];
    const NAME: &'static str = "a2w";
}

impl VariableSizeInMemoryAttribute for AW {    type SizeType = u8;}

pub struct IW;

impl Entity for IW {
    type T = Vec<crate::var_atts::WeightedEdge<u16>>;
    const N: usize = 5;
    type FullT = [Vec<crate::var_atts::WeightedEdge<u16>>; 5];
    const NAME: &'static str = "i2w";
}

impl VariableSizeInMemoryAttribute for IW {    type SizeType = u16;}