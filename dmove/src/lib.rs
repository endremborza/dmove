#![feature(min_specialization)]
// #![feature(generic_const_exprs)]
// rustup override set nightly-2024-07-25
mod common;
mod discontinuous_entity_mapper;
mod fixed_size_attributes;
mod ingest_entity;
pub mod para;
mod var_size_attributes;

pub use common::{
    camel_case, BackendLoading, BigId, ByteArrayInterface, ByteFixArrayInterface, CompactEntity,
    Entity, EntityImmutableMapperBackend, EntityImmutableRefMapperBackend,
    EntityMutableMapperBackend, Link, MainBuilder, MappableEntity, MarkedAttribute, MetaIntegrator,
    NamespacedEntity, UnsignedNumber, VariableSizeAttribute, ET, MAA,
};
pub use discontinuous_entity_mapper::{DiscoMapEntityBuilder, UniqueMap};
pub use fixed_size_attributes::{
    DowncastingBuilder, FixAttBuilder, FixAttIterator, FixWriteSizeEntity,
};
pub use ingest_entity::{Data64MappedEntityBuilder, IdMap, LoadedIdMap};
pub use var_size_attributes::{
    Locators, VarAttBuilder, VarAttIterator, VarBox, VarSizedAttributeElement, VattReadingMap,
    VattReadingRefMap,
};

//definitions
//compact entity: identifyable entity with ids 0-N
//0 might mean unknown so nullable / non-nullable compact entities possible

// a possible element:
// trait for possible elements to iterate over
// implement it for some (BigId for map, u8-u128 for fixAtts, String for varAtts)
// read + write based on a directory
// some struct to create extra elements of meta code

//id map is just a reversed index u64 fixed attribute
