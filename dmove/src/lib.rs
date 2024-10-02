#![feature(min_specialization)]
// rustup override set nightly-2024-07-25
// mod aggregate_quercus;
mod common;
mod discontinuous_entity_mapper;
mod fixed_size_attributes;
mod ingest_entity;
pub mod para;
mod var_size_attributes;
// mod var_size_attributes;
// mod prune_quercus;
// mod quercus;
// mod quercus_packet;

// pub use aggregate_quercus::aggregate;
// pub use prune_quercus::prune;
// pub use quercus::dump_all_cache;
// pub use quercus_packet::dump_packets;

pub use common::{
    BackendLoading, BigId, ByteArrayInterface, Entity, EntityImmutableMapperBackend,
    EntityImmutableRefMapperBackend, EntityMutableMapperBackend, FixedSizeAttribute, Link,
    MainBuilder, MappableEntity, MetaIntegrator, NamespacedEntity, UnsignedNumber,
    VariableSizeAttribute,
};
pub use discontinuous_entity_mapper::DiscoMapEntityBuilder;
pub use fixed_size_attributes::{FixAttBuilder, FixedAttributeElement};
pub use ingest_entity::{Data64MappedEntityBuilder, IdMap};
pub use var_size_attributes::{VarAttBuilder, VarAttIterator, VarBox, VarSizedAttributeElement};

//definitions
//compact entity: identifyable entity with ids 0-N
//0 might mean unknown so nullable / non-nullable compact entities possible

// a possible element:
// trait for possible elements to iterate over
// implement it for some (BigId for map, u8-u128 for fixAtts, String for varAtts)
// read + write based on a directory
// some struct to create extra elements of meta code

//id map is just a reversed index u64 fixed attribute
