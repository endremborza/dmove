rankless_rs/src/gen/a1_entity_mapping.rs: rankless_rs/src/steps/a1_entity_mapping.rs 
	./target/release/dmove-macro -p rankless_rs pre-build -s a1_entity_mapping
	cargo build -p rankless-rs --release
	cargo run -p rankless-rs --release -- a1_entity_mapping
	./target/release/dmove-macro -p rankless_rs post-run -s a1_entity_mapping

rankless_rs/src/gen/a2_init_atts.rs: rankless_rs/src/steps/a2_init_atts.rs rankless_rs/src/gen/a1_entity_mapping.rs
	./target/release/dmove-macro -p rankless_rs pre-build -s a2_init_atts
	cargo build -p rankless-rs --release
	cargo run -p rankless-rs --release -- a2_init_atts
	./target/release/dmove-macro -p rankless_rs post-run -s a2_init_atts

rankless_rs/src/gen/derive_links1.rs: rankless_rs/src/steps/derive_links1.rs rankless_rs/src/gen/a2_init_atts.rs
	./target/release/dmove-macro -p rankless_rs pre-build -s derive_links1
	cargo build -p rankless-rs --release
	cargo run -p rankless-rs --release -- derive_links1
	./target/release/dmove-macro -p rankless_rs post-run -s derive_links1

rankless_rs/src/gen/derive_links2.rs: rankless_rs/src/steps/derive_links2.rs rankless_rs/src/gen/derive_links1.rs
	./target/release/dmove-macro -p rankless_rs pre-build -s derive_links2
	cargo build -p rankless-rs --release
	cargo run -p rankless-rs --release -- derive_links2
	./target/release/dmove-macro -p rankless_rs post-run -s derive_links2

rankless_rs/src/gen/derive_links3.rs: rankless_rs/src/steps/derive_links3.rs rankless_rs/src/gen/derive_links2.rs
	./target/release/dmove-macro -p rankless_rs pre-build -s derive_links3
	cargo build -p rankless-rs --release
	cargo run -p rankless-rs --release -- derive_links3
	./target/release/dmove-macro -p rankless_rs post-run -s derive_links3

rankless_rs/src/gen/derive_links4.rs: rankless_rs/src/steps/derive_links4.rs rankless_rs/src/gen/derive_links3.rs
	./target/release/dmove-macro -p rankless_rs pre-build -s derive_links4
	cargo build -p rankless-rs --release
	cargo run -p rankless-rs --release -- derive_links4
	./target/release/dmove-macro -p rankless_rs post-run -s derive_links4

rankless_rs/src/gen/derive_links5.rs: rankless_rs/src/steps/derive_links5.rs rankless_rs/src/gen/derive_links4.rs
	./target/release/dmove-macro -p rankless_rs pre-build -s derive_links5
	cargo build -p rankless-rs --release
	cargo run -p rankless-rs --release -- derive_links5
	./target/release/dmove-macro -p rankless_rs post-run -s derive_links5
