include .env
export

hello:
	echo "no fuckup here!"

include rankless_rs/Makefile

download-snapshot:
	aws s3 sync "s3://openalex" $(OA_SNAPSHOT) --no-sign-request

build-prep:
	cargo build --release -p dmove-macro
	./target/release/dmove-macro -p rankless_rs make-setup

to-csv: 
	cargo run --release -p rankless-rs -- $@ $(OA_ROOT) $(OA_SNAPSHOT)/data

filter: clean-filters clean-keys clean-cache
	cargo run --release -p rankless-rs -- $@ $(OA_ROOT)

tree-test:
	cargo run --release -p rankless-rs -- $@ $(OA_ROOT)

run-server:
	cargo run --release -p rankless-server -- $(OA_ROOT) 

extend_csvs bm:
	python3 -m pyscripts.$@

set-full:
	cp bak-gen-full/* rankless_rs/src/gen/
	./set-env full

set-mini:
	./set-env mini

set-micro:
	./set-env micro

set-nano:
	rm rankless_rs/src/gen/*
	./set-env nano

complete: to-csv filter extend_csvs rankless_rs/src/gen/derive_links5.rs
	@echo Complete

big-test:
	cargo test --release -p rankless-trees --tests instances::tests::big_tree -- --nocapture

profile:
	cargo build --release
	echo "-1"  | sudo tee /proc/sys/kernel/perf_event_paranoid
	echo "0" | sudo tee /proc/sys/kernel/kptr_restrict
	# flamegraph -o make_fg.svg -- target/release/dmove fix-atts $(OA_ROOT)
	# flamegraph -o make_fg.svg -- target/release/rankless-server $(OA_ROOT)
	# flamegraph -o make_fg.svg -- cargo test --release -p rankless-trees --tests instances::tests::big_tree  -- --nocapture
	flamegraph -o make_fg.svg -- target/release/rankless-trees
	echo "4"  | sudo tee /proc/sys/kernel/perf_event_paranoid
	echo "1" | sudo tee /proc/sys/kernel/kptr_restrict
	# install linux-tools-generic

test-server:
	time curl localhost:3038/v1/names/authors?q=ces

backup-gens:
	mkdir -p rankless_rs/src/gen/$(RANKLESS_ENV)/
	cp rankless_rs/src/gen_* rankless_rs/src/gen/$(RANKLESS_ENV)/

nuke:
	rm -rf $(OA_ROOT)

clean-filters:
	rm -rf $(OA_ROOT)/filter-steps

clean-keys:
	rm -rf $(OA_ROOT)/entity_mapping

clean-cache:
	rm -rf $(OA_ROOT)/cache
	rm -rf /tmp/dmove-parts

clean-profile:
	rm perf.data*
	rm make_fg.svg

quiet-build:
	RUSTFLAGS="$RUSTFLAGS -A dead_code -A non_snake_case -A unused_variables" cargo build
