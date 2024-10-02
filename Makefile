include .env
export

QV := quercus-basis-v`date +%Y-%m-%d`
S3_LOC := s3://tmp-borza-public-cyx/$(QV)
TEST_DIR := /tmp/dmove-test
ACT_OA_ROOT := $(OA_ROOT)
ACT_OA_SNAPSHOT := $(OA_SNAPSHOT)
RANKLESS_ENV = full

export

hello:
	echo "no fuckup here!"

download-snapshot:
	aws s3 sync "s3://openalex" $(OA_SNAPSHOT) --no-sign-request
	# aws s3 sync "s3://openalex" /volume2/alpha-share-solid/oa-snapshot-2024-09 --no-sign-request

to-csv: 
	cargo run --release -p rankless-rs -- $@ $(ACT_OA_ROOT) $(ACT_OA_SNAPSHOT)/data


filter entity_mapping init_atts derive_links1 derive_links2:
	export RANKLESS_ENV
	cargo run --release -p rankless-rs -- $@ $(ACT_OA_ROOT) 

run-server:
	export RANKLESS_ENV
	cargo run --release -p rankless-server -- $@ $(ACT_OA_ROOT) 

serve extend_csvs post_agg common:
	export ACT_OA_ROOT=$(ACT_OA_ROOT)
	export ACT_SNAPSHOT=$(ACT_OA_SNAPSHOT)
	python3 -m pyscripts.$@

deploy-data-to-s3:
	aws s3 rm $(S3_LOC) --recursive
	aws s3 sync $(OA_ROOT)/pruned-cache $(S3_LOC)  --acl public-read --content-encoding gzip
	echo $(S3_LOC)

mini-%: ACT_OA_ROOT = $(OA_TEST_ROOT)/mini-root
mini-%: ACT_OA_SNAPSHOT = $(OA_TEST_ROOT)/mini-snapshot

micro-%: ACT_OA_ROOT = $(OA_TEST_ROOT)/micro-root
micro-%: ACT_OA_SNAPSHOT = $(OA_TEST_ROOT)/micro-snapshot

nano-%: ACT_OA_ROOT = $(OA_TEST_ROOT)/nano-root
nano-%: ACT_OA_SNAPSHOT = $(OA_TEST_ROOT)/nano-snapshot
nano-%: RANKLESS_ENV = nano

complete: common to-csv filter extend_csvs entity_mapping init_atts derive_links1 derive_links2 #post_agg
	@echo Complete

mini-test: nuke complete
micro-test: nuke complete
nano-test: nuke complete

nano-server: run-server


profile:
	# echo "-1"  > perf_event_paranoid
	sudo nvim perf_event_paranoid /proc/sys/kernel/
	flamegraph -o make_fg.svg -- target/release/dmove fix-atts $(ACT_OA_ROOT)
	sudo nvim perf_event_paranoid /proc/sys/kernel/
	# echo "4"  > perf_event_paranoid
	# sudo mv perf_event_paranoid /proc/sys/kernel/
	# install linux-tools-generic

nuke:
	rm -rf $(ACT_OA_ROOT)

clean-filters:
	rm -rf $(OA_ROOT)/filter-steps

clean-cache:
	rm -rf $(OA_ROOT)/cache

quiet-build:
	RUSTFLAGS="$RUSTFLAGS -A dead_code -A non_snake_case -A unused_variables" cargo build
