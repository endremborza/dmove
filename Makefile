include .env
export

QV := quercus-basis-v`date +%Y-%m-%d`
S3_LOC := s3://tmp-borza-public-cyx/$(QV)

hello:
	echo "no fuckup here!"

download-snapshot:
	aws s3 sync "s3://openalex" $(OA_SNAPSHOT) --no-sign-request

to-csv: 
	cargo run --release -- $@ $(OA_ROOT) $(OA_SNAPSHOT)/data

filter fix-atts var-atts build-qcs prune-qcs agg-qcs packet-qcs:
	cargo run --release -- $@ $(OA_ROOT) 

serve extend_csvs post_agg:
	python3 -m pyscripts.$@

deploy-data-to-s3:
	aws s3 rm $(S3_LOC) --recursive
	aws s3 sync $(OA_ROOT)/pruned-cache $(S3_LOC)  --acl public-read --content-encoding gzip
	echo $(S3_LOC)

profile:
	# echo "-1"  > perf_event_paranoid
	sudo nvim perf_event_paranoid /proc/sys/kernel/
	flamegraph -o make_fg.svg -- target/release/dmove fix-atts $(OA_ROOT)
	sudo nvim perf_event_paranoid /proc/sys/kernel/
	# echo "4"  > perf_event_paranoid
	# sudo mv perf_event_paranoid /proc/sys/kernel/
	# install linux-tools-generic

clean-keys:
	rm -rf $(OA_ROOT)/key-stores

clean-filters:
	rm -rf $(OA_ROOT)/filter-steps

clean-cache:
	rm -rf $(OA_ROOT)/cache

