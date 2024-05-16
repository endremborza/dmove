include .env
export

QV := quercus-basis-v6
S3_LOC := s3://tmp-borza-public-cyx/$(QV)

hello:
	echo "no fuckup here!"

download-snapshot:
	aws s3 sync "s3://openalex" $(OA_SNAPSHOT) --no-sign-request

to-csv: 
	cargo run --release -- $@ $(OA_ROOT) $(OA_SNAPSHOT)

filter to-keys fix-atts var-atts build-qcs prune-qcs:
	cargo run --release -- $@ $(OA_ROOT) 

inst_str_id serve to_build_urls paper_qs:
	python3 pyscripts/$@.py

pre_var_att_py: paper_qs inst_str_id
	echo "pyruns"

deploy:
	aws s3 rm $(S3_LOC) --recursive
	aws s3 sync $(OA_ROOT)/cache $(S3_LOC)  --acl public-read --content-encoding gzip

clean-keys:
	rm -rf $(OA_ROOT)/key-stores

clean-filters:
	rm -rf $(OA_ROOT)/filter-steps

clean-cache:
	rm -rf $(OA_ROOT)/cache

