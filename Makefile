include .env
export

to-csv filter to-keys fix-atts var-atts build-qcs:
	cargo run --release -- $@ $(OA_ROOT) 

serve:
	python3 pyscripts/serve.py
