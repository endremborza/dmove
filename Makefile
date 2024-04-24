include .env
export

to-csv:
	cargo run --release -- to-csv $(OA_ROOT) $(OA_SNAPSHOT) 

filter:
	cargo run --release -- filter $(OA_ROOT) 

to-keys:
	cargo run --release -- to-keys $(OA_ROOT) 

to-edges:
	cargo run --release -- to-edges $(OA_ROOT) 

fix-atts:
	cargo run --release -- fix-atts $(OA_ROOT) 

var-atts:
	cargo run --release -- var-atts $(OA_ROOT) 

serve:
	python pysrc/serve.py
