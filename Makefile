.PHONY: run build refdata

run:
	@cargo run --release

build:
	@cargo build --release

refdata:
	@cargo run --release -- refdata
