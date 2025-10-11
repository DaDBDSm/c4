run:
	cargo run

build:
	cargo build

test:
	cargo test

gen-proto:
	cd client && cargo build

cmp-encoders:
	cargo run -p encoders-comparison --release

cmp-plot:
	gnuplot encoders-comparison/plot_benchmark.gp

.PHONY: run test cmp-encoders cmp-plot