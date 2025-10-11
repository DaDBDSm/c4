run:
	cargo run

test:
	cargo test

cmp-encoders:
	cargo run -p encoders-comparison --release

cmp-plot:
	gnuplot encoders-comparison/plot_benchmark.gp

.PHONY: run test cmp-encoders cmp-plot