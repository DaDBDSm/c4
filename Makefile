run:
	cargo run

test:
	cargo test

gen-proto:
	cd client && cargo build

.PHONY: run test