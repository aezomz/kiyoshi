set shell := ["bash", "-uc"]

check:
	cargo check

fmt toolchain="+nightly":
	cargo {{toolchain}} fmt

fmt-check toolchain="+nightly":
	cargo {{toolchain}} fmt --check

lint:
	cargo clippy --no-deps -- -D warnings

test:
	cargo test

fix:
	cargo fix --allow-dirty --allow-staged

all: check fmt lint test

docker tag="latest":
	docker build \
		-t aezomz/kiyoshi:{{tag}} \
		-f Dockerfile \
		.