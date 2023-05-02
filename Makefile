CARGO=cargo
CARGO_NIGHTLY=rustup run nightly cargo

run: build
	RUST_BACKTRACE=1 ${CARGO} run -- --nocapture

build:
	${CARGO} build --color=always --all --all-targets

release:
	${CARGO} build --release --color=always --all --all-targets && bin/release.sh

test:
	RUST_BACKTRACE=1 ${CARGO} test -- --nocapture

fmt:
	${CARGO_NIGHTLY} fmt

clean:
	${CARGO} clean
