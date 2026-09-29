GO ?= go

.PHONY: build test vet check fmt clean

build:
	$(GO) build -o bin/dbgserver ./cmd/dbgserver

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

check: build
	python3 scripts/dbgcheck.py

fmt:
	$(GO) fmt ./...

clean:
	rm -rf bin
