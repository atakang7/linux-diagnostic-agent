GO ?= go
BIN_DIR ?= bin

.PHONY: build run test vet clean

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/agent ./cmd

run:
	$(GO) run ./cmd

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

clean:
	rm -rf $(BIN_DIR)
