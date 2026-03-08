.PHONY: help build run test fmt lint clean tidy

# Default target
help:
	@echo "Available targets:"
	@echo "  build    - Build the binary"
	@echo "  run      - Run the service"
	@echo "  test     - Run all tests"
	@echo "  fmt      - Format code using golangci-lint"
	@echo "  lint     - Run linters using golangci-lint"
	@echo "  tidy     - Tidy go modules"
	@echo "  clean    - Clean build artifacts"
	@echo "  check    - Run fmt, lint, and test"

# Binary name
BINARY_NAME=postgres-mem-go

# Build the binary
build:
	go build -o $(BINARY_NAME) .

# Run the service
run:
	go run .

# Run all tests
test:
	go test ./...

# Run tests with race detection
test-race:
	go test -race -v ./...

# Format code
format:
	@golangci-lint fmt

# Run linters
lint:
	@go build ./...
	@golangci-lint run

# Tidy go modules
tidy:
	go mod tidy

# Clean build artifacts
clean:
	rm -f $(BINARY_NAME)

# Run all checks (fmt, lint, test)
check: fmt lint test


tests-dry-run:
	juris-go-parser dry-run .

tests-generate:
	juris-go-parser generate -default-system "postgres-mem-go" .

tests-upload:
	juris-go-parser upload --endpoint http://127.0.0.1:8000 .