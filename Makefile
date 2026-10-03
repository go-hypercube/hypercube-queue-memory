# Test the codebase
test:
	@echo "Testing..."
	@go test ./...

lint:
	@echo "Running golangci-lint"
	@golangci-lint run ./...

# Format the codebase using golangci-lint fmt
fmt:
	@echo "Formatting using golangci-lint"
	@golangci-lint fmt ./...
