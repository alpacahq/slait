install:
	go install .

vendor:
	go mod vendor

unittest:
	go fmt ./...
	go test ./...
