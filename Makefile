all:
	go install .

install: all

configure:
	dep ensure

update:
	dep ensure -update

unittest:
	go fmt ./...
	go vet ./...
	go test ./...
