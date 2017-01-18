BINARY=kayak

PHONY: all

test:
	go test  -v ./...

get:
	go get

image:
	cp ~/.ssh/id_rsa_jenkins .
	docker build --no-cache -t eu.gcr.io/scalezen/infra/${BINARY}:0.1 .
	gcloud docker push eu.gcr.io/scalezen/infra/${BINARY}:0.1
	rm -f id_rsa_jenkins

all:
	go build -ldflags "-X main.version=`git describe --tags`" -o ${BINARY} main.go
