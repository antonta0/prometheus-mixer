DOCKER_IMAGE_NAME ?= prometheus-mixer
DOCKER_IMAGE_TAG  ?= $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))

GOLINT := $(GOPATH)/bin/golint
PROMU := $(GOPATH)/bin/promu

PKGS = $(shell go list ./... | grep -v /vendor/)

.PHONY: all
all: format build test

.PHONY: test
test:
	go test $(PKGS)

.PHONY: format
format:
	go fmt $(PKGS)

.PHONY: lint
lint: $(GOLINT)
	$(GOLINT) -set_exit_status $(PKGS)

.PHONY: vet
vet:
	go vet -shadowstrict -methods=false $(PKGS)

.PHONY: build
build: $(PROMU)
	$(PROMU) build

.PHONY: docker
docker:
	docker build -t "$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)" .

$(GOLINT):
	go get -u github.com/golang/lint/golint

$(PROMU):
	GOOS= GOARCH= go get -u github.com/prometheus/promu
