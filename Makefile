APP ?= mqtt-loadtest
GOLANG_VERSION ?= 1.13
VERSION ?= $(shell git rev-parse HEAD)
DOCKER_IMAGE_NO_TAG ?= freenow/$(APP)
DOCKER_IMAGE ?= $(DOCKER_IMAGE_NO_TAG):$(VERSION)
DOCKER_DEFAULT_PARAMS ?= --rm -v "$(PWD):/go/src/github.com/freenowtech/$(APP)" -w "/go/src/github.com/freenowtech/$(APP)" golang:$(GOLANG_VERSION)

.DEFAULT_GOAL = package

.PHONY: package
package:
	docker build --pull --no-cache -t $(DOCKER_IMAGE) --build-arg GOLANG_VERSION=$(GOLANG_VERSION) .

.PHONY: release
release: package
	docker push $(DOCKER_IMAGE)

.PHONY: package_latest
package_latest:
	docker build --pull --no-cache -t "$(DOCKER_IMAGE_NO_TAG):latest" --build-arg GOLANG_VERSION=$(GOLANG_VERSION) .

.PHONY: release_latest
release_latest: package_latest
	docker push "$(DOCKER_IMAGE_NO_TAG):latest"

.PHONY: build_linux
build_linux:
	docker run -e "GOARCH=amd64" -e "GOOS=linux" $(DOCKER_DEFAULT_PARAMS) go build -o $(APP).linux-amd64

.PHONY: build_darwin
build_darwin:
	docker run -e "GOARCH=amd64" -e "GOOS=darwin" $(DOCKER_DEFAULT_PARAMS) go build -o $(APP).darwin-amd64

.PHONY: build_local
build_local:
	go build
