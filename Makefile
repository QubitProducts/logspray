NAME=logspray
VERSION=0.4.0

.PHONY: protoc server packages clean binary deploy-staging deploy-production

all: protoc server

protoc: proto/logspray/log.pb.go proto/logspray/log.pb.gw.go

proto/logspray/log.pb.go: proto/logspray/log.proto
	protoc -I/usr/local/include \
		     -I. \
				 -I$$GOPATH/src \
				 -I$$GOPATH/src/github.com/golang/protobuf/ptypes \
				 -I$$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
				 --go_out=plugins=grpc:. proto/logspray/log.proto

proto/logspray/log.pb.gw.go: proto/logspray/log.proto
	protoc -I/usr/local/include \
		     -I. \
				 -I$$GOPATH/src \
				 -I$$GOPATH/src/github.com/golang/protobuf/ptypes \
				 -I$$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
				 --grpc-gateway_out=logtostderr=true:. proto/logspray/log.proto

server: protoc
	$(MAKE) -C server/

clean:
	rm -f proto/logspray/log.pb.go proto/logspray/log.pb.gw.go

build-docker-binary: logs

logs: protoc server
	CGO_ENABLED=0 cd cmd/logs && go build -i -o ../../logs

prepare-deploy: build-docker-binary

PACKAGE_NAME=logspray
PACKAGE_VERSION=$(VERSION)

PACKAGE_REVISION=0
PACKAGE_ARCH=amd64
PACKAGE_MAINTAINER=tristan@qubit.com
PACKAGE_FILE=$(PACKAGE_NAME)_$(PACKAGE_VERSION)-$(PACKAGE_REVISION)_$(PACKAGE_ARCH).deb

BINNAME=logs

PWD=$(shell pwd)
CURL=/usr/bin/curl

.PHONY:

package-dist: $(BINNAME)
	mkdir -p dist/$(BINNAME)/usr/local/bin
	install -m755 $(BINNAME) dist/$(BINNAME)/usr/local/bin/$(BINNAME)
	mkdir -p dist/$(BINNAME)/etc/init
	mkdir -p dist/$(BINNAME)/etc/default
	install -m644 $(BINNAME).conf dist/$(BINNAME)/etc/init/$(BINNAME).conf
	install -m644 $(BINNAME).defaults dist/$(BINNAME)/etc/default/$(BINNAME)
	mkdir -p dist/$(BINNAME)/etc/$(BINNAME)
	echo '{}' dist/$(BINNAME)/etc/$(BINNAME)/service.json

packages: package-dist
	cd dist/$(BINNAME) && \
	  fpm \
	  -t deb \
	  -m $(PACKAGE_MAINTAINER) \
	  -n $(PACKAGE_NAME) \
	  -a $(PACKAGE_ARCH) \
	  -v $(PACKAGE_VERSION) \
	  --iteration $(PACKAGE_REVISION) \
	  -s dir \
	  -p ../../$(PACKAGE_FILE) \
	  .

deb-clean:
	rm -f $(PACKAGE_FILE)
	rm -rf dist
	rm -rf build


KUBE_CONTEXT_BASE=k8.irl.aws.qutics.com

CONTAINER_VERSION=$(shell git rev-parse HEAD)
DOCKER_IMAGE=logspray:latest

docker-build: prepare-deploy
	docker build -t $(DOCKER_IMAGE) .
	docker tag $(DOCKER_IMAGE) $(DOCKER_AWS_IMAGE)

docker-push: docker-build
	docker push $(DOCKER_IMAGE)

	kubectl --context "$*.$(KUBE_CONTEXT_BASE)" delete pod -l app=logspray-reader
	kubectl --context "$*.$(KUBE_CONTEXT_BASE)" delete pod -l app=logspray-server

deploy:
	helm --kube-context "$*.$(KUBE_CONTEXT_BASE)" upgrade \
    --set global.dockerImage=${DOCKER_IMAGE}
		--debug \
    logspray-server \
    ./helm/logspray

