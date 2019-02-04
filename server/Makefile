.PHONY: assets

assets: statik/statik.go

statik/statik.go: swagger-ui/swagger.json
	statik --src=swagger-ui -f

swagger-ui/swagger.json: ../proto/logspray/log.proto
	protoc -I/usr/local/include -I../ -I$$GOPATH/src -I$$GOPATH/src/github.com/golang/protobuf/ptypes -I$$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --swagger_out=logtostderr=true:. ../proto/logspray/log.proto
	mv proto/logspray/log.swagger.json swagger-ui/swagger.json

clean:
	rm -rf statik swagger-ui/swagger.json

