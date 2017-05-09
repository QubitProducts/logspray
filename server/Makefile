.PHONY: assets

assets: bindata_assetfs.go

bindata_assetfs.go: swagger.json
	go-bindata-assetfs -pkg server -prefix=swagger-ui swagger.json swagger-ui/dist/...

swagger.json: ../proto/logspray/log.proto
	protoc -I/usr/local/include -I../ -I$$GOPATH/src -I$$GOPATH/src/github.com/golang/protobuf/ptypes -I$$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --swagger_out=logtostderr=true:. ../log.proto
	mv log.swagger.json swagger.json

clean:
	rm bindata_assetfs.go swagger.json

