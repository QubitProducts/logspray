// Copyright 2016 Qubit Digital Ltd.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Package logspray is a collection of tools for streaming and indexing
// large volumes of dynamic logs.

package server

import (
	"fmt"
	"log"
	"mime"
	"net/http"
	"strings"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/rakyll/statik/fs"
	"github.com/tcolgate/grafana-simple-json-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	_ "github.com/QubitProducts/logspray/server/statik"
)

var statikFS http.FileSystem

func init() {
	var err error
	statikFS, err = fs.New()
	if err != nil {
		log.Println(err)
	}
}

// grpcHandlerFunc returns an http.Handler that delegates to grpcServer on incoming gRPC
// connections or otherHandler otherwise. Copied from cockroachdb.
func grpcHandlerFunc(grpcServer *grpc.Server, otherHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			otherHandler.ServeHTTP(w, r)
		}
	})
}

// HandleSwagger adds the swagger-ui to the provided mux
func HandleSwagger(mux *http.ServeMux) {
	mime.AddExtensionType(".svg", "image/svg+xml")

	mux.Handle("/swagger-ui/", http.StripPrefix("/swagger-ui/", http.FileServer(statikFS)))

	/*
		mux.HandleFunc("/swagger-ui/swagger.json", func(w http.ResponseWriter, req *http.Request) {
			lsj, _ := swaggerJsonSwaggerJsonBytes()
			io.Copy(w, strings.NewReader(string(lsj)))
		})
	*/
}

func basicAuth(next http.Handler, user, password string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user == "" {
			next.ServeHTTP(w, r)
			return
		}

		reqUser, reqPass, _ := r.BasicAuth()

		if user != reqUser || password != reqPass {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			http.Error(w, "Unauthorized.", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Register sets up the log server on the provided http and grpc servers. THe
// dial options will be used for all outbound gRPC reuqests.
func Register(ctx context.Context, srv *http.Server, grpcServer *grpc.Server, dopts []grpc.DialOption, opts ...serverOpt) error {
	lsrv := new(opts...)

	logspray.RegisterLogServiceServer(grpcServer, lsrv)

	gwmux := runtime.NewServeMux()

	err := logspray.RegisterLogServiceHandlerFromEndpoint(ctx, gwmux, srv.Addr, dopts)
	if err != nil {
		fmt.Printf("serve: %v\n", err)
		return err
	}

	sjch := basicAuth(simplejson.New(
		simplejson.WithQuerier(lsrv.indx),
		simplejson.WithTableQuerier(lsrv.indx),
		simplejson.WithSearcher(lsrv.indx),
		simplejson.WithTagSearcher(lsrv.indx),
		simplejson.WithAnnotator(lsrv.indx),
	), lsrv.grafanaUser, lsrv.grafanaPass)

	mux := http.NewServeMux()
	mux.Handle("/", sjch)
	mux.Handle("/v1/", gwmux)
	mux.Handle("/query", sjch)
	mux.Handle("/search", sjch)
	mux.Handle("/annotations", sjch)
	mux.Handle("/tag-keys", sjch)
	mux.Handle("/tag-values", sjch)

	srv.Handler = grpcHandlerFunc(grpcServer, mux)

	return nil
}
