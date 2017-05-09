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
	"crypto/rsa"
	"strings"

	"golang.org/x/net/context"

	"github.com/golang/glog"
	"github.com/mwitkow/go-grpc-middleware"
	"golang.org/x/oauth2/jws"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// ReadScope is the scope required for reading and searching logs.
const ReadScope = "https://baton.qutics.com/logspray/read"

// WriteScope is the scope required for writing logs to the server.
const WriteScope = "https://baton.qutics.com/logspray/write"

type key int

const claimsKey key = 0

// ClaimsFromContext retrieves the set of claims stored in a request context
func ClaimsFromContext(ctx context.Context) (*jws.ClaimSet, bool) {
	claims, ok := ctx.Value(claimsKey).(*jws.ClaimSet)
	return claims, ok
}

type keysFunc func() (map[string]*rsa.PublicKey, error)

// MakeAuthUnaryInterceptor is gRPC unary interceptor that checks for the
// required claims in JWT in the authorization header.
func MakeAuthUnaryInterceptor(kf keysFunc) func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		md, _ := metadata.FromContext(ctx)
		jst, ok := md["authorization"]
		if !ok && len(jst) != 1 {
			return handler(ctx, req)
		}

		var verified bool
		keys, err := kf()
		if err != nil {
			glog.Infof("could not fetch jws signing keys, %v", err)
			return handler(ctx, req)
		}

		if strings.HasPrefix(jst[0], "Bearer ") {
			jst[0] = strings.SplitN(jst[0], " ", 2)[1]
		}

		for kid, k := range keys {
			if glog.V(2) {
				glog.Infof("checking token %s against, %v", jst[0], kid)
			}
			if err := jws.Verify(jst[0], k); err == nil {
				verified = true
				break
			} else {
				if glog.V(1) {
					glog.Infof("could not verify jws token, %v", err)
				}
			}
		}
		if !verified {
			glog.Infof("could not verify jws token against any keys")
			return handler(ctx, req)
		}

		cs, err := func(tok string) (*jws.ClaimSet, error) {
			return jws.Decode(jst[0])
		}(jst[0])
		if err != nil {
			return handler(ctx, req)
		}

		newCtx := context.WithValue(ctx, claimsKey, cs)
		return handler(newCtx, req)
	}
}

// MakeAuthStreamingInterceptor is gRPC stream interceptor that checks for the
// required claims in JWT in the authorization header.
func MakeAuthStreamingInterceptor(kf keysFunc) func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return func(
		srv interface{},
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		newStream := grpc_middleware.WrapServerStream(stream)
		md, _ := metadata.FromContext(newStream.WrappedContext)

		jst, ok := md["authorization"]
		if !ok && len(jst) != 1 {
			return handler(srv, newStream)
		}

		var verified bool
		keys, err := kf()
		if err != nil {
			glog.Infof("could not fetch jws signing keys, %v", err)
			return handler(srv, newStream)
		}

		if strings.HasPrefix(jst[0], "Bearer ") {
			jst[0] = strings.SplitN(jst[0], " ", 2)[1]
		}

		for kid, k := range keys {
			if glog.V(2) {
				glog.Infof("checking token %q against, %v", jst[0], kid)
			}
			if err := jws.Verify(jst[0], k); err == nil {
				verified = true
				break
			} else {
				if glog.V(1) {
					glog.Infof("could not verify jws token, %v", err)
				}
			}
		}
		if !verified {
			glog.Infof("could not verify jws token against any keys")
			return handler(srv, newStream)
		}

		cs, err := jws.Decode(jst[0])
		if err != nil {
			return handler(srv, newStream)
		}

		newStream.WrappedContext = context.WithValue(newStream.WrappedContext, claimsKey, cs)
		return handler(srv, newStream)
	}
}

func (l *logServer) ensureScope(ctx context.Context, rs string) error {
	if !l.checkClaims {
		return nil
	}

	cs, ok := ClaimsFromContext(ctx)
	if !ok {
		return grpc.Errorf(codes.Unauthenticated, "no jwt claims found")
	}

	for _, s := range strings.Split(cs.Scope, " ") {
		if s == rs {
			return nil
		}
	}
	return grpc.Errorf(codes.Unauthenticated, "scope %s not present", rs)
}
