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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	logs "github.com/QubitProducts/logspray/cmd/logs/root"
	"github.com/QubitProducts/logspray/indexer"
	server "github.com/QubitProducts/logspray/server"
)

var (
	tlsAddr   string
	adminAddr string

	caFile string

	certFile string
	keyFile  string

	jwsKeyURL string

	indexDir      string
	shardDuration time.Duration
	retention     time.Duration
	searchGrace   time.Duration

	grafanaBasicAuthUser string
	grafanaBasicAuthPass string
)

func init() {
	logs.RootCmd.AddCommand(serverCmd)

	serverCmd.Flags().StringVar(&tlsAddr, "tls.addr", "localhost:10000", "Address listen for gRPC TLS on")
	serverCmd.Flags().StringVar(&adminAddr, "admin.addr", "localhost:9999", "Address listen for plain http")

	serverCmd.Flags().StringVar(&caFile, "tls.cacert", "", "Path to root CA")

	serverCmd.Flags().StringVar(&certFile, "tls.cert", "cert.pem", "Path to TLS Cert file")
	serverCmd.Flags().StringVar(&keyFile, "tls.key", "key.pem", "Path TLS Key file")

	serverCmd.Flags().StringVar(&jwsKeyURL, "jws.key-url", "", "URL to retrieve JWS signing key from")

	serverCmd.Flags().StringVar(&indexDir, "index.dir", "data", "Directory to store the index data in")
	serverCmd.Flags().DurationVar(&shardDuration, "index.shard-duration", 15*time.Minute, "Length of eacch index shard")
	serverCmd.Flags().DurationVar(&retention, "index.retention", 0*time.Hour, "Length of eacch index shard, 0 disables pruning")
	serverCmd.Flags().DurationVar(&searchGrace, "index.search-grace", 15*time.Minute, "shards started +/- search grace will be included in searches")

	serverCmd.Flags().StringVar(&grafanaBasicAuthUser, "grafana.user", os.Getenv("GRAFANA_BASICAUTH_USER"), "User for grafana simplejson basic auth")
	serverCmd.Flags().StringVar(&grafanaBasicAuthPass, "grafana.pass", os.Getenv("GRAFANA_BASICAUTH_PASS"), "Password for grafana simplejson basic auth")
}

// rootCmd represents the base command when called without any subcommands
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "the logspray server",
	Long:  `The server can recieves logs, and stores them for later querying`,
	RunE:  run,
}

func run(*cobra.Command, []string) error {
	//runtime.SetBlockProfileRate(100)
	//runtime.SetMutexProfileFraction(100)

	flag.Set("logtostderr", "true")
	flag.Parse()

	ctx := context.Background()

	opts := []grpc.ServerOption{}

	glog.Infof("Loading TLS cert file %v", certFile)
	cert, err := ioutil.ReadFile(certFile)
	if err != nil {
		glog.Fatalf("Unable to read cert file %v", err)
	}

	glog.Infof("Loading TLS key file %v", keyFile)
	key, err := ioutil.ReadFile(keyFile)
	if err != nil {
		glog.Fatalf("Unable to read key file %v", err)
	}

	pair, err := tls.X509KeyPair([]byte(cert), []byte(key))
	if err != nil {
		glog.Fatalf("Unable to create key pair, %v", err)
	}

	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM([]byte(cert))
	if !ok {
		glog.Fatalf("Unable to add cert to ca pool, %v", err)
	}

	if caFile != "" {
		glog.Infof("Loading ca file %v", caFile)
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			glog.Fatalf("Unable to read ca file %v", err)
		}

		ok = certPool.AppendCertsFromPEM([]byte(caCert))
		if !ok {
			glog.Fatalf("Unable to add cert to ca pool, %v", err)
		}

		opts = append(opts,
			grpc.Creds(credentials.NewClientTLSFromCert(certPool, tlsAddr)),
		)
	}

	uis := []grpc.UnaryServerInterceptor{grpc_prometheus.UnaryServerInterceptor}
	sis := []grpc.StreamServerInterceptor{grpc_prometheus.StreamServerInterceptor}

	checkClaims := false
	if jwsKeyURL != "" {
		// Setup JWS Token verification
		cc := certCache{u: jwsKeyURL}
		uis = append(uis, server.MakeAuthUnaryInterceptor(cc.getKeys))
		sis = append(sis, server.MakeAuthStreamingInterceptor(cc.getKeys))
		checkClaims = true
	}

	opts = append(opts,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(uis...)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(sis...)),
	)

	grpcServer := grpc.NewServer(opts...)
	server.RegisterStats()

	conn, err := net.Listen("tcp", tlsAddr)
	if err != nil {
		glog.Fatalf("Unable to list on TLS port, %v", err)
	}

	srv := &http.Server{
		Addr: tlsAddr,
		TLSConfig: &tls.Config{
			Certificates:             []tls.Certificate{pair},
			NextProtos:               []string{"h2"},
			PreferServerCipherSuites: true,
			// Only use curves which have assembly implementations
			CurvePreferences: []tls.CurveID{
				tls.CurveP256,
				tls.X25519,
			},
			MinVersion: tls.VersionTLS12,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			},
		},
		ErrorLog: log.New(&logCleaner{}, "", 0),
	}

	// Setup the gRPC client for the RPC gateway
	// We know this cert parses, we need to get the subject
	pemb, _ := pem.Decode([]byte(cert))
	crt, _ := x509.ParseCertificate(pemb.Bytes)
	dcreds := credentials.NewTLS(&tls.Config{
		ServerName:         string(crt.Subject.CommonName),
		RootCAs:            certPool,
		InsecureSkipVerify: true,
	})

	dopts := []grpc.DialOption{
		grpc.WithTransportCredentials(dcreds),
	}

	var indx *indexer.Indexer
	if indexDir != "" {
		indx, err = indexer.New(
			indexer.WithDataDir(indexDir),
			indexer.WithSharDuration(shardDuration),
			indexer.WithSearchGrace(searchGrace),
			indexer.WithRetention(retention),
		)
		if err != nil {
			glog.Fatalf("Unable to create index, err = %v", err)
		}
		prometheus.MustRegister(indx)
	}

	err = server.Register(
		ctx,
		srv,
		grpcServer,
		dopts,
		server.WithIndex(indx),
		server.WithCheckClaims(checkClaims),
		server.WithGrafanaBasicAuth(grafanaBasicAuthUser, grafanaBasicAuthPass))
	if err != nil {
		glog.Fatalf("Failed to register endpoints, %v", err)
	}

	e := errgroup.Group{}

	e.Go(func() error {
		glog.Infof("grpc at: %s\n", tlsAddr)
		return srv.Serve(tls.NewListener(conn, srv.TLSConfig))
	})

	if adminAddr != "" {
		e.Go(func() error {
			glog.Infof("admin services at at: %s\n", adminAddr)
			server.HandleSwagger(http.DefaultServeMux)
			http.Handle("/metrics", promhttp.Handler())

			return http.ListenAndServe(adminAddr, nil)
		})
	}

	e.Wait()

	return nil
}

type certCache struct {
	u string

	sync.Mutex
	keys map[string]*rsa.PublicKey
}

func (cc *certCache) getKeys() (map[string]*rsa.PublicKey, error) {
	cc.Lock()
	defer cc.Unlock()

	if cc.keys != nil {
		return cc.keys, nil
	}

	res := map[string]*rsa.PublicKey{}

	r, err := http.Get(cc.u)
	if err != nil {
		return nil, errors.Wrap(err, "can't fetch jws keys")
	}

	if r.StatusCode != 200 {
		return nil, fmt.Errorf("JWS PublicKey URL retured %v, %v", r.StatusCode, r.Status)
	}

	keyStrs := map[string]string{}
	dec := json.NewDecoder(r.Body)
	err = dec.Decode(&keyStrs)
	if err != nil {
		return nil, fmt.Errorf("unable to read json jws keys,  %v", err)
	}

	for kid, kstr := range keyStrs {
		if glog.V(2) {
			glog.Infof("JWS public key :\n %s", kstr)
		}

		block, _ := pem.Decode([]byte(kstr))

		jwsKey, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			glog.Warningf("Unable to parse kid %s , jws key %v", kid, err)
			continue
		}

		jwsRSA, ok := jwsKey.(*rsa.PublicKey)
		if !ok {
			glog.Warningf("JWS key must be an RSA public key")
			continue
		}

		res[kid] = jwsRSA
	}

	cc.keys = res
	if glog.V(2) {
		glog.Infof("JWS keys found: %v", cc.keys)
	}

	return res, nil
}

// logCleaner acts as an io.Write for tls.ErrorLog, to strip out
// some noisey errors
type logCleaner struct{}

func (l *logCleaner) Write(p []byte) (int, error) {
	// TODO(tcm): These could be prom stats

	if !glog.V(3) {
		if strings.HasSuffix(string(p), " EOF\n") {
			return len(p), nil
		}

		if strings.HasSuffix(string(p), "tls: first record does not look like a TLS handshake\n") {
			return len(p), nil
		}
	}

	glog.Error(string(p))

	return len(p), nil
}
