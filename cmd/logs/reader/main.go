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

package reader

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/QubitProducts/logspray/proto/logspray"
	promgrpc "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/spf13/cobra"
	yaml "gopkg.in/yaml.v2"

	"github.com/QubitProducts/logspray/cmd/logs/root"
	"github.com/QubitProducts/logspray/relabel"
	"github.com/QubitProducts/logspray/sinks"
	"github.com/QubitProducts/logspray/sinks/devnull"
	"github.com/QubitProducts/logspray/sinks/relabeler"
	"github.com/QubitProducts/logspray/sinks/remote"
	"github.com/QubitProducts/logspray/sources"
	"github.com/QubitProducts/logspray/sources/docker"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	logSprayAddr   string
	srvns          string
	statsAddr      string
	dockerFind     bool
	dockerRoot     string
	dockerPoll     bool
	todevnull      bool
	caFile         string
	insecure       bool
	svcj           string
	oauth2TokenURL string
)

func init() {
	root.RootCmd.AddCommand(readerCmd)

	readerCmd.Flags().StringVar(&logSprayAddr, "server", "localhost:10000", "Address to send logs to")
	readerCmd.Flags().StringVar(&srvns, "srv.ns", "", "Comma separated list of name servers for SRV lookup of server addresses,")
	readerCmd.Flags().StringVar(&statsAddr, "stats.addr", ":9998", "Address to listen for stats on, set to \"\" to disable")
	readerCmd.Flags().BoolVar(&dockerFind, "docker", true, "Whether check for docker container logs")
	readerCmd.Flags().StringVar(&dockerRoot, "docker.root", "", "Path to the docker root, by default it is autodiscovered")
	readerCmd.Flags().BoolVar(&dockerPoll, "docker.poll", false, "poll docker log files rather than inotify")
	readerCmd.Flags().BoolVar(&todevnull, "devnull", false, "Drop all logs, but do the stats")
	readerCmd.Flags().StringVar(&caFile, "tls.ca", "", "Path to root CA")
	readerCmd.Flags().BoolVar(&insecure, "tls.insecure", false, "Turn off transport cert verification")
	readerCmd.Flags().StringVar(&svcj, "service", "", "Google service account file")
	readerCmd.Flags().StringVar(&oauth2TokenURL, "oauth2.token_url", "", "URL for oauth2 tokens")
}

var readerCmd = &cobra.Command{
	Use:   "reader",
	Short: "reader collects logs and publishes them to a server",
	Long: `This is an example log rader, it can collect logs from files
	and docker containers. re-labeling rules can be used to rewrite message
	and decorate them with labels`,
	Run: run,
}

func run(*cobra.Command, []string) {
	flag.Set("logtostderr", "true")
	flag.Parse()
	glog.CopyStandardLogTo("INFO")
	var err error

	if statsAddr != "" {
		http.Handle("/metrics", promhttp.Handler())
		go http.ListenAndServe(statsAddr, nil)
	}

	var outSink sinks.Sinker
	switch {

	case todevnull:
		outSink = &devnull.DevNull{}

	default:
		// Setup gRPC prom statistics
		dopts := []grpc.DialOption{
			grpc.WithUnaryInterceptor(promgrpc.UnaryClientInterceptor),
			grpc.WithStreamInterceptor(promgrpc.StreamClientInterceptor),
		}

		var creds credentials.TransportCredentials
		// Load a ca to verify server cert from
		if caFile != "" {
			cert, err := ioutil.ReadFile(caFile)
			if err != nil {
				log.Fatalf("Unable to read ca file %v", err)
			}

			certPool := x509.NewCertPool()
			certPool.AppendCertsFromPEM([]byte(cert))

			creds = credentials.NewClientTLSFromCert(certPool, logSprayAddr)
		} else {
			creds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: insecure})
		}

		dopts = append(dopts,
			grpc.WithTransportCredentials(creds),
		)

		// Use an explicit DNS server for SRV lookups.
		if srvns != "" {
			res, err := newSRVNameservice(strings.Split(srvns, ","))
			if err != nil {
				glog.Errorf("could not create srv lookup service, %s", err.Error())
				return
			}
			balancer := grpc.RoundRobin(res)
			dopts = append(dopts, grpc.WithBalancer(balancer))
		}

		conn, err := grpc.Dial(
			logSprayAddr,
			dopts...,
		)
		if err != nil {
			glog.Errorf("could not connection to log servce, %s", err.Error())
			return
		}
		defer conn.Close()

		client := logspray.NewLogServiceClient(conn)
		outSink = remote.New(client)
	}

	targetRules := relabel.Config{}
	if err := yaml.Unmarshal([]byte(tcfg), &targetRules); err != nil {
		log.Fatalf("lrules : %v ", err)
	}

	lineRules := relabel.Config{}
	if err := yaml.Unmarshal([]byte(lcfg), &lineRules); err != nil {
		log.Fatalf("lrules : %v ", err)
	}

	rebytes, err := json.Marshal(logRegex.String())
	if err != nil {
		glog.Fatalf("faled to marshal regex to json, %v", err)
	}

	re := &relabel.JSONRegexp{}
	if err := json.Unmarshal(rebytes, re); err != nil {
		glog.Fatalf("cold not compile regex, %v", err)
	}

	lineRules[0].Regex = re

	for {
		func() {
			defer func() {
				if glog.V(1) {
					glog.Infof("restarting in 5 seconds")
					time.Sleep(5 * time.Second)
				}
			}()

			dockerSrc, err := docker.New(
				docker.WithEnvVarWhiteList(
					[]*regexp.Regexp{
						regexp.MustCompile("^MESOS_.+"),
						regexp.MustCompile("^MARATHON_.+"),
						regexp.MustCompile("^CHRONOS_.+"),
					}),
				docker.WithRoot(dockerRoot),
				docker.WithPoll(dockerPoll),
			)
			if err != nil {
				log.Fatal(err)
			}

			relabelSink := relabeler.New(outSink, &targetRules, &lineRules)

			ctx, cancel := context.WithCancel(context.Background())
			err = sources.ReadAllTargets(ctx, relabelSink, dockerSrc)
			if err != nil {
				glog.Errorf("ReadAllTargets exited with err = %v", err)
			}
			cancel()
		}()
	}
}

var tcfg = `
- action: "replace"
  source_labels:
  - container_env_marathon_app_id
  target_label: "job"
  regex: "^/?(.+)$"
- action: "replace"
  source_labels:
  - container_env_chronos_job_name
  target_label: "job"
- action: "replace"
  source_labels:
  - container_label_io.kubernetes.container.name
  target_label: "job"
- action: "replace"
  source_labels:
  - container_env_mesos_task_id
  target_label: "mesos_task_id"
- action: "replace"
  source_labels:
  - container_label_io.kubernetes.pod.name
  target_label: "k8s_pod_name"
- action: "replace"
  source_labels:
  - container_label_io.kubernetes.pod.namespace
  target_label: "k8s_pod_namespace"
- action: "labelmap"
  regex: "^container_env_marathon_app_label_(.+)$"
- action: "labeldrop"
  regex: "^container_env_.+"
- action: "labeldrop"
  regex: "^container_label_.+"
`

// YAML wont accept the control chars, needs to work out a better option here
// (maybe manually unescape?)
var logRegex = regexp.MustCompile("(?i)(\u001b\\[1;32m)?GET(\u001b\\[0m)? /(_haproxy_status|status|admin/healthcheck|metrics)( {})?( HTTP/1.[10]\\\"?)? (\u001b\\[32m)?200")
var lcfg = `
- action: "drop"
  source_labels:
  - __text__
`
