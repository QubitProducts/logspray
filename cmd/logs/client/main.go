// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package client

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"text/template"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/Masterminds/sprig"
	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes"
	"github.com/spf13/cobra"

	"github.com/QubitProducts/logspray/cmd/logs/root"
	"github.com/QubitProducts/logspray/proto/logspray"
)

// Flags
var (
	cacheToken bool
	showLabels bool
	insecure   bool
	format     string
	grep       string
	grepv      bool
	count      uint64
	startTime  = cliTime(time.Now().Add(-1 * time.Hour))
	endTime    = cliTime(time.Now())

	follow     bool
	listLabels bool
	search     bool

	outTmpl *template.Template
)

var (
	addr string
)

func init() {
	root.RootCmd.AddCommand(clientCmd)

	clientCmd.PersistentFlags().StringVar(&addr, "addr", "127.0.0.1:10000", "address of logserver")

	clientCmd.Flags().Var(&startTime, "start", "Start of search query time (default to now-1h)")
	clientCmd.Flags().Var(&endTime, "end", "End of search query time (default to now")

	clientCmd.Flags().BoolVar(&cacheToken, "cachetoken", true, "use cached oauth2 token")
	clientCmd.Flags().BoolVar(&showLabels, "showlabels", false, "show all labels")
	clientCmd.Flags().BoolVar(&insecure, "tls.insecure", false, "Don't check SSL cert details")
	clientCmd.Flags().StringVar(&format, "fmt", "{{.Text}}", "Go template to use to format each output line")
	clientCmd.Flags().StringVar(&grep, "grep", "", "Regular expression to match the message on the server side")
	clientCmd.Flags().BoolVar(&grepv, "grep-v", false, "Negate regex match")
	clientCmd.Flags().Uint64Var(&count, "count", 10, "number of values to return")

	clientCmd.Flags().BoolVarP(&follow, "follow", "f", false, "follow logs")
	clientCmd.Flags().BoolVar(&listLabels, "labels", false, "list labels and label values")
	clientCmd.Flags().BoolVar(&search, "search", false, "search for log entries")
}

var clientCmd = &cobra.Command{
	Use:     "client",
	Short:   `client can be used for streaming and searching logs`,
	Long:    `client is the primary tool for querying a logspray cluster.`,
	Example: `logs client -f job=myjob dc~"europe-[0-9]"`,
	RunE:    run,
}

type cliTime time.Time

func (t *cliTime) Get() interface{} {
	return *t
}

func (t *cliTime) String() string {
	return time.Time(*t).String()
}

func (t *cliTime) Type() string {
	return "time"
}

func (t *cliTime) Set(s string) error {
	switch {
	case s == "now":
		*t = cliTime(time.Now())
		return nil
	case strings.HasPrefix(s, "now-"):
		d, err := time.ParseDuration(s[4:])
		*t = cliTime(time.Now().Add(-1 * d))
		return err
	case strings.HasPrefix(s, "now+"):
		d, err := time.ParseDuration(s[4:])
		*t = cliTime(time.Now().Add(d))
		return err
	default:
		pt, err := time.Parse(time.RFC3339Nano, s)
		*t = cliTime(pt)
		return err
	}
}

func run(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	dopts := []grpc.DialOption{}

	var creds credentials.TransportCredentials
	if insecure {
		creds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	} else {
		certPool := x509.NewCertPool()
		creds = credentials.NewTLS(&tls.Config{
			RootCAs: certPool,
		})
	}
	dopts = append(dopts, grpc.WithTransportCredentials(creds))

	conn, err := grpc.Dial(
		addr,
		dopts...,
	)
	if err != nil {
		log.Fatalf("could not connection to log servce, %s", err.Error())
	}
	defer conn.Close()
	client := logspray.NewLogServiceClient(conn)

	outTmpl, err = template.New("out").Funcs(sprig.TxtFuncMap()).Parse(format + "\n")
	if err != nil {
		fatalf("failed to compile output template, %v", err)
	}

	if grep != "" {
		_, err := regexp.Compile(grep)
		if err != nil {
			fatalf("could not compile grep regex, %v", err)
		}
	}

	switch {
	case follow:
		doFollow(ctx, client, args)
	case listLabels:
		doListLabels(ctx, client, args)
	case search:
		doSearch(ctx, client, args)
	default:
		fatalf("must pick an action")
	}

	return nil
}

func outputMessage(m *logspray.Message) {
	if glog.V(2) {
		glog.Infof("Raw: %#v\n", *m)
	}
	tm := map[string]interface{}{}
	tm["Text"] = m.Text
	mtime, _ := ptypes.Timestamp(m.Time)
	tm["Time"] = mtime
	tm["Labels"] = m.Labels
	tm["ID"], _ = m.ID()

	if err := outTmpl.ExecuteTemplate(os.Stdout, "out", tm); err != nil {
		fatalf("Got error executing template, %v", err)
	}
}

func fatalf(str string, vs ...interface{}) {
	fmt.Fprintf(os.Stderr, str, vs...)
	os.Exit(1)
}

func usageFatalf(str string, vs ...interface{}) {
	fmt.Fprintf(os.Stderr, str, vs...)
	flag.Usage()
	os.Exit(1)
}
