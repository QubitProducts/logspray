// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

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

	"github.com/QubitProducts/logspray/proto/logspray"
)

// Flags
var (
	optFlags    = flag.NewFlagSet("logclient", flag.ContinueOnError)
	addr        = optFlags.String("addr", "127.0.0.1:5000", "address of logserver")
	cacheToken  = optFlags.Bool("cachetoken", true, "use cached oauth2 token")
	showLabels  = optFlags.Bool("showlabels", false, "show all labels")
	insecure    = optFlags.Bool("tls.insecure", false, "Don't check SSL cert details")
	format      = optFlags.String("fmt", "{{.Text}}", "Go template to use to format each output line")
	grep        = optFlags.String("grep", "", "Regular expression to match the message on the server side")
	grepv       = optFlags.Bool("grep-v", false, "Negate regex match")
	count       = optFlags.Uint64("count", 10, "number of values to return")
	startTime   = cliTime(time.Now().Add(-1 * time.Hour))
	endTime     = cliTime(time.Now())
	actionFlags = flag.NewFlagSet("latonactions", flag.ContinueOnError)
	follow      = actionFlags.Bool("f", false, "follow logs")
	listLabels  = actionFlags.Bool("labels", false, "list labels and label values")
	search      = actionFlags.Bool("search", false, "search for log entries")

	outTmpl *template.Template
)

func init() {
	optFlags.Var(&startTime, "start", "Start of search query time (default to now-1h)")
	optFlags.Var(&endTime, "end", "End of search query time (default to now")
}

type cliTime time.Time

func (t *cliTime) Get() interface{} {
	return *t
}

func (t *cliTime) String() string {
	return time.Time(*t).String()
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

func main() {
	actionFlags.VisitAll(func(f *flag.Flag) {
		flag.Var(f.Value, f.Name, f.Usage)
	})
	optFlags.VisitAll(func(f *flag.Flag) {
		flag.Var(f.Value, f.Name, f.Usage)
	})
	flag.Usage = usage
	flag.Parse()

	ctx := context.Background()

	dopts := []grpc.DialOption{}

	var creds credentials.TransportCredentials
	if *insecure {
		creds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	} else {
		certPool := x509.NewCertPool()
		creds = credentials.NewTLS(&tls.Config{
			RootCAs: certPool,
		})
	}
	dopts = append(dopts, grpc.WithTransportCredentials(creds))

	conn, err := grpc.Dial(
		*addr,
		dopts...,
	)
	if err != nil {
		log.Fatalf("could not connection to log servce, %s", err.Error())
	}
	defer conn.Close()
	client := logspray.NewLogServiceClient(conn)

	outTmpl, err = template.New("out").Funcs(sprig.TxtFuncMap()).Parse(*format + "\n")
	if err != nil {
		fatalf("failed to compile output template, %v", err)
	}

	if *grep != "" {
		_, err := regexp.Compile(*grep)
		if err != nil {
			fatalf("could not compile grep regex, %v", err)
		}
	}

	switch {
	case *follow:
		doFollow(ctx, client)
	case *listLabels:
		doListLabels(ctx, client)
	case *search:
		doSearch(ctx, client)
	default:
		fatalf("must pick an action")
	}
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

func usage() {
	fmt.Fprintf(os.Stderr,
		`Usage: laton [label:value ...]

`)
	fmt.Fprintln(os.Stderr, "Actions")
	actionFlags.PrintDefaults()
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Options")
	optFlags.PrintDefaults()
	os.Exit(2)
}
