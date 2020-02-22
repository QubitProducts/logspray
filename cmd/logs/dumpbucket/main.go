// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dumpbucket

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"text/template"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes"
	"github.com/spf13/cobra"

	"github.com/QubitProducts/logspray/cmd/logs/root"
	"github.com/QubitProducts/logspray/indexer"
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
	offset     uint64
	count      uint64

	follow     bool
	listLabels bool
	search     bool

	outTmpl *template.Template
)

var (
	addr string
)

func init() {
	root.RootCmd.AddCommand(dumpBucketCmd)
}

var dumpBucketCmd = &cobra.Command{
	Use:     "dumpbucket",
	Short:   ``,
	Long:    ``,
	Example: ``,
	RunE:    run,
}

func run(cmd *cobra.Command, args []string) error {
	//ctx := context.Background()
	if len(args) != 1 {
		return errors.New("pass one file name")
	}

	sfi, err := indexer.OpenShardFileIterator(args[0])
	if err != nil {
		return err
	}

	for sfi.Next() {
		fmt.Printf("%d ", sfi.Offset())
		fmt.Printf("%#v\n", sfi.Message())
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

func errRetryable(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.DeadlineExceeded, codes.ResourceExhausted, codes.Unavailable:
		return true
	default:
		return false
	}
}
