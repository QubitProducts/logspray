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

package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/sources"
	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes"
	"github.com/hpcloud/tail"

	"github.com/docker/engine-api/client"
)

var dockerTimeFmt = time.RFC3339Nano

// MessageReader is a reder log source that reads docker logs.
type MessageReader struct {
	id        string
	cli       *client.Client
	fromStart bool
	poll      bool

	path string

	lines chan *logspray.Message
}

// ReadTarget creates a new docker log source
func (w *Watcher) ReadTarget(ctx context.Context, id string, fromStart bool) (sources.MessageReader, error) {
	path := filepath.Join(w.root, "containers", id, id+"-json.log")
	dls := &MessageReader{
		id:    id,
		lines: make(chan *logspray.Message),
		cli:   w.dcli,
		poll:  w.poll,
		path:  path,
	}

	go dls.dockerReadLogs(ctx, fromStart)

	return dls, nil
}

// MessageRead implements the LogSourcer interface
func (dls *MessageReader) MessageRead(ctx context.Context) (*logspray.Message, error) {
	select {
	case m := <-dls.lines:
		if m == nil {
			return nil, io.EOF
		}
		return m, nil
	}
}

func (dls *MessageReader) dockerReadLogs(ctx context.Context, fromStart bool) {
	defer func() {
		dls.logExit()
		close(dls.lines)
	}()

	whence := io.SeekEnd
	if fromStart {
		whence = io.SeekStart
	}
	ft, err := tail.TailFile(dls.path, tail.Config{
		Location:  &tail.SeekInfo{Whence: whence, Offset: 0},
		MustExist: false,
		Follow:    true,
		ReOpen:    true,
		Poll:      dls.poll,
		//		Logger:    logto,
	})
	if err != nil {
		return
	}

	base := &logspray.Message{}
	base.Labels = map[string]string{}

	jsonline := struct {
		Log    string
		Stream string
		Time   string
	}{}

	for {
		select {
		case line := <-ft.Lines:
			if line == nil || line.Err != nil {
				return
			}

			nbase := base.Copy()

			err := json.Unmarshal([]byte(line.Text), &jsonline)
			if err != nil {
				if glog.V(2) {
					glog.Error("failed unmarshaling line, err = ", err)
				}
			}

			if t, err := time.Parse(dockerTimeFmt, jsonline.Time); err == nil {
				nbase.Time, _ = ptypes.TimestampProto(t)
			} else {
				if glog.V(2) {
					glog.Errorf("error parsing docker log time, %v", err)
				}
				nbase.Time, _ = ptypes.TimestampProto(line.Time)
			}

			nbase.Text = strings.TrimSuffix(jsonline.Log, "\n")
			nbase.Labels["source"] = jsonline.Stream

			dls.lines <- nbase
		case <-ctx.Done():
			go ft.Stop()
		}
	}
}

func (dls *MessageReader) logExit() {
	cinfo, _, err := dls.cli.ContainerInspectWithRaw(context.Background(), dls.id, false)
	if err != nil {
		glog.Errorf("error retrieving container exit info,  %v", err)
		return
	}

	pt, _ := ptypes.TimestampProto(time.Now())
	dls.lines <- &logspray.Message{
		Time:   pt,
		Text:   fmt.Sprintf("Container exitted: error = %#v, exitcode = %d", cinfo.State.Error, cinfo.State.ExitCode),
		Labels: map[string]string{},
	}

	switch {
	case cinfo.State.OOMKilled:
		pt, _ := ptypes.TimestampProto(time.Now())
		dls.lines <- &logspray.Message{
			Time:   pt,
			Text:   "Container died due to OOM",
			Labels: map[string]string{},
		}
	}
	return
}
