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

package filesystem

import (
	"context"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/sources"
	"github.com/hpcloud/tail"
)

// MessageReader is used to tail a file path and create log data from
// the lines.
type MessageReader struct {
	*tail.Tail

	labels map[string]string
}

func (w *Watcher) ReadTarget(ctx context.Context, fn string, fromStart bool) (sources.MessageReader, error) {
	ft, err := tail.TailFile(fn, tail.Config{
		Location:  &tail.SeekInfo{Whence: 0, Offset: 0},
		MustExist: false,
		Follow:    true,
		ReOpen:    true,
		Poll:      w.Poll,
		//		Logger:    logto,
	})

	labels := map[string]string{"filename": fn}

	return &MessageReader{Tail: ft, labels: labels}, err
}

// MessageRead implements the LogSourcer interface
func (fs *MessageReader) MessageRead(ctx context.Context) (*logspray.Message, error) {
	m := logspray.Message{
		Labels: fs.labels,
	}
	err := fs.MessageWriteTo(ctx, &m)
	return &m, err
}

// MessageWriteTo implements the LogSourcer interface
func (fs *MessageReader) MessageWriteTo(ctx context.Context, tm *logspray.Message) error {
	select {
	case l := <-fs.Lines:
		if l.Err != nil {
			return l.Err
		}
		tm.Text = l.Text
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
