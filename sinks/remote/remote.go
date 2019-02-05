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

package remote

import (
	"context"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/sinks"
	"github.com/cloudflare/backoff"
	"github.com/golang/glog"
)

// Remote is a sinks.Sink impllementation that sends messages to a remote
// logspray gRPC server
type Remote struct {
	client logspray.LogServiceClient
}

// New creates a new sink that send via the provided client
func New(client logspray.LogServiceClient) *Remote {
	r := &Remote{
		client: client,
	}

	return r
}

// MessageWriter is a sinks.MessageWriter that writes to a remote server
type MessageWriter struct {
	*Remote
	header     *logspray.Message
	headerSent bool
	strc       logspray.LogService_LogStreamClient
}

// AddSource adds a new source ot the remote server
func (r *Remote) AddSource(id string, labels map[string]string) (sinks.MessageWriter, error) {
	return &MessageWriter{
		strc:   nil,
		Remote: r,
		header: &logspray.Message{
			Labels:         labels,
			StreamID:       id,
			ControlMessage: logspray.Message_SETHEADER,
		},
		headerSent: false,
	}, nil
}

// WriteMessage writes a message to the log stream.
func (r *MessageWriter) WriteMessage(ctx context.Context, m *logspray.Message) error {
	b := backoff.New(10*time.Second, 1*time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if r.strc == nil {
			strc, err := r.client.LogStream(ctx)
			if err != nil {
				if glog.V(1) {
					glog.Info("failed opening log stream, %v", err)
				}
				r.headerSent = false
				<-time.After(b.Duration())
				continue
			}
			r.strc = strc
		}

		if !r.headerSent {
			if err := r.strc.Send(r.header); err != nil {
				if glog.V(1) {
					glog.Info("failed sending header, %v", err)
				}
				r.strc = nil
				r.headerSent = false
				<-time.After(b.Duration())
				continue
			}
			r.headerSent = true
		}

		if err := r.strc.Send(m); err != nil {
			if glog.V(1) {
				glog.Info("failed sending message, %v", err)
			}
			r.strc = nil
			r.headerSent = false
			<-time.After(b.Duration())
			continue
		}
		break
	}

	return nil
}

// Close closes the remote stream.
func (r *MessageWriter) Close() error {
	if r.strc == nil {
		return nil
	}

	_, err := r.strc.CloseAndRecv()
	return err
}
