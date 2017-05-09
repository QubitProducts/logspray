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

package stdout

import (
	"context"
	"fmt"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/sinks"
)

// Stdout is a sink.Sinker that wites streams to stdout.
type Stdout struct {
}

// AddSource implements sink.Sinker
func (o *Stdout) AddSource(ctx context.Context, id string, Labels map[string]string) (sinks.MessageWriter, error) {
	fmt.Printf("New Stream %#v: %v\n", id, Labels)
	return &MessageWriter{}, nil
}

// MessageWriter implements a message writer for the Stdout Sink
type MessageWriter struct {
}

// WriteMessage writes a message to the stdout sink.
func (o *MessageWriter) WriteMessage(m *logspray.Message) error {
	fmt.Printf("Msg: %#v\n", *m)
	return nil
}

// Close closes the Srdout sink
func (o *MessageWriter) Close() error {
	return nil
}

/*
func thing() {
	lastSigLabels := map[string]string{}
	for l := range siglabels {
		lastSigLabels[l] = ""
	}
	newSigLabels := map[string]string{}
	for {
		m, err := cr.Recv()
		if err != nil {
			fmt.Fprint(os.Stderr, err.Error())
			return
		}

		switch m.ControlMessage {
		case logspray.Message_OK:
			continue
		case logspray.Message_ERROR:
			fmt.Fprintf(os.Stderr, "-- ERROR %s\n", m.Text)
			continue
		}

		newlabels := false
		for lsk, lsv := range lastSigLabels {
			if mv, ok := m.Labels[lsk]; ok {
				if mv != lsv {
					newlabels = true
				}
				newSigLabels[lsk] = mv
			}
		}
		if newlabels {
			fmt.Fprintf(os.Stderr, "--")
			if !*showLabels {
				for k, v := range newSigLabels {
					fmt.Fprintf(os.Stderr, " %s=%s", k, v)
				}
			} else {
				for k, v := range m.Labels {
					fmt.Fprintf(os.Stderr, " %s=%s", k, v)
				}
			}
			fmt.Fprintf(os.Stderr, "\n")
			lastSigLabels = newSigLabels
		}
		outputMessage(m)
		if m == nil {
			break
		}
	}
}
*/
