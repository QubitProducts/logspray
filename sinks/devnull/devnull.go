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

package devnull

import (
	"context"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/sinks"
)

// DevNull is sinks.Sink that drop all traffic
type DevNull struct {
}

// AddSource adds a new source to the sink
func (o *DevNull) AddSource(id string, Labels map[string]string) (sinks.MessageWriter, error) {
	return &MessageWriter{}, nil
}

// MessageWriter is sinks.MessageWriter that drop all traffic
type MessageWriter struct {
}

// WriteMessage writes a message to devnull
func (o *MessageWriter) WriteMessage(ctx context.Context, m *logspray.Message) error {
	return nil
}

// Close closes the DevNull writer
func (o *MessageWriter) Close() error {
	return nil
}
