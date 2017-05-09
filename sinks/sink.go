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

package sinks

import (
	"context"

	"github.com/QubitProducts/logspray/proto/logspray"
)

// A Sinker accepts new sources of messages.
type Sinker interface {
	// AddSource should return a MessageWriter that log messages can then be
	// written to. All Messages written to the returned writer are associated with
	// the StreamID given. The Labels passed here are sent once at the start of a
	// stream and are associated with every message written to the MessageWriter.
	// StreamID should be a UUID that is uniq to this call to AddSource
	AddSource(StreamID string, Labels map[string]string) (MessageWriter, error)
}

// A MessageWriter is used ot add new messages to an existing log stream. Each
// message should have a unique numerical Index within the stream.
type MessageWriter interface {
	WriteMessage(ctx context.Context, msg *logspray.Message) error
	Close() error
}
