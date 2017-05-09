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

package sources

import (
	"context"

	"github.com/QubitProducts/logspray/proto/logspray"
)

// Sourcer watches some arbitrary potential source of multiple log streams.
// Each time the Updater spots a new stream, ReadTarget is called to read
// message from that stream.
type Sourcer interface {
	// Updater implementation required to find new streams.
	Updater

	// ReadTarget is called in response to a new target being found by the updater
	// The supplied context will be cancel'ed when the updater sees the source go
	// away. FromStart indicates whther you should read the stream from the
	// beginning, or the current position.
	ReadTarget(ctx context.Context, id string, FromStart bool) (MessageReader, error)
}

// MessageReader is used to Read a message from a source.
type MessageReader interface {
	MessageRead(ctx context.Context) (*logspray.Message, error)
}

// Updaer is used to watch for changes to a set of potential log sources. The
// furst call to Next should return any pre-existing log sources. Subsequent
// calls should describe changes to that set.
type Updater interface {
	Next(context.Context) ([]*Update, error)
}
