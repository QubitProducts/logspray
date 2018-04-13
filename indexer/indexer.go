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

package indexer

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/ql"
	"github.com/QubitProducts/logspray/sinks"
	"github.com/oklog/ulid"
)

// Indexer implements a queryable index for storage of logspray
// mssages.
type Indexer struct {
	shardDuration time.Duration
	dataDir       string

	id string

	sync.RWMutex
	activeShard *Shard
	archive     *shardArchive
}

// Opt defines an index option function.
type Opt func(i *Indexer) error

// MessageWriter is a sinks.MessageWriter that writes to a remote server
type MessageWriter struct {
	indx     *Indexer
	streamID string
	labels   map[string]string
}

// New creates a new index.
func New(opts ...Opt) (*Indexer, error) {
	t := time.Now()
	entropy := rand.New(rand.NewSource(t.UnixNano()))
	id := ulid.MustNew(ulid.Timestamp(t), entropy).String()

	indx := &Indexer{
		id:            id,
		shardDuration: time.Minute * 1,
		dataDir:       "data",
	}

	for _, o := range opts {
		if err := o(indx); err != nil {
			return nil, err
		}
	}

	arch, err := NewArchive(WithArchiveDataDir(indx.dataDir))
	if err != nil {
		return nil, err
	}

	indx.archive = arch

	return indx, nil
}

// WithSharDuration allows you to set the time duration of a shard.
func WithSharDuration(d time.Duration) Opt {
	return func(i *Indexer) error {
		i.shardDuration = d
		return nil
	}
}

// WithDataDir lets you set the base filesystem path to store
// data to.
func WithDataDir(d string) Opt {
	return func(i *Indexer) error {
		i.dataDir = d
		return nil
	}
}

// Close this index
func (idx *Indexer) Close() error {
	return nil
}

// AddSource adds a new source to the remote server
func (idx *Indexer) AddSource(ctx context.Context, id string, labels map[string]string) (sinks.MessageWriter, error) {
	return &MessageWriter{
		indx:   idx,
		labels: labels,
	}, nil
}

// WriteMessage writes a message to the log stream.
func (w *MessageWriter) WriteMessage(ctx context.Context, m *logspray.Message) error {
	var err error
	shardStart := time.Now().Truncate(w.indx.shardDuration)

	w.indx.RLock()
	if w.indx.activeShard == nil || shardStart.After(w.indx.activeShard.shardStart) {
		oldShard := w.indx.activeShard
		w.indx.RUnlock()

		w.indx.Lock()
		w.indx.activeShard, err = newShard(
			shardStart,
			w.indx.dataDir,
			w.indx.id,
		)
		if err != nil {
			w.indx.Unlock()
			return err
		}

		w.indx.Unlock()
		w.indx.RLock()

		if oldShard != nil {
			go func() {
				oldShard.close()
				w.indx.archive.Add(oldShard)
			}()
		}
	}
	shard := w.indx.activeShard

	w.indx.RUnlock()

	return shard.writeMessage(ctx, m, w.labels)
}

// Close closes the remote stream.
func (w *MessageWriter) Close() error {
	return nil
}

// Labels lists all the label names in the current index
func (idx *Indexer) Labels(from, to time.Time) ([]string, error) {
	idx.RLock()
	s := idx.activeShard
	idx.RUnlock()

	res := s.Labels()

	return res, nil
}

// LabelValues returns all the known values for a given label.
func (idx *Indexer) LabelValues(name string, from, to time.Time, count int) ([]string, int, error) {
	idx.RLock()
	s := idx.activeShard
	idx.RUnlock()

	res := s.LabelValues(name)

	return res, len(res), nil
}

// Search queries the index for documents matching the provided
// search query.
func (idx *Indexer) Search(ctx context.Context, msgFunc logspray.MessageFunc, matcher ql.MatchFunc, from, to time.Time, count, offset uint64, reverse bool) error {
	if to.Before(from) {
		return fmt.Errorf("time to must be after time from")
	}
	idx.RLock()
	s := idx.activeShard
	idx.RUnlock()

	return s.Search(ctx, msgFunc, matcher, from, to, count, offset, reverse)
}
