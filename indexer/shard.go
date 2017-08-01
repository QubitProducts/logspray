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
	"encoding/base64"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/ql"
	"github.com/oklog/ulid"
)

// Shard represents a logging session.
type Shard struct {
	id         string
	shardStart time.Time
	dataDir    string
	batchSize  uint

	filesLock sync.Mutex
	files     map[string]*ShardFile
	pbfiles   map[string]*ShardFile

	writeRaw bool
	writePB  bool

	cacheLock  sync.Mutex
	labelCache map[string]map[string]struct{}
}

// ShardFile tracks our offset into a protobuf serialisation.
type ShardFile struct {
	writer io.WriteCloser
	fn     string
	pb     bool
	id     string

	sync.RWMutex
	headersSent bool
	labels      map[string]string
	offset      int64
}

func newShard(startTime time.Time, baseDir, id string, writeRaw, writePB bool) (*Shard, error) {
	dataDir := filepath.Join(baseDir, fmt.Sprintf("%d", startTime.Unix()))
	return &Shard{
		dataDir:    dataDir,
		id:         id,
		shardStart: startTime,
		writePB:    writePB,
		writeRaw:   writeRaw,

		files:   map[string]*ShardFile{},
		pbfiles: map[string]*ShardFile{},

		labelCache: map[string]map[string]struct{}{},
	}, nil
}

func (s *Shard) writeMessage(ctx context.Context, m *logspray.Message, shardKey string, labels map[string]string) error {
	// THere's a horrid mess of locking here that should be tidied
	// up
	s.filesLock.Lock()

	var ok bool
	var rawf *ShardFile
	if s.writeRaw {
		rawf, ok = s.files[m.StreamID]
		if !ok {
			uidStr := ""
			if uid, err := ulid.Parse(m.StreamID); err != nil {
				uidStr = uid.String()
			} else {
				uidStr = base64.StdEncoding.EncodeToString(m.StreamID)
			}

			dir := filepath.Join(s.dataDir, shardKey, s.id)
			rawfn := filepath.Join(dir, fmt.Sprintf("%s.log", uidStr))

			rawf = &ShardFile{
				fn:     rawfn,
				offset: 0,
				pb:     false,
			}
			s.files[m.StreamID] = rawf
		}
	}

	var pbf *ShardFile
	if s.writePB {
		pbf, ok = s.pbfiles[m.StreamID]
		if !ok {
			uidStr := ""
			if uid, err := ulid.Parse(m.StreamID); err != nil {
				uidStr = uid.String()
			} else {
				uidStr = base64.StdEncoding.EncodeToString(m.StreamID)
			}
			dir := filepath.Join(s.dataDir, shardKey, s.id)
			pbfn := filepath.Join(dir, fmt.Sprintf("%s.pb.log", uidStr))

			pbf = &ShardFile{
				fn:     pbfn,
				pb:     true,
				id:     m.StreamID,
				labels: labels,
			}
			s.pbfiles[m.StreamID] = pbf
		}
	}

	/* update labels cache */
	if !ok {
		s.cacheLock.Lock()
		for k, v := range labels {
			_, ok := s.labelCache[k]
			if !ok {
				s.labelCache[k] = map[string]struct{}{}
			}
			s.labelCache[k][v] = struct{}{}
		}
		s.cacheLock.Unlock()
	}

	s.cacheLock.Lock()
	for k, v := range m.Labels {
		_, ok := s.labelCache[k]
		if !ok {
			s.labelCache[k] = map[string]struct{}{}
		}
		s.labelCache[k][v] = struct{}{}
	}
	s.cacheLock.Unlock()

	s.filesLock.Unlock()

	var err error
	if s.writePB {
		err = pbf.writeMessageToFile(ctx, m)
		if err != nil {
			return err
		}
	}

	if s.writeRaw {
		err = rawf.writeMessageToFile(ctx, m)
	}

	return err
}

func (s *Shard) close() {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	s.labelCache = nil
	for fs := range s.files {
		s.files[fs].writer.Close()
	}
	for fs := range s.pbfiles {
		s.pbfiles[fs].writer.Close()
	}
}

// Labels retursn the list of labels in the label cache
func (s *Shard) Labels() []string {
	res := []string{}
	if s == nil {
		return res
	}
	s.cacheLock.Lock()
	for k := range s.labelCache {
		res = append(res, k)
	}
	s.cacheLock.Unlock()
	return res
}

// LabelValues returns all the known values for a given label.
func (s *Shard) LabelValues(name string) []string {
	res := []string{}
	if s == nil {
		return res
	}

	s.cacheLock.Lock()
	for v := range s.labelCache[name] {
		res = append(res, v)
	}
	s.cacheLock.Unlock()
	return res
}

func (s *Shard) findFiles(from, to time.Time) []*ShardFile {
	var fs []*ShardFile
	if s == nil {
		return fs
	}

	s.filesLock.Lock()

	for _, f := range s.pbfiles {
		fs = append(fs, f)
	}

	s.filesLock.Unlock()

	return fs
}

// Search this shard for queries between the provided time frames
// for message matched by the provided match function.
func (s *Shard) Search(ctx context.Context, msgFunc logspray.MessageFunc, matcher ql.MatchFunc, from, to time.Time, count, offset uint64, reverse bool) error {
	if s == nil {
		return nil
	}

	fs := s.findFiles(from, to)
	for _, f := range fs {
		if err := f.Search(ctx, msgFunc, matcher, from, to, count, offset, reverse); err != nil {
			return err
		}
	}
	return nil
}
