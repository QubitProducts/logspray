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
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/ql"
	"github.com/golang/glog"
	"github.com/oklog/ulid"
)

// Shard represents a logging session.
type Shard struct {
	indexId    string
	id         string
	shardStart time.Time
	dataDir    string

	filesLock sync.Mutex
	files     map[string]*ShardFile

	cacheLock  sync.Mutex
	labelCache map[string]map[string]struct{}
}

func newShard(startTime time.Time, baseDir, indexId string) (*Shard, error) {
	t := time.Now()
	entropy := rand.New(rand.NewSource(t.UnixNano()))
	id := ulid.MustNew(ulid.Timestamp(t), entropy).String()

	return &Shard{
		dataDir:    baseDir,
		indexId:    indexId,
		id:         id,
		shardStart: startTime,

		files: map[string]*ShardFile{},

		labelCache: map[string]map[string]struct{}{},
	}, nil
}

func (s *Shard) writeMessage(ctx context.Context, m *logspray.Message, labels map[string]string) error {
	// THere's a horrid mess of locking here that should be tidied
	// up
	s.filesLock.Lock()

	var ok bool
	var pbf *ShardFile
	pbf, ok = s.files[m.StreamID]
	if !ok {
		uidStr := ""
		if uid, err := ulid.Parse(m.StreamID); err == nil {
			uidStr = uid.String()
		} else {
			uidStr = base64.StdEncoding.EncodeToString([]byte(m.StreamID))
		}
		dir := filepath.Join(s.dataDir, s.id)
		pbfn := filepath.Join(dir, fmt.Sprintf("%s.pb.log", uidStr))

		pbf = &ShardFile{
			fn:     pbfn,
			id:     m.StreamID,
			labels: labels,
		}
		s.files[m.StreamID] = pbf
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
	err = pbf.writeMessageToFile(ctx, m)
	if err != nil {
		return err
	}

	return err
}

func (s *Shard) close() {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	s.labelCache = nil
	for fs := range s.files {
		s.files[fs].Close()
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
	defer s.filesLock.Unlock()

	if s.files == nil {
		s.files = make(map[string]*ShardFile)
		filepath.Walk(s.dataDir, func(path string, info os.FileInfo, err error) error {
			if path == s.dataDir {
				return nil
			}
			if err != nil {
				return err
			}
			if info.IsDir() {
				return filepath.SkipDir
			}

			if !strings.HasSuffix(path, ".pb.log") {
				return nil
			}
			fn := filepath.Base(path)
			streamID := fn[:len(fn)-7]
			_, err = ulid.Parse(streamID)
			if err != nil {
				glog.Infof("skipping shardFile, err = %v", err)
				return nil
			}

			s.files[streamID] = &ShardFile{id: streamID, fn: path}

			return nil
		})
	}

	for _, f := range s.files {
		fs = append(fs, f)
	}
	sort.Sort(ShardFileByStartTime(fs))

	return fs
}

type ShardFileByStartTime []*ShardFile

func (so ShardFileByStartTime) Len() int           { return len(so) }
func (so ShardFileByStartTime) Swap(i, j int)      { so[i], so[j] = so[j], so[i] }
func (so ShardFileByStartTime) Less(i, j int) bool { return so[i].fn < so[j].fn }

// Search this shard for queries between the provided time frames
// for message matched by the provided match function.
func (s *Shard) Search(ctx context.Context, msgFunc logspray.MessageFunc, matcher ql.MatchFunc, from, to time.Time, reverse bool) error {
	if s == nil {
		return nil
	}

	fs := s.findFiles(from, to)
	for _, f := range fs {
		if err := f.Search(ctx, msgFunc, matcher, from, to, reverse); err != nil {
			return err
		}
	}
	return nil
}

type shardSet []*Shard

func (ss shardSet) Search(ctx context.Context, msgFunc logspray.MessageFunc, matcher ql.MatchFunc, from, to time.Time, count, offset uint64, reverse bool) error {
	for _, s := range ss {
		if err := s.Search(ctx, msgFunc, matcher, from, to, reverse); err != nil {
			return err
		}
	}
	return nil
}
