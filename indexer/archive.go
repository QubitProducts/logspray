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
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"golang.org/x/crypto/openpgp"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/ql"
	"github.com/golang/glog"
	"github.com/oklog/ulid"

	"github.com/graymeta/stow"
	//_ "github.com/graymeta/stow/google"
	//_ "github.com/graymeta/stow/local"
	//_ "github.com/graymeta/stow/s3"
)

type shardArchive struct {
	dataDir     string
	stowConfig  stow.ConfigMap
	retention   time.Duration
	encryptTo   []openpgp.Entity
	gzipLevel   int
	searchGrace time.Duration

	sync.RWMutex
	history      map[time.Time][]*Shard
	historyOrder []time.Time
}

type ArchiveOpt func(a *shardArchive) (*shardArchive, error)

func NewArchive(opts ...ArchiveOpt) (*shardArchive, error) {
	var err error
	a := &shardArchive{
		dataDir:     "data",
		history:     map[time.Time][]*Shard{},
		searchGrace: time.Minute * 15,
	}
	for _, o := range opts {
		a, err = o(a)
		if err != nil {
			return nil, err
		}
	}

	var shards []*Shard
	// Search the dataDir and find all previous shards.
	filepath.Walk(a.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			glog.Errorf("failed walking archive datadir, %v", err)
			return err
		}

		if !info.IsDir() || a.dataDir == path {
			return nil
		}

		uid, err := ulid.Parse(filepath.Base(path))
		if err != nil {
			glog.Errorf("failed walking archive dir, %v", err)
			return filepath.SkipDir
		}

		t := time.Unix(0, int64(uint64(time.Millisecond)*uid.Time()))
		shards = append(shards, &Shard{
			id:         uid.String(),
			shardStart: t,
			dataDir:    path,
		})
		return filepath.SkipDir
	})

	a.Add(shards...)

	return a, nil
}

func WithArchiveDataDir(datadir string) ArchiveOpt {
	return func(a *shardArchive) (*shardArchive, error) {
		a.dataDir = datadir
		return a, nil
	}
}

func WithArchiveSearchGrace(grace time.Duration) ArchiveOpt {
	return func(a *shardArchive) (*shardArchive, error) {
		a.searchGrace = grace
		return a, nil
	}
}

func WithArchiveStowConfig(scfg stow.ConfigMap) ArchiveOpt {
	return func(a *shardArchive) (*shardArchive, error) {
		a.stowConfig = scfg
		return a, nil
	}
}

func WithArchiveRetention(d time.Duration) ArchiveOpt {
	return func(a *shardArchive) (*shardArchive, error) {
		a.retention = d
		return a, nil
	}
}

func WithArchiveEncryptTo(ent []openpgp.Entity) ArchiveOpt {
	return func(a *shardArchive) (*shardArchive, error) {
		a.encryptTo = ent
		return a, nil
	}
}

func WithArchiveGzipCompression(level int) ArchiveOpt {
	return func(a *shardArchive) (*shardArchive, error) {
		a.gzipLevel = level
		return a, nil
	}
}

// Add moves the files from an active shard into the archive.
func (sa *shardArchive) Add(shards ...*Shard) {
	sa.Lock()
	defer sa.Unlock()

	for _, s := range shards {
		glog.V(2).Infof("Adding shard %v to archive history (%v) ", s.id, s.shardStart)
		if _, ok := sa.history[s.shardStart]; !ok {
			sa.history[s.shardStart] = nil
			sa.historyOrder = append(sa.historyOrder, s.shardStart)
		}
		sa.history[s.shardStart] = append(sa.history[s.shardStart], s)
	}
	sort.Slice(sa.historyOrder, func(i, j int) bool { return sa.historyOrder[i].Before(sa.historyOrder[j]) })
}

func (sa *shardArchive) findShards(from, to time.Time) []shardSet {
	sa.RLock()
	defer sa.RUnlock()

	var qs []shardSet

	//for i := len(sa.historyOrder) - 1; i >= 0; i-- {
	for i := 0; i < len(sa.historyOrder); i++ {
		t := sa.historyOrder[i]
		if t.Before(from.Add(-1 * sa.searchGrace)) {
			continue
		}
		qs = append(qs, sa.history[t])

		if t.After(to.Add(sa.searchGrace)) {
			break
		}
	}

	return qs
}

func (sa *shardArchive) Search(ctx context.Context, msgFunc logspray.MessageFunc, matcher ql.MatchFunc, from, to time.Time, reverse bool) error {
	foundShardSets := sa.findShards(from, to)
	glog.V(2).Infof("searching %v archived shards", len(foundShardSets))

	for _, shardSet := range foundShardSets {
		for _, ss := range shardSet {
			if err := ss.Search(ctx, msgFunc, matcher, from, to, reverse); err != nil {
				return err
			}
		}
	}

	return nil
}
