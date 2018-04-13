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
	"github.com/golang/glog"
	"github.com/oklog/ulid"

	"github.com/graymeta/stow"
	_ "github.com/graymeta/stow/google"
	_ "github.com/graymeta/stow/local"
	_ "github.com/graymeta/stow/s3"
)

type shardArchive struct {
	dataDir    string
	stowConfig stow.ConfigMap
	retention  time.Duration
	encryptTo  []openpgp.Entity
	gzipLevel  int

	sync.RWMutex
	history      map[time.Time][]*Shard
	historyOrder []time.Time
}

type ArchiveOpt func(a *shardArchive) (*shardArchive, error)

func NewArchive(opts ...ArchiveOpt) (*shardArchive, error) {
	var err error
	a := &shardArchive{
		dataDir: "data",
		history: map[time.Time][]*Shard{},
	}
	for _, o := range opts {
		a, err = o(a)
		if err != nil {
			return nil, err
		}
	}

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

		glog.V(2).Infof("Adding %v to archive history", path)
		t := time.Unix(0, int64(uid.Time()))
		a.history[t] = append(a.history[t], &Shard{
			shardStart: t,
			dataDir:    path,
		})
		return filepath.SkipDir
	})

	var ts []time.Time
	for k := range a.history {
		ts = append(ts, k)
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
	a.historyOrder = ts

	return a, nil
}

func WithArchiveDataDir(datadir string) ArchiveOpt {
	return func(a *shardArchive) (*shardArchive, error) {
		a.dataDir = datadir
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
func (sa *shardArchive) Add(s *Shard) {
	sa.Lock()
	defer sa.Unlock()

	if _, ok := sa.history[s.shardStart]; !ok {
		sa.history[s.shardStart] = nil
	}
	sa.history[s.shardStart] = append(sa.history[s.shardStart], s)

	var ts []time.Time
	for k := range sa.history {
		ts = append(ts, k)
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
	sa.historyOrder = ts
}

func (sa *shardArchive) findShards(from, to time.Time) [][]*Shard {
	sa.RLock()
	defer sa.RUnlock()

	var qs [][]*Shard

	for i := len(sa.historyOrder) - 1; i >= 0; i-- {
		t := sa.historyOrder[i]
		if !t.Before(to) {
			continue
		}
		qs = append(qs, sa.history[t])

		if !t.After(from) {
			break
		}
	}

	return qs
}

func (sa *shardArchive) searchShards(ctx context.Context, query string, from, to time.Time, count, offset int) ([]*logspray.Message, uint64, error) {
	foundShardSets := sa.findShards(from, to)

	var hitCount uint64
	var msgs []*logspray.Message
	for _, shardSet := range foundShardSets {
		for _, ss := range shardSet {
			if to.After(ss.shardStart) {
				//Relevant message
			}
			if !from.Before(ss.shardStart) {
				break
			}
		}
	}

	return msgs, hitCount, nil
}
