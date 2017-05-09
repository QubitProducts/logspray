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

package filesystem

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/QubitProducts/logspray/sources"
	"github.com/golang/glog"
	"github.com/rjeczalik/notify"
)

// New creates a new filesystem log source
func New(Path string, NameRegexp *regexp.Regexp, Recur bool) *Watcher {
	return &Watcher{
		Path:       Path,
		Recur:      Recur,
		NameRegexp: NameRegexp,
	}
}

// Watcher watches for files being added and removed from a filesystem
type Watcher struct {
	Path       string
	NameRegexp *regexp.Regexp
	Recur      bool //recur into directories

	ups chan []*sources.Update
}

func (fs *Watcher) String() string {
	str := fs.Path
	if fs.Recur {
		str += "/..."
	}
	if fs.NameRegexp != nil {
		str += fmt.Sprintf("(%s)", fs.NameRegexp)
	}
	return str
}

// Next should be called each time you wish to watch for an update.
func (fs *Watcher) Next(ctx context.Context) ([]*sources.Update, error) {
	if fs.ups == nil {
		fs.ups = make(chan []*sources.Update, 1)
		initFiles := []*sources.Update{}
		filepath.Walk(fs.Path, func(path string, info os.FileInfo, err error) error {
			if info == nil {
				return nil
			}
			if info.IsDir() && fs.Path != path && !fs.Recur {
				return filepath.SkipDir
			}
			if info.IsDir() {
				return nil
			}
			if fs.NameRegexp != nil {
				if !fs.NameRegexp.MatchString(info.Name()) {
					return nil
				}
			}
			initFiles = append(initFiles, &sources.Update{Action: sources.Add, Target: path})
			return nil
		})

		fs.ups <- initFiles
		if err := fs.watch(ctx); err != nil {
			return nil, err
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case up := <-fs.ups:
		return up, nil
	}
}

const fsevs = notify.Create | notify.Remove | notify.Rename | notify.InDeleteSelf | notify.InMoveSelf

func (fs *Watcher) watch(ctx context.Context) error {
	// Watcher doesn't wait, but also doesn't inform us if we miss
	// events
	ec := make(chan notify.EventInfo, 100)

	path := fs.Path
	if fs.Recur {
		path = filepath.Join(path, "...")
	}

	err := notify.Watch(path, ec, fsevs)
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			notify.Stop(ec)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case e := <-ec:
				go func(e notify.EventInfo) {
					afn, err := filepath.Abs(e.Path())
					if err != nil {
						// probbaly need to log here
						return
					}

					fn := filepath.Base(afn)
					if fs.NameRegexp != nil {
						if !fs.NameRegexp.MatchString(fn) {
							return
						}
					}

					if e.Path() == afn {
						labels := map[string]string{
							"filename": afn,
						}
						switch e.Event() {

						case notify.Create:
							fs.ups <- []*sources.Update{
								{
									Action: sources.Add,
									Target: afn,
									Labels: labels,
								},
							}
						case notify.Remove:
							fs.ups <- []*sources.Update{
								{
									Action: sources.Remove,
									Target: afn,
									Labels: labels,
								},
							}
						case notify.InDeleteSelf:
							fs.ups <- []*sources.Update{
								{
									Action: sources.Remove,
									Target: afn,
									Labels: labels,
								},
							}
						case notify.Rename, notify.InMoveSelf:
							fs.ups <- []*sources.Update{
								{
									Action: sources.Remove,
									Target: afn,
									Labels: labels,
								},
							}
						default:
							glog.Infof("Ignoring $s event on %s", e.Event(), e.Path())
						}
					}
				}(e)
			}
		}
	}()
	return nil
}
