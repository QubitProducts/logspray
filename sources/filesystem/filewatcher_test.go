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
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/QubitProducts/logspray/sources"
)

func TestFileSystemWatcher_Next1(t *testing.T) {
	fsts := []struct {
		fstest
		watcher *Watcher
		expect  []map[string]sources.Update
	}{
		{
			fstest: fstest{
				preactions: []taction{},
			},
			watcher: &Watcher{},
			expect:  []map[string]sources.Update{{}},
		},
		{
			fstest: fstest{
				preactions: []taction{
					createFile("file1"),
				},
			},
			watcher: &Watcher{},
			expect: []map[string]sources.Update{
				{
					"file1": sources.Update{Action: sources.Add},
				},
			},
		},
		{
			fstest: fstest{
				subdirs: []string{"dir1"},
				preactions: []taction{
					createFile("file1"),
					createFile("dir1/ignored"),
				},
			},
			watcher: &Watcher{},
			expect: []map[string]sources.Update{
				{
					"file1": sources.Update{Action: sources.Add},
				},
			},
		},
		{
			fstest: fstest{
				subdirs: []string{"dir1/dir2"},
				preactions: []taction{
					createFile("file1"),
					createFile("dir1/dir2/file1"),
				},
			},
			watcher: &Watcher{
				Recur: true,
			},
			expect: []map[string]sources.Update{
				{
					"file1":           sources.Update{Action: sources.Add},
					"dir1/dir2/file1": sources.Update{Action: sources.Add},
				},
			},
		},
		{
			fstest: fstest{
				actions: []taction{
					createFile("file1"),
				},
			},
			watcher: &Watcher{},
			expect: []map[string]sources.Update{
				{},
				{
					"file1": sources.Update{Action: sources.Add},
				},
			},
		},
		{
			fstest: fstest{
				actions: []taction{
					createFile("file1"),
				},
			},
			watcher: &Watcher{},
			expect: []map[string]sources.Update{
				{},
				{
					"file1": sources.Update{Action: sources.Add},
				},
			},
		},
		{
			fstest: fstest{
				actions: []taction{
					createFile("file1"),
					createFile("file2"),
					createFile("file3"),
					createFile("file4"),
				},
			},
			watcher: &Watcher{
				Recur: true,
			},
			expect: []map[string]sources.Update{
				{},
				{"file1": sources.Update{Action: sources.Add}},
				{"file2": sources.Update{Action: sources.Add}},
				{"file3": sources.Update{Action: sources.Add}},
				{"file4": sources.Update{Action: sources.Add}},
			},
		},
		{
			fstest: fstest{
				actions: []taction{
					createDir("dir1/dir2/dir3"),
					createFile("dir1/dir2/dir3/file1"),
				},
			},
			watcher: &Watcher{
				Recur: true,
			},
			expect: []map[string]sources.Update{
				{},
				{"dir1": sources.Update{Action: sources.Add}},
				{
					"dir1/dir2/dir3/file1": sources.Update{Action: sources.Add},
				},
			},
		},
		{
			fstest: fstest{
				actions: []taction{
					createDir("dir1/dir2/dir3"),
					createFile("dir1/dir2/dir3/file1"),
				},
			},
			watcher: &Watcher{
				Recur:      true,
				NameRegexp: regexp.MustCompile("file1"),
			},
			expect: []map[string]sources.Update{
				{},
				{
					"dir1/dir2/dir3/file1": sources.Update{Action: sources.Add},
				},
			},
		},
	}

	for i, fst := range fsts {
		t.Run(fmt.Sprintf("%s/%d", t.Name(), i), func(t *testing.T) {
			fst.run(t, func(ctx context.Context, t *testing.T, start func(context.Context, *testing.T)) {
				var err error
				w := fst.watcher
				w.Path, err = os.Getwd()
				if err != nil {
					t.Skip(err)
					return
				}

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				//defer cancel()
				ius, err := w.Next(ctx)
				if err != nil {
					t.Fatalf("Initial Next() got err = %v", err)
					return
				}

				if len(ius) != len(fst.expect[0]) {
					t.Fatalf("wrong initial file count expected = %d, got = %d", len(fst.expect[0]), len(ius))
				}
				for _, u := range ius {
					t.Logf("got update %v %s", u.Action, u.Target)
					if u.Action != sources.Add {
						t.Fatalf("Initial events should only be sources.Add")
					}
					if !strings.HasPrefix(u.Target, w.Path) {
						t.Fatalf("File update for file not in path path = %s, got = %s", w.Path, u.Target)
					}
					p := u.Target[len(w.Path)+1:]
					if _, ok := fst.expect[0][p]; !ok {
						t.Fatalf("Got unexpected event path = %s", p)
					}
				}

				start(ctx, t)

				for i := range fst.expect[1:] {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					us, err := w.Next(ctx)
					if err != nil {
						t.Fatalf("intermediate Next() got err = %v", err)
						return
					}
					cancel()

					for _, u := range us {
						t.Logf("got update %v %s", u.Action, u.Target)
						if !strings.HasPrefix(u.Target, w.Path) {
							t.Fatalf("Intermediate update for file not in path path = %s, got = %s", w.Path, u.Target)
						}
						p := u.Target[len(w.Path)+1:]
						if _, ok := fst.expect[i+1][p]; !ok {
							t.Fatalf("Got intermediate unexpected event path = %s; expecting = %v", p, fst.expect[i+1])
						}
						if u.Action != fst.expect[i+1][p].Action {
							t.Fatalf("Got wrong action want = %v , got = %v", fst.expect[i+1][p].Action, u.Action)
						}
					}
				}

				// We'll wait a bit and see if we get any extra events
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				fus, err := w.Next(ctx)
				defer cancel()
				if err == nil {
					for _, u := range fus {
						t.Logf("unexpected u = %v, f = %s", u.Action, u.Target)
					}
					t.Fatal("Got unexpected additional updates")
					return
				}
			})
		})
	}
}

type taction func(context.Context, *testing.T)

type tactions []taction

func (as tactions) run(ctx context.Context, t *testing.T) {
	for _, a := range as {
		a(ctx, t)
		// we lose notification events if they happen too quickly
		time.Sleep(1 * time.Millisecond)
	}
	return
}

type fstest struct {
	subdirs    []string
	preactions tactions
	actions    tactions
}

func (fst fstest) run(t *testing.T, tf func(context.Context, *testing.T, func(context.Context, *testing.T))) {
	dir, err := ioutil.TempDir("", "fstest")
	if err != nil {
		t.Skip(err)
		return
	}
	//defer os.RemoveAll(dir) // clean up

	for _, d := range fst.subdirs {
		path := filepath.Join(dir, d)
		err = os.MkdirAll(path, 0777)
		if err != nil {
			t.Skip(err)
			return
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := os.Chdir(dir); err != nil {
		t.Skip(err)
		return
	}

	fst.preactions.run(ctx, t)
	tf(ctx, t, fst.actions.run)
}

func createFile(fn string) taction {
	return func(ctx context.Context, t *testing.T) {
		t.Logf("create file %v", fn)
		if _, err := os.Create(fn); err != nil {
			t.Skip(err)
		}
	}
}

func removeFile(fn string) taction {
	return func(ctx context.Context, t *testing.T) {
		t.Logf("removing file %v", fn)
		if err := os.Remove(fn); err != nil {
			t.Skip(err)
		}
	}
}

func createDir(fn string) taction {
	return func(ctx context.Context, t *testing.T) {
		t.Logf("creating dir %v", fn)
		if err := os.MkdirAll(fn, 0777); err != nil {
			t.Skip(err)
		}
	}
}

func removeDir(fn string) taction {
	return func(ctx context.Context, t *testing.T) {
		t.Logf("removing dir %v", fn)
		if err := os.RemoveAll(fn); err != nil {
			t.Skip(err)
		}
	}
}

func pause(d time.Duration) taction {
	return func(ctx context.Context, t *testing.T) {
		t.Logf("pausing for %v", d)
		select {
		case <-ctx.Done():
			t.Skip(ctx.Err())
		case <-time.After(d):
		}
	}
}

func noop() taction {
	return func(ctx context.Context, t *testing.T) {
		t.Logf("noop")
		return
	}
}
