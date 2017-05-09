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

package docker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/sources"
	"github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

// Watcher watches for files being added and removed from a filesystem
type Watcher struct {
	ups          chan []*sources.Update
	envWhitelist []*regexp.Regexp
	dcli         *client.Client
	root         string

	sync.Mutex
	running map[string]*sources.Update
}

// New creates a watcher that reports on the apperance, and
// dissapearance of sources
func New(opts ...Opt) (*Watcher, error) {
	dcli, err := client.NewEnvClient()
	if err != nil {
		return nil, err
	}
	w := &Watcher{
		envWhitelist: []*regexp.Regexp{},
		dcli:         dcli,
		running:      map[string]*sources.Update{},
		root:         "",
	}
	for _, opt := range opts {
		err = opt(w)
		if err != nil {
			return nil, err
		}
	}

	if w.root == "" {
		i, err := dcli.Info(context.Background())
		if err != nil {
			return nil, fmt.Errorf("Failed to lookup docker root, %v", err)
		}
		w.root = i.DockerRootDir
	}
	return w, nil
}

// Opt is a type for configuration options for the docker Watcher
type Opt func(*Watcher) error

// WithDockerClient sets the default docker client
func WithDockerClient(cli *client.Client) Opt {
	return func(w *Watcher) error {
		w.dcli = cli
		return nil
	}
}

// WithEnvVarWhiteList lets you set a selection of regular
// experessions to match against the container environment
// variables
func WithEnvVarWhiteList(evwl []*regexp.Regexp) Opt {
	return func(w *Watcher) error {
		w.envWhitelist = evwl
		return nil
	}
}

// WithRoot sets the docker root to read files from
func WithRoot(root string) Opt {
	return func(w *Watcher) error {
		w.root = root
		return nil
	}
}

// startBackground creates a watcher that reports on the apperance, and
// dissapearance of sources
func (w *Watcher) startBackground(ctx context.Context) {
	w.ups = make(chan []*sources.Update, 1)
	go func() {
		defer close(w.ups)

		w.reconcile()

		g := errgroup.Group{}

		g.Go(func() error {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
					w.reconcile()
				}
			}
		})

		g.Go(func() error {
			return w.watchEvs(ctx)
		})

		g.Wait()
	}()
	return
}

func (w *Watcher) reconcileRunning(targets []*sources.Update) {
	w.Lock()
	defer w.Unlock()
	updates := []*sources.Update{}
	found := map[string]*sources.Update{}

	if glog.V(1) {
		glog.Infof("reconcile start: %d running, %d found", len(w.running), len(targets))
	}

	for _, t := range targets {
		_, ok := w.running[t.Target]
		if !ok {
			if glog.V(1) {
				glog.Infof("reconcilliation found unwatched container %s", t.Target)
			}

			base, err := w.dockerDecorator(t.Target, w.envWhitelist)
			if err != nil {
				glog.Errorf("failed to fetch container information %v", err)
				continue
			}
			t.Action = sources.Add
			t.Labels = base.Labels
			updates = append(updates, t)
			w.running[t.Target] = t
		}
		found[t.Target] = t
	}
	for tn, t := range w.running {
		_, ok := found[tn]
		if !ok {
			if glog.V(1) {
				glog.Infof("reconcilliation found watched dead container %s", t.Target)
			}
			t.Action = sources.Remove
			updates = append(updates, t)
			delete(w.running, t.Target)
		}
	}
	if len(updates) > 0 {
		w.ups <- updates
	}
}

func (w *Watcher) reconcile() error {
	if glog.V(2) {
		glog.Infof("Running reconcilliation")
	}

	containers, err := w.dcli.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return err
	}

	existing := []*sources.Update{}
	for _, container := range containers {
		if container.State == "running" {
			existing = append(existing, &sources.Update{
				Target: container.ID,
			})
		}
	}
	w.reconcileRunning(existing)
	return nil
}

func (w *Watcher) watchEvs(ctx context.Context) error {
	if glog.V(2) {
		glog.Infof("Running watch loop")
	}

	evs, err := w.dcli.Events(context.Background(), types.EventsOptions{})
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(evs)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		func() {
			w.Lock()
			defer w.Unlock()
			str := scanner.Text()
			ev := evMessage{}
			err := json.Unmarshal([]byte(str), &ev)
			if err != nil {
				glog.Errorf("error unmarshaling docker update%v\n", err)
				return
			}

			switch ev.Action {
			case "die":
				if glog.V(1) {
					glog.Infof("Saw die event for %s", ev.ID)
				}
				upd, ok := w.running[ev.ID]
				if !ok {
					glog.Error("Update for a container we didn't think was running")
					return
				}
				delete(w.running, ev.ID)
				upd.Action = sources.Remove
				w.ups <- []*sources.Update{upd}
			case "start":
				base, err := w.dockerDecorator(ev.ID, w.envWhitelist)
				if err != nil {
					glog.Infof("failed to fetch container information", err)
					return
				}
				if glog.V(1) {
					glog.Infof("Saw start event for %s", ev.ID)
				}
				if _, ok := w.running[ev.ID]; ok {
					glog.Error("start event for container we are already watching")
					return
				}
				update := &sources.Update{Action: sources.Add, Target: ev.ID, Labels: base.Labels}
				w.running[ev.ID] = update
				w.ups <- []*sources.Update{update}
			default:
			}
		}()
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

// Next should be called each time you wish to watch for an update.
func (w *Watcher) Next(ctx context.Context) ([]*sources.Update, error) {
	if w.ups == nil {
		w.startBackground(ctx)
	}

	select {
	case u := <-w.ups:
		if u != nil {
			return u, nil
		}
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

const (
	// ContainerEventType is the event type that containers generate
	evContainerEventType = "container"
	// DaemonEventType is the event type that daemon generate
	evDaemonEventType = "daemon"
	// ImageEventType is the event type that images generate
	evImageEventType = "image"
	// NetworkEventType is the event type that networks generate
	evNetworkEventType = "network"
	// PluginEventType is the event type that plugins generate
	evPluginEventType = "plugin"
	// VolumeEventType is the event type that volumes
	// generate
	evVolumeEventType = "volume"
)

// Actor describes something that generates events,
// like a container, or a network, or a volume.
// It has a defined name and a set or attributes.
// The container attributes are its labels, other actors
// can generate these attributes from other properties.
type evActor struct {
	ID         string
	Attributes map[string]string
}

// Message represents the information an event
// contains
type evMessage struct {
	// Deprecated information from JSONMessage.
	// With data only in container events.
	Status string `json:"status,omitempty"`
	State  string `json:"state,omitempty"`
	ID     string `json:"id,omitempty"`
	From   string `json:"from,omitempty"`

	Type   string
	Action string
	Actor  evActor

	Time     int64 `json:"time,omitempty"`
	TimeNano int64 `json:"timeNano,omitempty"`
}

func (w *Watcher) dockerDecorator(id string, envWhitelist []*regexp.Regexp) (logspray.Message, error) {
	hn, _ := os.Hostname()
	msg := logspray.Message{
		Labels: map[string]string{
			"instance":     hn,
			"job":          "unknown",
			"container_id": "none",
		},
	}

	vars, err := w.getContainerMetadata(id)
	if err != nil {
		return logspray.Message{}, errors.Wrapf(err, "could no get container metadata for %v", id)
	}

	if v, ok := vars["id"]; ok {
		msg.Labels["container_id"] = v[0]
	}

	if v, ok := vars["image"]; ok {
		parts := strings.Split(v[0], "-")
		id := parts[len(parts)-1]
		name := id
		if len(parts) > 1 {
			name = strings.Join(parts[0:len(parts)-1], "-")
		}
		msg.Labels["container_image_id"] = id
		msg.Labels["container_image_name"] = name
	}

	for _, rx := range envWhitelist {
		for k, vs := range vars {
			if !rx.MatchString(k) {
				continue
			}
			if len(vs) != 1 {
				if glog.V(1) {
					glog.Infof("picking first item from environment variable %s with %d values", k, len(vars[k]))
				}
			}
			msg.Labels[fmt.Sprintf("container_env_%s", strings.ToLower(k))] = vs[0]
		}
	}

	for lk, lv := range vars {
		if !strings.HasPrefix(lk, "label_") {
			continue
		}
		msg.Labels[fmt.Sprintf("container_%s", lk)] = lv[0]
	}

	return msg, nil
}

// getContainerMetadata takes  docker container id and returns some
// metadata.
func (w *Watcher) getContainerMetadata(id string) (map[string][]string, error) {
	cinfo, _, err := w.dcli.ContainerInspectWithRaw(context.Background(), id, false)
	if err != nil {
		return nil, err
	}

	if cinfo.State.Status == "exited" {
		return nil, errors.New("ignore exited container")
	}

	res := map[string][]string{}
	res["id"] = []string{cinfo.ID}
	res["image"] = []string{cinfo.Config.Image}
	for _, str := range cinfo.Config.Env {
		ss := strings.SplitN(str, "=", 2)
		k := ss[0]
		v := ss[1]
		res[k] = append(res[k], v)
	}

	for lk, lv := range cinfo.Config.Labels {
		res[fmt.Sprintf("label_%s", lk)] = []string{lv}
	}

	return res, nil
}
