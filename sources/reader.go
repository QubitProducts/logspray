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
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/QubitProducts/logspray/sinks"
	"github.com/cloudflare/backoff"
	"github.com/golang/glog"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	lineCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logspray_reader_read_lines_total",
		Help: "Counter of total lines read since process start.",
	}, []string{"logspray_job"})
	bytesCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logspray_reader_read_bytes_total",
		Help: "Counter of total bytes read since process start.",
	}, []string{"logspray_job"})
	sourcesActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "logspray_reader_active_sources",
		Help: "Gauge of number of active sources.",
	})
	sourcesOpened = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logspray_reader_opened_sources_total",
		Help: "Counter of number of sources since process start.",
	})
)

func init() {
	prometheus.MustRegister(lineCount)
	prometheus.MustRegister(bytesCount)
	prometheus.MustRegister(sourcesActive)
	prometheus.MustRegister(sourcesOpened)
}

// ReadAllTargets drains a source of log data
func ReadAllTargets(ctx context.Context, snk sinks.Sinker, src Sourcer) error {
	var err error

	targets := &targetSet{
		Sourcer: src,
		entropy: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	defer targets.cancelAll()

	existing, err := src.Next(ctx)
	if err != nil {
		return err
	}

	for _, u := range existing {
		if glog.V(2) {
			glog.Infof("Found pre-existing job: %#v", *u)
		}

		go targets.addSource(ctx, snk, u, false)
	}

	for us, err := src.Next(ctx); err == nil; us, err = src.Next(ctx) {
		if err != nil {
			return err
		}
		for _, u := range us {
			switch u.Action {
			case Remove:
				if glog.V(2) {
					glog.Infof("Removing job: %#v", *u)
				}
				targets.cancelTarget(u.Target)
			case Add:
				if glog.V(2) {
					glog.Infof("Found new job: %#v", *u)
				}
				go targets.addSource(ctx, snk, u, true)
			}
		}
	}
	return err
}

func (ts *targetSet) addSource(ctx context.Context, snk sinks.Sinker, u *Update, fromStart bool) {
	b := backoff.New(10*time.Second, 1*time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streamID := ulid.MustNew(ulid.Timestamp(time.Now()), ts.entropy)

			w, err := snk.AddSource(streamID.String(), u.Labels)
			if err != nil {
				if glog.V(2) {
					glog.Errorf("addsource error: %#v , %v", *u, err)
				}
				<-time.After(b.Duration())
				continue
			}
			b.Reset()

			err = ts.readAllFromTarget(ctx, w, u, fromStart)
			switch err {
			case nil, io.EOF, context.Canceled:
				return
			}
		}
	}
}

func (ts *targetSet) readAllFromTarget(ctx context.Context, w sinks.MessageWriter, u *Update, fromStart bool) error {
	sourcesOpened.Inc()
	sourcesActive.Inc()
	defer sourcesActive.Dec()

	fctx := ts.setTarget(ctx, u.Target)
	defer w.Close()
	// Start at 1, control messages like setheader then all get 0s
	msgid := uint64(1)

	r, err := ts.ReadTarget(fctx, u.Target, fromStart)
	if err != nil {
		return err
	}

	for {
		msg, err := r.MessageRead(fctx)
		if err == io.EOF {
			if glog.V(2) {
				glog.Infof("Stream ended %v\n", u.Target)
			}
			return err
		}

		if err != nil {
			if glog.V(1) {
				glog.Errorf("read message error: %#v , %v", *u, err)
			}
			continue
		}
		lineCount.WithLabelValues(u.Labels["job"]).Inc()
		bytesCount.WithLabelValues(u.Labels["job"]).Add(float64(len(msg.Text)))
		msg.Index = msgid
		msgid++
		if err := w.WriteMessage(ctx, msg); err != nil {
			if glog.V(1) {
				glog.Errorf("write message error: %#v , %v", *u, err)
			}
		}
	}
}

type targetSet struct {
	Sourcer
	sinks.Sinker
	entropy io.Reader

	sync.Mutex
	cfs map[string]context.CancelFunc
}

func (ts *targetSet) setTarget(ctx context.Context, id string) context.Context {
	fctx, cf := context.WithCancel(ctx)

	ts.Lock()
	defer ts.Unlock()
	if ts.cfs == nil {
		ts.cfs = map[string]context.CancelFunc{}
	}

	ts.cfs[id] = cf
	return fctx
}

func (ts *targetSet) cancelTarget(id string) {
	ts.Lock()
	defer ts.Unlock()

	if cf, ok := ts.cfs[id]; ok {
		delete(ts.cfs, id)
		cf()
	}
}

func (ts *targetSet) cancelAll() {
	ts.Lock()
	defer ts.Unlock()
	for _, cf := range ts.cfs {
		cf()
	}
}
