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

package relabeler

import (
	"context"
	"errors"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/relabel"
	"github.com/QubitProducts/logspray/sinks"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	relabelDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "logspray_sink_relabeler_duration_seconds",
		Help:    "Counter of total time spent relabeling.",
		Buckets: prometheus.ExponentialBuckets(0.00001, 10, 5),
	})
	relabelLineDrops = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logspray_sink_relabeler_line_drops_count",
		Help: "Counter of total number of lines dropped by relabeling.",
	}, []string{"logspray_job"})
	relabelTargetDrops = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logspray_sink_relabeler_target_drops_count",
		Help: "Counter of total number of targets dropped by relabeling.",
	}, []string{"logspray_job"})
)

func init() {
	prometheus.MustRegister(relabelDuration)
	prometheus.MustRegister(relabelLineDrops)
	prometheus.MustRegister(relabelTargetDrops)
}

// Relabeler is a sink that runs all new messages through a set of relabel
// rules.
type Relabeler struct {
	nextSink                 sinks.Sinker
	instanceRules, lineRules *relabel.Config
}

// New creates a new Relabeler
func New(nextSink sinks.Sinker, instRules, lineRules *relabel.Config) *Relabeler {
	return &Relabeler{nextSink, instRules, lineRules}
}

// AddSource implemets sink.Sinker for the Relabeler sink
func (o *Relabeler) AddSource(id string, Labels map[string]string) (sinks.MessageWriter, error) {
	m := &logspray.Message{Labels: Labels}
	if o.instanceRules != nil {
		t := prometheus.NewTimer(relabelDuration)
		if !o.instanceRules.Relabel(m) {
			t.ObserveDuration()
			relabelTargetDrops.WithLabelValues(m.Labels["job"]).Inc()
			return nil, errors.New("rejected by instance relabel rules")
		}
		t.ObserveDuration()
	}
	mw, err := o.nextSink.AddSource(id, m.Labels)
	if err != nil {
		return mw, err
	}

	return &MessageWriter{
		cfg: o,
		mw:  mw,
	}, err
}

// MessageWriter is a message writer implementatino for the Relabeler sink
type MessageWriter struct {
	cfg *Relabeler
	mw  sinks.MessageWriter
}

// WriteMessage implements the MessageWriter interface for the relabeler sink
func (o *MessageWriter) WriteMessage(ctx context.Context, m *logspray.Message) error {
	if o.cfg.lineRules != nil {
		t := prometheus.NewTimer(relabelDuration)
		if !o.cfg.lineRules.Relabel(m) {
			t.ObserveDuration()
			relabelLineDrops.WithLabelValues(m.Labels["job"]).Inc()
			return nil
		}
		t.ObserveDuration()
	}
	return o.mw.WriteMessage(ctx, m)
}

// Close implements Close() for the relabeler MessageWriter
func (o *MessageWriter) Close() error {
	return o.mw.Close()
}
