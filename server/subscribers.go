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

package server

import (
	"sync"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

var (
	lineTxDropCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logspray_server_transmit_dropped_lines_total",
		Help: "Counter of total lines dropped due to slow clients.",
	}, []string{})
)

func init() {
	prometheus.MustRegister(lineTxDropCount)
}

type subscriberSet struct {
	sync.RWMutex
	subs map[*subscriber]chan *logspray.Message
}

type subscriber struct {
	client string

	sync.Mutex
	hdrSent map[*logspray.Message]struct{}
}

func newSubsSet() *subscriberSet {
	return &subscriberSet{
		subs: map[*subscriber]chan *logspray.Message{},
	}
}

var blockLimiter = rate.NewLimiter(rate.Every(1*time.Second), 5)

func (ss *subscriberSet) publish(hdr *logspray.Message, m *logspray.Message) {
	ss.RLock()
	defer ss.RUnlock()

	if ss == nil || ss.subs == nil {
		return
	}

	for s, sc := range ss.subs {
		func(s *subscriber, sc chan *logspray.Message) {
			s.Lock()
			defer s.Unlock()

			if _, ok := s.hdrSent[hdr]; !ok {
				select {
				case sc <- hdr:
					s.hdrSent[hdr] = struct{}{}
				default:
					if blockLimiter.Allow() {
						glog.Warning("subscriber tried to block us whilst sending header ", s)
					}
					lineTxDropCount.WithLabelValues().Inc()
					return
				}
			}

			// When a stream ends, we must inform downstream, but don't
			// want to drop it if the client is slow to respond.
			if m.ControlMessage == logspray.Message_STREAMEND {
				go func() {
					sc <- m
				}()
				delete(s.hdrSent, hdr)
				return
			}

			select {
			case sc <- m:
			default:
				if blockLimiter.Allow() {
					glog.Warning("subscriber tried to block us ", s)
				}
				lineTxDropCount.WithLabelValues().Inc()
			}
		}(s, sc)
	}
}

const bufSize = 1000

func (ss *subscriberSet) subscribe() <-chan *logspray.Message {
	ss.Lock()
	defer ss.Unlock()

	mc := make(chan *logspray.Message, bufSize)
	s := subscriber{
		hdrSent: map[*logspray.Message]struct{}{},
	}
	ss.subs[&s] = mc

	return mc
}

func (ss *subscriberSet) unsubscribe(csc <-chan *logspray.Message) {
	ss.Lock()
	defer ss.Unlock()

	for s, sc := range ss.subs {
		if sc == csc {
			delete(ss.subs, s)
		}
	}

	return
}
