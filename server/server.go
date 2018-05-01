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
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/QubitProducts/logspray/common"
	"github.com/QubitProducts/logspray/indexer"
	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/ql"
	"github.com/QubitProducts/logspray/sinks"
	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
)

var (
	lineRxCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logspray_server_received_lines_total",
		Help: "Counter of total lines received since process start.",
	})
	lineTxCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logspray_server_transmit_lines_total",
		Help: "Counter of total lines sent to clients since process start.",
	})
	subscribersGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "logspray_server_active_subscribers",
		Help: "Gauge of number of active subscribers.",
	})
	sourcesGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "logspray_server_active_sources",
		Help: "Gauge of number of active sources.",
	})
	lagTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "logspray_server_message_lag_time_seconds",
		Help:    "Histogram of he difference between wall clock time and the message time.",
		Buckets: prometheus.ExponentialBuckets(0.001, 10, 5),
	})
)

type dateRanger interface {
	GetFrom() *timestamp.Timestamp
	GetTo() *timestamp.Timestamp
}

func getRange(dr dateRanger) (time.Time, time.Time, error) {
	from, err := ptypes.Timestamp(dr.GetFrom())
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	to, err := ptypes.Timestamp(dr.GetTo())
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	return from, to, nil
}

type serverOpt func(*logServer) error
type logServer struct {
	checkClaims bool
	indx        *indexer.Indexer

	subs                     *subscriberSet
	grafanaUser, grafanaPass string
}

// RegisterStats explicitly register the prometheus metrics, this prevents them
// showing up in the client
func RegisterStats() {
	prometheus.MustRegister(lineRxCount)
	prometheus.MustRegister(lineTxCount)
	prometheus.MustRegister(subscribersGauge)
	prometheus.MustRegister(sourcesGauge)
	prometheus.MustRegister(lagTime)
}

func new(opts ...serverOpt) *logServer {
	lsrv := &logServer{
		checkClaims: true,
		indx:        nil,
		subs:        newSubsSet(),
	}

	for _, o := range opts {
		if err := o(lsrv); err != nil {
			panic(err)
		}
	}

	glog.Info("Creating a new server")
	return lsrv
}

func WithCheckClaims(check bool) serverOpt {
	return func(srv *logServer) error {
		srv.checkClaims = check
		return nil
	}
}

func WithIndex(index *indexer.Indexer) serverOpt {
	return func(srv *logServer) error {
		srv.indx = index
		return nil
	}
}

func WithGrafanaBasicAuth(user, pass string) serverOpt {
	return func(srv *logServer) error {
		srv.grafanaUser, srv.grafanaPass = user, pass
		return nil
	}
}

func (l *logServer) Log(ctx context.Context, r *logspray.Message) (*logspray.LogSummary, error) {
	if err := l.ensureScope(ctx, common.WriteScope); err != nil {
		return nil, err
	}
	l.subs.publish(nil, r)

	if glog.V(1) {
		glog.Info("New Log event arriving")
	}

	return &logspray.LogSummary{}, nil
}

func (l *logServer) LogStream(s logspray.LogService_LogStreamServer) error {
	sourcesGauge.Add(1.0)
	defer sourcesGauge.Sub(1.0)

	var err error
	if err := l.ensureScope(s.Context(), common.WriteScope); err != nil {
		return err
	}

	var hdr *logspray.Message

	if glog.V(1) {
		glog.Info("New Log stream arriving")
	}
	defer func() {
		if err != nil && err != context.Canceled {
			glog.Info("Log stream ended: err = %v", err)
		}
	}()

	var iw sinks.MessageWriter
	for {
		m, err := s.Recv()
		if err != nil {
			return err
		}
		if glog.V(3) {
			glog.Infof("Incoming message: %#v", *m)
		}
		if m.Setheader || m.ControlMessage == logspray.Message_SETHEADER {
			if hdr != nil {
				return errors.New("Multiple headers in one steram are not allowed")
			}
			hdr = m
			if l.indx != nil {
				iw, err = l.indx.AddSource(s.Context(), m.StreamID, m.Labels)
				if err != nil {
					glog.Errorf("Error adding index source, err = %v\n", err)
				}
			}

			defer func() {
				if iw != nil {
					iw.Close()
				}

				l.subs.publish(
					hdr,
					&logspray.Message{
						StreamID:       hdr.StreamID,
						ControlMessage: logspray.Message_STREAMEND,
					})
			}()
			continue
		}

		if hdr == nil {
			return errors.New("Message data sent before header")
		}

		// We'll set the StreamID here
		m.StreamID = hdr.StreamID

		lineRxCount.Inc()
		if mtime, err := ptypes.Timestamp(m.Time); err == nil {
			lagTime.Observe(float64(time.Since(mtime)) / float64(time.Second))
		}

		if m.Labels == nil {
			m.Labels = map[string]string{}
		}

		l.subs.publish(hdr, m)

		err = iw.WriteMessage(s.Context(), m)
		if err != nil {
			glog.Errorf("Error adding index source, err = %v\n", err)
		}
	}
}

func (l *logServer) Tail(r *logspray.TailRequest, s logspray.LogService_TailServer) error {
	ctx := s.Context()

	if err := l.ensureScope(ctx, common.ReadScope); err != nil {
		return err
	}

	mc := l.subs.subscribe()
	if glog.V(1) {
		glog.Info("Subscriber added")
	}
	subscribersGauge.Add(1.0)

	defer l.subs.unsubscribe(mc)
	defer subscribersGauge.Sub(1.0)
	defer glog.Info("Subscriber gone")

	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()

	matcher, err := ql.Compile(r.Query)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, err.Error())
	}

	headers := map[string]*logspray.Message{}
	sentHeaders := map[*logspray.Message]struct{}{}

	for {
		select {
		case m := <-mc:
			if m.ControlMessage == logspray.Message_SETHEADER {
				headers[m.StreamID] = m
				continue
			}

			if m.ControlMessage == logspray.Message_STREAMEND {
				hm, ok := headers[m.StreamID]
				if !ok {
					glog.Errorf("Got close for untracked stream")
					continue
				}
				delete(headers, hm.StreamID)
				delete(sentHeaders, hm)
				if err := s.Send(m); err != nil {
					glog.Errorf("Error sending stream end to subscribe err = %v", err)
					return err
				}
				continue
			}

			hdr, ok := headers[m.StreamID]
			if !ok {
				glog.Info("Error no known header for Stream %s", fmt.Sprintf("%s", m.StreamID))
			}

			if !matcher(hdr, m, false) {
				continue
			}
			if hdr != nil {
				if _, ok := sentHeaders[hdr]; !ok {
					if err := s.Send(hdr); err != nil {
						glog.Info("Error sending to subscribe err = %v", err)
						return err
					}
					sentHeaders[hdr] = struct{}{}
				}
			}
			if err := s.Send(m); err != nil {
				glog.Errorf("Error sending to subscribe err = %v", err)
				return err
			}
			lineTxCount.Inc()
		case <-tick.C:
			err := s.Send(&logspray.Message{
				ControlMessage: logspray.Message_OK,
			})
			if err != nil {
				glog.Errorf("Error sending heartbeat to subscribe err = %v", err)
				return err
			}
		case <-ctx.Done():
			if err != nil && err != context.Canceled {
				glog.Errorf("Tail Context closed = %v", ctx.Err())
				return err
			}
			return nil
		}
	}
}

func (l *logServer) Labels(ctx context.Context, r *logspray.LabelsRequest) (*logspray.LabelsResponse, error) {
	var err error
	if err = l.ensureScope(ctx, common.ReadScope); err != nil {
		return nil, err
	}

	from, to, err := getRange(r)
	if err != nil {
		return nil, err
	}

	res := &logspray.LabelsResponse{Names: []string{}}
	res.Names, err = l.indx.Labels(from, to)

	return res, err
}

func (l *logServer) LabelValues(ctx context.Context, r *logspray.LabelValuesRequest) (*logspray.LabelValuesResponse, error) {
	var err error
	if err = l.ensureScope(ctx, common.ReadScope); err != nil {
		return nil, err
	}

	from, to, err := getRange(r)
	if err != nil {
		return nil, err
	}
	vs, hitcount, err := l.indx.LabelValues(r.Name, from, to, int(r.Count))
	if err != nil {
		return nil, err
	}

	res := &logspray.LabelValuesResponse{Values: vs, TotalHitCount: uint64(hitcount)}

	return res, nil
}

func (l *logServer) Search(ctx context.Context, r *logspray.SearchRequest) (*logspray.SearchResponse, error) {
	ctx, cancel := context.WithCancel(ctx)

	var err error
	if err = l.ensureScope(ctx, common.ReadScope); err != nil {
		return nil, err
	}
	from, to, err := getRange(r)
	if err != nil {
		return nil, err
	}

	matcher, err := ql.Compile(r.Query)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	offset := r.Offset
	count := r.Count
	res := &logspray.SearchResponse{}
	msgFunc := logspray.MakeFlattenStreamFunc(func(m *logspray.Message) error {
		t := time.Unix(m.Time.Seconds, int64(m.Time.Nanos))
		if m.ControlMessage == 0 {
			if t.Before(from) || t.After(to) {
				return nil
			}
			if offset != 0 {
				offset--
				return nil
			}
		}
		res.TotalHitCount++
		res.Messages = append(res.Messages, m)
		if m.ControlMessage == 0 {
			count--
			if count == 0 {
				cancel()
			}
		}
		return nil
	})
	err = l.indx.Search(ctx, msgFunc, matcher, from, to, r.Reverse)
	if err != nil && err != context.Canceled {
		return res, err
	}

	return res, nil
}

func (l *logServer) SearchStream(r *logspray.SearchRequest, s logspray.LogService_SearchStreamServer) error {
	ctx := s.Context()
	ctx, cancel := context.WithCancel(ctx)

	if err := l.ensureScope(ctx, common.ReadScope); err != nil {
		return err
	}

	from, to, err := getRange(r)
	if err != nil {
		return err
	}

	matcher, err := ql.Compile(r.Query)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, err.Error())
	}

	count := r.Count
	offset := r.Offset
	msgFunc := logspray.MakeInjectStreamHeadersFunc(func(m *logspray.Message) error {
		t := time.Unix(m.Time.Seconds, int64(m.Time.Nanos))
		if m.ControlMessage == 0 {
			if t.Before(from) || t.After(to) {
				return nil
			}
			if offset != 0 {
				offset--
				return nil
			}
		}

		if err := s.Send(m); err != nil {
			return err
		}

		if m.ControlMessage == 0 {
			count--
			if count == 0 {
				cancel()
			}
		}
		return nil
	})

	err = l.indx.Search(ctx, msgFunc, matcher, from, to, r.Reverse)
	if err != nil && err != context.Canceled {
		return err
	}

	return nil
}
