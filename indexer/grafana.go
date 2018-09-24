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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/ql"
	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	simplejson "github.com/tcolgate/grafana-simple-json-go"
)

func adhocFilterToQuery(base string, afs []simplejson.QueryAdhocFilter) (string, error) {
	terms := []string{base}

	for _, af := range afs {
		var op string
		switch af.Operator {
		case "=", "!=", "!~":
			op = af.Operator
		case "=~":
			op = "~"
		default:
			return "", errors.New("unsupported operator")
		}
		terms = append(terms, fmt.Sprintf("%q %s %q", af.Key, op, af.Value))
	}

	return strings.Join(terms, " "), nil
}

// GrafanaQuery implements the Grafana Simple JSON Query request
func (idx *Indexer) GrafanaQuery(ctx context.Context, target string, args simplejson.QueryArguments) ([]simplejson.DataPoint, error) {
	query, err := adhocFilterToQuery(target, args.Filters)
	if err != nil {
		return nil, err
	}

	matcher, err := ql.Compile(query)
	if err != nil {
		return nil, err
	}

	hits := map[time.Time]float64{}
	var tvals []time.Time
	for qtime := args.From; qtime.Before(args.To); qtime = qtime.Add(args.Interval) {
		t := qtime.Truncate(args.Interval)
		hits[t] = 0
		tvals = append(tvals, t)
	}

	msgFunc := func(m *logspray.Message) error {
		if m.ControlMessage != 0 {
			return nil
		}
		t, _ := ptypes.Timestamp(m.Time)
		if t.Before(args.From) || t.After(args.To) {
			return nil
		}
		hits[t.Truncate(args.Interval)]++
		return nil
	}

	err = idx.Search(ctx, msgFunc, matcher, args.From, args.To, false)
	if err != nil {
		return nil, err
	}

	data := []simplejson.DataPoint{}
	for _, t := range tvals {
		data = append(data, simplejson.DataPoint{Time: t, Value: hits[t]})
	}

	return data, nil
}

// GrafanaQueryTable implements the Grafana Simple JSON Query request for tables
func (idx *Indexer) GrafanaQueryTable(ctx context.Context, target string, args simplejson.TableQueryArguments) ([]simplejson.TableColumn, error) {
	query, err := adhocFilterToQuery(target, args.Filters)
	if err != nil {
		return nil, err
	}

	matcher, err := ql.Compile(query)
	if err != nil {
		return nil, err
	}

	labelCols := map[string]simplejson.TableStringColumn{}
	timeCol := simplejson.TableTimeColumn{}
	textCol := simplejson.TableStringColumn{}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	max := idx.grafanaMaxRes
	j := 0
	msgFunc := logspray.MakeFlattenStreamFunc(func(m *logspray.Message) error {
		if m.ControlMessage != 0 {
			return nil
		}
		t, _ := ptypes.Timestamp(m.Time)
		if t.Before(args.From) || t.After(args.To) {
			return nil
		}
		timeCol = append(timeCol, t)
		textCol = append(textCol, m.Text)
		for ln, lv := range m.Labels {
			if _, ok := labelCols[ln]; !ok {
				labelCols[ln] = make(simplejson.TableStringColumn, j+1)
			}
			if len(labelCols[ln]) < j+1 {
				vs := labelCols[ln]
				vs = append(vs, make(simplejson.TableStringColumn, j+1-len(vs))...)
				col := labelCols[ln]
				col = vs
				labelCols[ln] = col
			}
			labelCols[ln][j] = lv
		}
		j++
		if j >= max {
			cancel()
		}
		return nil
	})

	err = idx.Search(ctx, msgFunc, matcher, args.From, args.To, false)
	if err != nil && err != context.Canceled {
		return nil, err
	}

	// extend any label columns o the full length
	var colNames []string
	for ln := range labelCols {
		colNames = append(colNames, ln)
		if len(labelCols[ln]) < len(timeCol) {
			vs := labelCols[ln]
			vs = append(vs, make(simplejson.TableStringColumn, len(timeCol)-len(vs))...)
			col := labelCols[ln]
			col = vs
			labelCols[ln] = col
		}
	}

	res := []simplejson.TableColumn{
		{
			Text: "Time",
			Data: timeCol,
		},
		{
			Text: "Text",
			Data: textCol,
		},
	}

	sort.Strings(colNames)
	for _, n := range colNames {
		res = append(res, simplejson.TableColumn{
			Text: n,
			Data: labelCols[n],
		})
	}

	return res, nil
}

// GrafanaAnnotations implements the grafana Simple JSON Annotations request
func (idx *Indexer) GrafanaAnnotations(ctx context.Context, query string, args simplejson.AnnotationsArguments) ([]simplejson.Annotation, error) {
	query, err := adhocFilterToQuery(query, args.Filters)
	if err != nil {
		return nil, err
	}

	matcher, err := ql.Compile(query)
	if err != nil {
		return nil, err
	}

	res := []simplejson.Annotation{}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	max := idx.grafanaMaxRes
	var hits []*logspray.Message
	msgFunc := logspray.MakeFlattenStreamFunc(func(m *logspray.Message) error {
		t, _ := ptypes.Timestamp(m.Time)
		if t.Before(args.From) || t.After(args.To) {
			return nil
		}
		hits = append(hits, m)
		max--
		if max == 0 {
			cancel()
		}
		return nil
	})
	err = idx.Search(ctx, msgFunc, matcher, args.From, args.To, true)
	if err != nil && err != context.Canceled {
		return nil, err
	}
	for i := range hits {
		t, _ := ptypes.Timestamp(hits[i].Time)
		tags := []string{}
		for k, v := range hits[i].Labels {
			tags = append(tags, k+":"+v)
		}
		res = append(res, simplejson.Annotation{
			Time:  t,
			Text:  hits[i].Text,
			Title: hits[i].Text,
			Tags:  tags,
		})
	}

	return res, nil
}

// GrafanaSearch implements the Grafana Simple JSON search query
func (idx *Indexer) GrafanaSearch(ctx context.Context, target string) ([]string, error) {
	res := []string{}

	parts := strings.SplitN(target, "=", 2)
	if !strings.HasSuffix(target, "=") && len(parts) == 1 {
		ls, err := idx.Labels(time.Now().Add(-1*time.Hour), time.Now())
		for _, l := range ls {
			res = append(res, l+"=")
		}
		return res, err
	}

	lvs, _, err := idx.LabelValues(parts[0], time.Now().Add(-1*time.Hour), time.Now(), 100)
	for i := range lvs {
		res = append(res, parts[0]+"="+lvs[i])
	}

	return res, err
}

// GrafanaAdhocFilterTags implements the Grafana Simple JSON tags query
func (idx *Indexer) GrafanaAdhocFilterTags(ctx context.Context) ([]simplejson.TagInfoer, error) {
	res := []simplejson.TagInfoer{}

	ls, err := idx.Labels(time.Now().Add(-1*time.Hour), time.Now())
	if err != nil {
		return nil, err
	}

	for _, l := range ls {
		res = append(res, simplejson.TagStringKey(l))
	}

	return res, err
}

// GrafanaAdhocFilterTagValues implements the Grafana Simple JSON tag values query
func (idx *Indexer) GrafanaAdhocFilterTagValues(ctx context.Context, key string) ([]simplejson.TagValuer, error) {
	res := []simplejson.TagValuer{}

	lvs, _, err := idx.LabelValues(key, time.Now().Add(-1*time.Hour), time.Now(), 100)
	for i := range lvs {
		res = append(res, simplejson.TagStringValue(lvs[i]))
	}

	return res, err
}
