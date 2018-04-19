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
	"sort"
	"strings"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/ql"
	"github.com/golang/protobuf/ptypes"
	"github.com/tcolgate/grafanasj"
)

// GrafanaQuery implements the Grafana Simple JSON Query request
func (idx *Indexer) GrafanaQuery(ctx context.Context, from, to time.Time, interval time.Duration, maxDPs int, target string) ([]grafanasj.Data, error) {
	matcher, err := ql.Compile(target)
	if err != nil {
		return nil, err
	}

	hits := map[time.Time]float64{}
	var tvals []time.Time
	for qtime := from; qtime.Before(to); qtime = qtime.Add(interval) {
		t := qtime.Truncate(interval)
		hits[t] = 0
		tvals = append(tvals, t)
	}

	msgFunc := func(m *logspray.Message) error {
		if m.ControlMessage != 0 {
			return nil
		}
		t, _ := ptypes.Timestamp(m.Time)
		hits[t.Truncate(interval)]++
		return nil
	}

	err = idx.Search(ctx, msgFunc, matcher, from, to, 500, 0, false)
	if err != nil {
		return nil, err
	}

	data := []grafanasj.Data{}
	for _, t := range tvals {
		data = append(data, grafanasj.Data{Time: t, Value: hits[t]})
	}

	return data, nil
}

// GrafanaQueryTable implements the Grafana Simple JSON Query request for tables
func (idx *Indexer) GrafanaQueryTable(ctx context.Context, from, to time.Time, target string) ([]grafanasj.TableColumn, error) {
	matcher, err := ql.Compile(target)
	if err != nil {
		return nil, err
	}

	labelCols := map[string]grafanasj.TableColumn{}
	timeCol := grafanasj.TableColumn{Text: "Time", Type: "time"}
	textCol := grafanasj.TableColumn{Text: "Text", Type: "string"}

	j := 0
	msgFunc := logspray.MakeFlattenStreamFunc(func(m *logspray.Message) error {
		if m.ControlMessage != 0 {
			return nil
		}
		t, _ := ptypes.Timestamp(m.Time)
		timeCol.Values = append(timeCol.Values, t)
		textCol.Values = append(textCol.Values, m.Text)
		for ln, lv := range m.Labels {
			if _, ok := labelCols[ln]; !ok {
				labelCols[ln] = grafanasj.TableColumn{
					Text:   ln,
					Type:   "string",
					Values: make([]interface{}, j+1),
				}
			}
			if len(labelCols[ln].Values) < j+1 {
				vs := labelCols[ln].Values
				vs = append(vs, make([]interface{}, j+1-len(vs))...)
				col := labelCols[ln]
				col.Values = vs
				labelCols[ln] = col
			}
			labelCols[ln].Values[j] = lv
		}
		j++
		return nil
	})

	err = idx.Search(ctx, msgFunc, matcher, from, to, 500, 0, false)
	if err != nil {
		return nil, err
	}

	// extend any label columns o the full length
	var colNames []string
	for ln := range labelCols {
		colNames = append(colNames, ln)
		if len(labelCols[ln].Values) < len(timeCol.Values) {
			vs := labelCols[ln].Values
			vs = append(vs, make([]interface{}, len(timeCol.Values)-len(vs))...)
			col := labelCols[ln]
			col.Values = vs
			labelCols[ln] = col
		}
	}

	res := []grafanasj.TableColumn{
		timeCol,
		textCol,
	}

	sort.Strings(colNames)
	for _, n := range colNames {
		res = append(res, labelCols[n])
	}

	return res, nil
}

// GrafanaAnnotations implements the grafana Simple JSON Annotations request
func (idx *Indexer) GrafanaAnnotations(ctx context.Context, from, to time.Time, query string) ([]grafanasj.Annotation, error) {
	matcher, err := ql.Compile(query)
	if err != nil {
		return nil, err
	}

	offset := uint64(0)
	res := []grafanasj.Annotation{}

	var hits []*logspray.Message
	msgFunc := logspray.MakeFlattenStreamFunc(func(m *logspray.Message) error {
		hits = append(hits, m)
		return nil
	})
	err = idx.Search(ctx, msgFunc, matcher, from, to, 500, offset, true)
	if err != nil {
		return nil, err
	}
	for i := range hits {
		t, _ := ptypes.Timestamp(hits[i].Time)
		tags := []string{}
		for k, v := range hits[i].Labels {
			tags = append(tags, k+":"+v)
		}
		res = append(res, grafanasj.Annotation{
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
