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
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Data represents a Grafana Simple JSON data point
type Data struct {
	Time  time.Time
	Value float64
}

// GrafanaSimpleJSON describes requirements for serving a Grafana Simple JSON
// endpoint.
type GrafanaSimpleJSON interface {
	GrafanaQuery(ctx context.Context, from, to time.Time, interval time.Duration, maxDPs int, targets []string) (map[string][]Data, error)
	GrafanaAnnotations(ctx context.Context, from, to time.Time, query string) ([]Annotation, error)
	GrafanaSearch(ctx context.Context, target string) ([]string, error)
}

func (*sjcSrc) HandleRoot(w http.ResponseWriter, r *http.Request) {
	addCORS(w)
	if r.URL.Path != "/" {
		http.Error(w, "File not found", http.StatusNotFound)
	}
	w.Write([]byte("OK"))
}

func addCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Headers", "accept, content-type")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

type simpleJSONTime time.Time

func (sjt *simpleJSONTime) MarshalJSON() ([]byte, error) {
	out := time.Time(*sjt).Format(time.RFC3339Nano)
	return json.Marshal(out)
}

func (sjt *simpleJSONTime) UnmarshalJSON(injs []byte) error {
	in := ""
	err := json.Unmarshal(injs, &in)
	if err != nil {
		return err
	}

	t, err := time.Parse(time.RFC3339Nano, in)
	if err != nil {
		return err
	}

	*sjt = simpleJSONTime(t)

	return nil
}

type simpleJSONDuration time.Duration

func (sjd *simpleJSONDuration) MarshalJSON() ([]byte, error) {
	out := time.Duration(*sjd).String()
	return json.Marshal(out)
}

func (sjd *simpleJSONDuration) UnmarshalJSON(injs []byte) error {
	in := ""
	err := json.Unmarshal(injs, &in)
	if err != nil {
		return err
	}

	d, err := time.ParseDuration(in)
	if err != nil {
		return err
	}

	*sjd = simpleJSONDuration(d)

	return nil
}

type simpleJSONRawRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type simpleJSONRange struct {
	From simpleJSONTime     `json:"from"`
	To   simpleJSONTime     `json:"to"`
	Raw  simpleJSONRawRange `json:"raw"`
}

type simpleJSONTarget struct {
	Target string `json:"target"`
	RefID  string `json:"refId"`
}

/*
{
  "panelId": 1,
  "range": {
    "from": "2016-10-31T06:33:44.866Z",
    "to": "2016-10-31T12:33:44.866Z",
    "raw": {
      "from": "now-6h",
      "to": "now"
    }
  },
  "rangeRaw": {
    "from": "now-6h",
    "to": "now"
  },
  "interval": "30s",
  "intervalMs": 30000,
  "targets": [
     { "target": "upper_50", refId: "A" },
     { "target": "upper_75", refId: "B" }
  ],
  "format": "json",
  "maxDataPoints": 550
}
*/

type simpleJSONQuery struct {
	PanelID       int                `json:"panelId"`
	Range         simpleJSONRange    `json:"range"`
	RangeRaw      simpleJSONRawRange `json:"rangeRaw"`
	Interval      simpleJSONDuration `json:"interval"`
	IntervalMS    int                `json:"intervalMs"`
	Targets       []simpleJSONTarget `json:"targets"`
	Format        string             `json:"format"`
	MaxDataPoints int                `json:"maxDataPoints"`
}

/*
[
  {
    "target":"upper_75", // The field being queried for
    "datapoints":[
      [622,1450754160000],  // Metric value as a float , unixtimestamp in milliseconds
      [365,1450754220000]
    ]
  },
  {
    "target":"upper_90",
    "datapoints":[
      [861,1450754160000],
      [767,1450754220000]
    ]
  }
]
*/

type simpleJSONDPTime time.Time

func (sjdpt *simpleJSONDPTime) MarshalJSON() ([]byte, error) {
	out := time.Time(*sjdpt).UnixNano() / 1000000
	return json.Marshal(out)
}

func (sjdpt *simpleJSONDPTime) UnmarshalJSON(injs []byte) error {
	in := int64(0)
	err := json.Unmarshal(injs, &in)
	if err != nil {
		return err
	}

	t := time.Unix(0, in*1000000)

	*sjdpt = simpleJSONDPTime(t)

	return nil
}

type simpleJSONDataPoint struct {
	Value float64          `json:"value"`
	Time  simpleJSONDPTime `json:"time"`
}

func (sjdp *simpleJSONDataPoint) MarshalJSON() ([]byte, error) {
	out := [2]float64{sjdp.Value, float64(time.Time(sjdp.Time).UnixNano() / 1000000)}
	return json.Marshal(out)
}

func (sjdp *simpleJSONDataPoint) UnmarshalJSON(injs []byte) error {
	in := [2]float64{}
	err := json.Unmarshal(injs, &in)
	if err != nil {
		return err
	}
	*sjdp = simpleJSONDataPoint{}
	sjdp.Value = in[0]
	sjdp.Time = simpleJSONDPTime(time.Unix(0, int64(in[1])*1000000))

	return nil
}

type simpleJSONData struct {
	Target     string                `json:"target"`
	DataPoints []simpleJSONDataPoint `json:"datapoints"`
}

func (sjc *sjcSrc) HandleQuery(w http.ResponseWriter, r *http.Request) {
	if !sjc.checkAuth(w, r) {
		w.Header().Set("WWW-Authenticate", `Basic realm="logsray-reader"`)
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
		return
	}

	addCORS(w)

	req := simpleJSONQuery{}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ts := []string{}
	for _, t := range req.Targets {
		ts = append(ts, t.Target)
	}

	resp, err := sjc.GrafanaQuery(
		r.Context(),
		time.Time(req.Range.From),
		time.Time(req.Range.To),
		time.Duration(req.Interval),
		req.MaxDataPoints,
		ts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	out := make([]simpleJSONData, len(resp))
	i := 0
	for sn, vs := range resp {
		sort.Slice(vs, func(i, j int) bool { return vs[i].Time.Before(vs[j].Time) })
		sout := simpleJSONData{Target: sn}
		for _, v := range vs {
			sout.DataPoints = append(sout.DataPoints, simpleJSONDataPoint{
				Time:  simpleJSONDPTime(v.Time),
				Value: v.Value,
			})
		}
		out[i] = sout
		i++
	}

	bs, err := json.Marshal(out)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

/*
{
  "range": {
    "from": "2016-04-15T13:44:39.070Z",
    "to": "2016-04-15T14:44:39.070Z"
  },
  "rangeRaw": {
    "from": "now-1h",
    "to": "now"
  },
  "annotation": {
    "name": "deploy",
    "datasource": "Simple JSON Datasource",
    "iconColor": "rgba(255, 96, 96, 1)",
    "enable": true,
    "query": "#deploy"
  }
}
*/

type simpleJSONAnnotation struct {
	Name       string `json:"name"`
	Datasource string `json:"datasource"`
	Query      string `json:"query"`
	Enable     bool   `json:"enable"`
	IconColor  string `json:"iconColor"`
}

// Annotation describes a Grafana Simple JSON annotation
type Annotation struct {
	Time  simpleJSONDPTime `json:"time"`
	Title string           `json:"title"`
	Text  string           `json:"text"`
	Tags  []string         `json:"tags"`
}

type simpleJSONAnnotationResponse struct {
	ReqAnnotation simpleJSONAnnotation `json:"annotation"`
	Annotation
}

type simpleJSONAnnotationsQuery struct {
	Range      simpleJSONRange      `json:"range"`
	RangeRaw   simpleJSONRawRange   `json:"rangeRaw"`
	Annotation simpleJSONAnnotation `json:"annotation"`
}

func (sjc *sjcSrc) HandleAnnotations(w http.ResponseWriter, r *http.Request) {
	addCORS(w)

	if r.Method == http.MethodOptions {
		w.Write([]byte("Allow: POST,OPTIONS"))
		return
	}

	if !sjc.checkAuth(w, r) {
		w.Header().Set("WWW-Authenticate", `Basic realm="logsray-reader"`)
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
		return
	}

	req := simpleJSONAnnotationsQuery{}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := []simpleJSONAnnotationResponse{}
	anns, err := sjc.GrafanaAnnotations(r.Context(), time.Time(req.Range.From), time.Time(req.Range.To), req.Annotation.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for i := range anns {
		resp = append(resp, simpleJSONAnnotationResponse{
			ReqAnnotation: req.Annotation,
			Annotation:    anns[i],
		})
	}

	bs, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

type simpleJSONSearchQuery struct {
	Target string
}

func (sjc *sjcSrc) HandleSearch(w http.ResponseWriter, r *http.Request) {
	if !sjc.checkAuth(w, r) {
		w.Header().Set("WWW-Authenticate", `Basic realm="logsray-reader"`)
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
		return
	}

	addCORS(w)

	req := simpleJSONSearchQuery{}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := sjc.GrafanaSearch(r.Context(), req.Target)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	bs, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

type sjcOpt func(*sjcSrc) error
type sjcSrc struct {
	GrafanaSimpleJSON
	user, pass string
}

func (sjc *sjcSrc) checkAuth(w http.ResponseWriter, r *http.Request) bool {
	if sjc.user == "" {
		return true
	}

	s := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(s) != 2 {
		return false
	}

	b, err := base64.StdEncoding.DecodeString(s[1])
	if err != nil {
		return false
	}

	pair := strings.SplitN(string(b), ":", 2)
	if len(pair) != 2 {
		return false
	}

	return pair[0] == sjc.user && pair[1] == sjc.pass
}

// NewSimpleJSONHandler returns a HTTP handler that uses data
// from src to serve Grafana SimpleJSON queries.
func NewSimpleJSONHandler(src GrafanaSimpleJSON, opts ...sjcOpt) *sjcSrc {
	sjc := &sjcSrc{src, "", ""}
	for _, o := range opts {
		if err := o(sjc); err != nil {
			panic(err)
		}
	}

	return sjc
}

// WithGrafanaBasicAuth sets basic auth params for the simpleJson endpoint
func WithGrafanaBasicAuth(user, pass string) sjcOpt {
	return func(sjc *sjcSrc) error {
		sjc.user, sjc.pass = user, pass
		return nil
	}
}

type stubIndex struct{}

func (stubIndex) Query(from, to time.Time, interval time.Duration, maxDPs int, targets []string) (map[string][]Data, error) {
	return map[string][]Data{
		"thing1": []Data{{Time: time.Now(), Value: 4.0}},
		"thing2": []Data{{Time: time.Now(), Value: 5.0}},
	}, nil
}

func (stubIndex) Annotations(from, to time.Time, query string) ([]Annotation, error) {
	ann := Annotation{Time: simpleJSONDPTime(time.Now().Add(-5 * time.Minute)), Title: "thing happened", Tags: []string{"some", "tag", "spose"}}
	ann.Text = "more text in here"
	return []Annotation{ann}, nil
}

func (stubIndex) Search(target string) ([]string, error) {
	return []string{"thing1", "thing2"}, nil
}
