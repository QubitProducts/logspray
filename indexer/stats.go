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

import "github.com/prometheus/client_golang/prometheus"

// Describe implements the prometheus describe interfaces for metric
// collection
func (i *Indexer) Describe(ch chan<- *prometheus.Desc) {
	// Must send one description, or the registry panics
	ch <- prometheus.NewDesc("dummy", "dummy", nil, nil)
}

// Collect implements the prom metrics collection Collector
// interface
func (i *Indexer) Collect(ch chan<- prometheus.Metric) {
	i.RLock()
	defer i.RUnlock()

	if i.activeShard == nil {
		return
	}

	/*
		activeSpace := int64(0)
		path := i.activeShard.bindex.Name()
		// We should probably unlock before doinng this walk
		filepath.Walk(path, func(p string, i os.FileInfo, err error) error {
			if !i.IsDir() {
				activeSpace += i.Size()
			}
			return nil
		})

		ls := []string{"store"}
		lvs := []string{"goleveldb"}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc("logspray_index_kv_disk_used_bytes", "Amount of disk space used by the current index.", ls, nil),
			prometheus.GaugeValue,
			float64(activeSpace),
			lvs...,
		)
	*/
}
