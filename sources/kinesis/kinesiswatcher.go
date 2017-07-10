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

package kinesis

import (
	"context"
	"flag"

	"github.com/QubitProducts/logspray/sources"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
)

// Watcher watches for files being added and removed from a filesystem
type Watcher struct {
	ups     chan []*sources.Update
	kinesis *kinesis.Kinesis
}

// Next should be called each time you wish to watch for an update.
func (w *Watcher) Next(ctx context.Context) ([]*sources.Update, error) {
	if w.ups == nil {
		w.ups = make(chan []*sources.Update, 1)

		w.initialize()

		shards, err := w.getShardIds()
		if err != nil {
			return nil, err
		}

		logSources := []*sources.Update{}
		for _, shard := range shards {
			logSources = append(logSources, &sources.Update{Action: sources.Add, Target: shard})
		}

		w.ups <- logSources
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case up := <-w.ups:
		return up, nil
	}
}

var (
	stream = "/aws/lambda/esp-production"
)

func New() *Watcher {
	flag.StringVar(&stream, "kinesis.stream", stream, "The kinesis stream to read from")
	flag.Set("logtostderr", "true")
	flag.Parse()

	return &Watcher{}
}

func (w *Watcher) initialize() error {
	awsSession, err := session.NewSession()
	if err != nil {
		return errors.Wrap(err, "could not create AWS session")
	}

	w.kinesis = kinesis.New(awsSession)

	return nil
}

func (w *Watcher) getShardIds() ([]string, error) {
	resp, err := w.kinesis.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(stream),
	})
	if err != nil {
		return nil, errors.Wrap(err, "could not list Kinesis streams")
	}

	shardIds := make([]string, len(resp.StreamDescription.Shards))
	for i, shard := range resp.StreamDescription.Shards {
		shardIds[i] = *shard.ShardId
	}
	return shardIds, nil
}
