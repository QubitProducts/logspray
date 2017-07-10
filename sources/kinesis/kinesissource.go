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

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/sources"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
)

// MessageReader is a reder log source that reads docker logs.
type MessageReader struct {
	shardIterator *string
	shardId       string
	kinesis       *kinesis.Kinesis
}

// ReadTarget creates a new docker log source
func (w *Watcher) ReadTarget(ctx context.Context, shardId string, fromStart bool) (sources.MessageReader, error) {
	shardIterator, err := shardIterator(ctx, w.kinesis, shardId, fromStart)
	if err != nil {
		return nil, err
	}
	return &MessageReader{shardIterator: shardIterator, shardId: shardId, kinesis: w.kinesis}, err
}

// MessageRead implements the LogSourcer interface
func (mr *MessageReader) MessageRead(ctx context.Context) (*logspray.Message, error) {
	message := logspray.Message{}

	resp, err := mr.kinesis.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: mr.shardIterator,
		Limit:         aws.Int64(1),
	})

	if err != nil {
		return nil, errors.Wrapf(err, "could not get records for shard %v: %v", mr.shardId)
	}

	mr.shardIterator = resp.NextShardIterator

	for _, r := range resp.Records {
		message.Text = string(r.Data)
	}

	return &message, nil
}

func shardIterator(ctx context.Context, svc *kinesis.Kinesis, shardId string, fromStart bool) (*string, error) {
	var shardIteratorType *string
	if fromStart {
		shardIteratorType = aws.String("TRIM_HORIZON")
	} else {
		shardIteratorType = aws.String("LATEST")
	}

	resp, err := svc.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardId),
		ShardIteratorType: shardIteratorType,
		StreamName:        aws.String(stream),
	})
	if err != nil {
		return nil, errors.Wrap(err, "could not get shard iterator")
	}

	return resp.ShardIterator, ctx.Err()
}
