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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/sources"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
)

// MessageReader is a log source that reads from kinesis shard.
type MessageReader struct {
	shardIterator   *string
	shardId         string
	kinesis         *kinesis.Kinesis
	stream          string
	messagesChannel chan Message
}

type Message struct {
	logsprayMsg *logspray.Message
	error       error
}

type KinesisMessage struct {
	LogEvents []interface{} `json:"logEvents"`
}

// ReadTarget creates a new log source from a kinesis shard
func (w *Watcher) ReadTarget(ctx context.Context, shardId string, fromStart bool) (sources.MessageReader, error) {
	shardIterator, err := w.shardIterator(ctx, shardId, fromStart)
	if err != nil {
		return nil, err
	}
	messagesChannel := make(chan Message, 100)

	msgReader := &MessageReader{
		shardIterator:   shardIterator,
		shardId:         shardId,
		kinesis:         w.kinesis,
		stream:          w.stream,
		messagesChannel: messagesChannel,
	}

	go msgReader.startReadingFromKinesis(ctx)

	return msgReader, err
}

// MessageRead implements the LogSourcer interface
func (mr *MessageReader) MessageRead(ctx context.Context) (*logspray.Message, error) {
	message := <-mr.messagesChannel
	return message.logsprayMsg, message.error
}

func (w *Watcher) shardIterator(ctx context.Context, shardId string, fromStart bool) (*string, error) {
	var shardIteratorType *string
	if fromStart {
		shardIteratorType = aws.String("TRIM_HORIZON")
	} else {
		shardIteratorType = aws.String("LATEST")
	}

	resp, err := w.kinesis.GetShardIteratorWithContext(ctx, &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardId),
		ShardIteratorType: shardIteratorType,
		StreamName:        aws.String(w.stream),
	})
	if err != nil {
		return nil, errors.Wrap(err, "could not get shard iterator")
	}

	return resp.ShardIterator, ctx.Err()
}

func (mr *MessageReader) startReadingFromKinesis(ctx context.Context) {
	for {
		resp, kinesisErr := mr.kinesis.GetRecordsWithContext(ctx, &kinesis.GetRecordsInput{
			ShardIterator: mr.shardIterator,
			Limit:         aws.Int64(64),
		})

		if kinesisErr != nil {
			mr.messagesChannel <- Message{nil, kinesisErr}
			continue
		}

		mr.shardIterator = resp.NextShardIterator

		for _, r := range resp.Records {
			// decompressing data from kinesis
			reader := bytes.NewReader(r.Data)
			gzipReader, gzipErr := gzip.NewReader(reader)
			if gzipErr != nil {
				continue
			}

			kinesisMsg := KinesisMessage{}
			buf := new(bytes.Buffer)
			buf.ReadFrom(gzipReader)
			marshlingErr := json.Unmarshal(buf.Bytes(), &kinesisMsg)
			gzipReader.Close()
			if marshlingErr != nil {
				continue
			}

			for _, log := range kinesisMsg.LogEvents {
				logsprayMsg := &logspray.Message{}
				logsprayMsg.Text = parseLog(log)
				logsprayMsg.Labels = map[string]string{
					"job":             "kinesis",
					"stream_name":     mr.stream,
					"stream_shard_id": mr.shardId,
				}

				msg := Message{logsprayMsg, nil}

				select {
				case <-ctx.Done():
					close(mr.messagesChannel)
					return
				case mr.messagesChannel <- msg:
				}
			}
		}
	}
}

func parseLog(log interface{}) string {
	logEvents := log.(map[string]interface{})
	return logEvents["message"].(string)
}
