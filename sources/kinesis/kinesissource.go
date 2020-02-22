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
	"fmt"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/sources"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pkg/errors"
)

// MessageReader is a log source that reads from kinesis shard.
type MessageReader struct {
	shardIterator   *string
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
	messagesChannel := make(chan Message, w.messagesChannelSize)

	msgReader := &MessageReader{
		shardIterator:   shardIterator,
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
	shardIteratorType := aws.String("LATEST")
	if fromStart {
		shardIteratorType = aws.String("TRIM_HORIZON")
	}

	resp, err := w.kinesis.GetShardIteratorWithContext(ctx, &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardId),
		ShardIteratorType: shardIteratorType,
		StreamName:        aws.String(w.stream),
	})
	if err != nil {
		return nil, fmt.Errorf("could not get shard iterator, %w", err)
	}

	return resp.ShardIterator, ctx.Err()
}

func (mr *MessageReader) startReadingFromKinesis(ctx context.Context) {
	defer close(mr.messagesChannel)
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
				glog.Error(gzipErr)
				continue
			}

			kinesisMsg := KinesisMessage{}
			buf := new(bytes.Buffer)
			buf.ReadFrom(gzipReader)
			marshlingErr := json.Unmarshal(buf.Bytes(), &kinesisMsg)
			gzipReader.Close()
			if marshlingErr != nil {
				glog.Error(marshlingErr)
				continue
			}

			for _, log := range kinesisMsg.LogEvents {
				logsprayMsg, parseErr := parseLog(log)

				if parseErr != nil {
					glog.Error(parseErr)
					continue
				}

				msg := Message{logsprayMsg, nil}

				select {
				case <-ctx.Done():
					return
				case mr.messagesChannel <- msg:
				}
			}
		}
	}
}

func parseLog(log interface{}) (*logspray.Message, error) {
	logEvents, ok := log.(map[string]interface{})
	if !ok {
		return nil, errors.New("Failed to parse kinesis message")
	}

	message, ok := logEvents["message"].(string)
	if !ok {
		return nil, errors.New("Failed to parse message field in kinesis message")
	}

	milliSeconds := int64(logEvents["timestamp"].(float64))

	logsprayMsg := &logspray.Message{}
	logsprayMsg.Text = message

	logsprayMsg.Time = &timestamp.Timestamp{
		Nanos:   int32((milliSeconds % 1000) * 1000000),
		Seconds: milliSeconds / 1000,
	}

	return logsprayMsg, nil
}
