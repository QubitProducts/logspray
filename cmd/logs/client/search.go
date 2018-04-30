package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/golang/protobuf/ptypes"
)

func doSearch(ctx context.Context, client logspray.LogServiceClient, args []string) {
	st, _ := ptypes.TimestampProto(time.Time(startTime))
	et, _ := ptypes.TimestampProto(time.Time(endTime))

	sr := &logspray.SearchRequest{
		From:   st,
		To:     et,
		Offset: offset,
		Count:  count,
		Query:  strings.Join(args, " "),
	}
	res, err := client.SearchStream(ctx, sr)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	lastLabels := map[string]string{}
	msgFunc := logspray.MakeFlattenStreamFunc(func(m *logspray.Message) error {
		if showLabels {
			if !reflect.DeepEqual(m.Labels, lastLabels) {
				line := "--"
				for k, v := range m.Labels {
					line = line + fmt.Sprintf(" %s=%s", k, v)
				}
				fmt.Println(line)
			}
		}
		lastLabels = m.Labels
		outputMessage(m)
		return nil
	})
	for {
		m, err := res.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatal(err.Error())
		}
		msgFunc(m)
	}
}
