package client

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/golang/protobuf/ptypes"
)

func doListLabels(ctx context.Context, client logspray.LogServiceClient) {
	st, _ := ptypes.TimestampProto(time.Now().Add(-1 * time.Hour))
	et, _ := ptypes.TimestampProto(time.Now())

	if len(flag.Args()) == 0 {
		lr := &logspray.LabelsRequest{From: st, To: et}
		res, err := client.Labels(ctx, lr)
		if err != nil {
			return
		}
		for _, n := range res.Names {
			fmt.Println(n)
		}
		return
	}

	if len(flag.Args()) > 1 {
		return
	}
	lvr := &logspray.LabelValuesRequest{
		Name:  flag.Args()[0],
		From:  st,
		To:    et,
		Count: int64(count),
	}
	res, err := client.LabelValues(ctx, lvr)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for _, n := range res.Values {
		fmt.Println(n)
	}

	if len(res.Values) == int(count) {
		fmt.Fprint(os.Stderr, "-- maximum request number of values returned, more may exist\n", int(res.TotalHitCount)-len(res.Values))
	}
}
