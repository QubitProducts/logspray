package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/QubitProducts/logspray/proto/logspray"
)

func doFollow(ctx context.Context, client logspray.LogServiceClient) {
	siglabels := map[string]bool{"job": true}
	initial := true

Reconnect:
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !initial {
			fmt.Fprintln(os.Stderr, "-- Reconnecting")
		} else {
			initial = false
		}
		tr := &logspray.TailRequest{
			Query: strings.Join(flag.Args(), " "),
		}

		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		cr, err := client.Tail(cctx, tr)
		if err != nil {
			fatalf("could not call Tail, %s", err.Error())
			return
		}

		hdrs := map[string]*logspray.Message{}

		lastSigLabels := map[string]string{}
		for l := range siglabels {
			lastSigLabels[l] = ""
		}
		newSigLabels := map[string]string{}
		for {
			m, err := cr.Recv()
			if err != nil {
				fmt.Fprintf(os.Stderr, "-- ERROR %v\n", err)
				continue Reconnect
			}
			if m == nil {
				return
			}

			switch m.ControlMessage {
			case logspray.Message_OK:
				continue
			case logspray.Message_ERROR:
				fmt.Fprintf(os.Stderr, "-- ERROR %s\n", m.Text)
				continue
			case logspray.Message_SETHEADER:
				hdrs[m.StreamID] = m
				continue
			case logspray.Message_STREAMEND:
				delete(hdrs, m.StreamID)
				continue
			}

			hdr, _ := hdrs[m.StreamID]

			if m.Labels == nil {
				m.Labels = map[string]string{}
			}
			if hdr != nil {
				for l, v := range hdr.Labels {
					if _, ok := m.Labels[l]; !ok {
						m.Labels[l] = v
					}
				}
			}
			newlabels := false
			for lsk, lsv := range lastSigLabels {
				if mv, ok := m.Labels[lsk]; ok {
					if mv != lsv {
						newlabels = true
					}
					newSigLabels[lsk] = mv
				}
			}
			if newlabels {
				line := "--"
				if !*showLabels {
					for k, v := range newSigLabels {
						line = line + fmt.Sprintf(" %s=%s", k, v)
					}
				} else {
					for k, v := range m.Labels {
						line = line + fmt.Sprintf(" %s=%s", k, v)
					}
				}
				fmt.Fprintln(os.Stderr, line)
				lastSigLabels = newSigLabels
			}
			outputMessage(m)
		}
	}
}
