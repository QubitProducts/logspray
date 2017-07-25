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

package reader

import (
	"context"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/naming"

	"github.com/golang/glog"
	"github.com/miekg/dns"
	"github.com/pkg/errors"
)

type srvNameservice struct {
	dns.Client
	nss    []string
	target string

	sync.Mutex
	active map[string]struct{}
}

func newSRVNameservice(nss []string) (*srvNameservice, error) {
	if len(nss) == 0 {
		return nil, errors.New("You must provide at least one name server")
	}
	if len(nss) > 1 {
		glog.Infof("Only the first nameserver will be used at present")
	}
	for _, ns := range nss {
		_, _, err := net.SplitHostPort(ns)
		if err != nil {
			return nil, errors.Wrap(err, "cannot parse nameserver address, should be host:port")
		}
	}

	return &srvNameservice{
		nss: nss,
	}, nil
}

func (b *srvNameservice) Resolve(target string) (naming.Watcher, error) {
	w := &srvNameservice{
		Client: dns.Client{
			Net: "udp",
		},
		nss:    b.nss,
		Mutex:  sync.Mutex{},
		target: target,
	}
	return w, nil
}

func (b *srvNameservice) Next() ([]*naming.Update, error) {
	b.Lock()
	defer b.Unlock()

	for {
		if b.active == nil {
			b.active = map[string]struct{}{}
		} else {
			// This should respect the TTL of returned records
			time.Sleep(time.Second * 5)
		}

		qctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		qres := make(chan map[string]struct{})

		for i := range b.nss {
			go func(ctx context.Context, ns string) {
				m := new(dns.Msg)
				m.SetQuestion(b.target, dns.TypeSRV)

				in, _, err := b.Exchange(m, ns)
				if err != nil {
					return
				}

				as := map[string]string{}
				for _, rr := range in.Extra {
					if ar, ok := rr.(*dns.A); ok {
						as[ar.Hdr.Name] = ar.A.String()
					}
				}

				targets := map[string]struct{}{}
				for _, a := range in.Answer {
					if srvr, ok := a.(*dns.SRV); ok {
						if ip, ok := as[srvr.Target]; ok {
							targets[net.JoinHostPort(ip, strconv.Itoa(int(srvr.Port)))] = struct{}{}
						} else {
							glog.Errorf("SRV target not in extras section, we don't deal with this yet")
						}
					}
				}
				if len(targets) == 0 {
					glog.Errorf("No DNS responses from %s", ns)
				}
				select {
				case qres <- targets:
				case <-ctx.Done():
				}
			}(qctx, b.nss[i])
		}

		res := []*naming.Update{}
		newtargets := map[string]struct{}{}
		select {
		case newtargets = <-qres:
		case <-qctx.Done():
			return res, qctx.Err()
		}

		newactive := map[string]struct{}{}
		for n := range newtargets {
			if _, ok := b.active[n]; !ok {
				glog.Info("Found new server: ", n)
				res = append(res, &naming.Update{Addr: n, Op: naming.Add})
			}
			newactive[n] = struct{}{}
		}

		for n := range b.active {
			if _, ok := newtargets[n]; !ok {
				glog.Info("Lost server: ", n)
				res = append(res, &naming.Update{Addr: n, Op: naming.Delete})
			}
		}

		b.active = newactive

		if len(res) > 0 {
			// There are changes, return
			return res, nil
		}
	}
}

func (b *srvNameservice) Close() {
}
