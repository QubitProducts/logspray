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

package logspray

import "fmt"

// MessageFunc represent a function for processing a stream of messages.
type MessageFunc func(m *Message) error

// MakeInjectStreamHeadersFunc creates a message processing
// function that sends Stream headers on demand.
func MakeInjectStreamHeadersFunc(next MessageFunc) MessageFunc {
	seen := map[string]*Message{}
	sent := map[string]struct{}{}
	return func(m *Message) error {
		switch m.ControlMessage {
		case Message_SETHEADER:
			seen[m.StreamID] = m
			return nil
		case Message_STREAMEND:
			delete(seen, m.StreamID)
			delete(sent, m.StreamID)
			return next(m)
		default:
			if _, ok := sent[m.StreamID]; !ok {
				var hdr *Message
				if hdr, ok = seen[m.StreamID]; !ok {
					return fmt.Errorf("filter failed for unknown stream %s", m.StreamID)
				}
				if err := next(hdr); err != nil {
					return err
				}
				sent[m.StreamID] = struct{}{}
			}
			return next(m)
		}
	}
}

// MakeFlattenStreamFunc create a stream processing function that
// reconstructs full messages from a stream of message headers and
// bodies.
func MakeFlattenStreamFunc(next MessageFunc) MessageFunc {
	seen := map[string]*Message{}
	return func(m *Message) error {
		if m.ControlMessage == Message_SETHEADER {
			seen[m.StreamID] = m
			return nil
		}

		var hdr *Message
		var ok bool
		if hdr, ok = seen[m.StreamID]; !ok {
			return fmt.Errorf("filter flatten failed for unknown stream %s", m.StreamID)
		}

		if m.ControlMessage == Message_STREAMEND {
			delete(seen, m.StreamID)
		}

		nmsg := hdr.Copy()
		nmsg.Time = m.Time
		nmsg.Index = m.Index
		nmsg.Text = m.Text
		nmsg.ControlMessage = m.ControlMessage
		for k, v := range m.Labels {
			nmsg.Labels[k] = v
		}
		return next(nmsg)
	}
}
