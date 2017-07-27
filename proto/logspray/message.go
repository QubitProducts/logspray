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

import (
	"fmt"

	"github.com/oklog/ulid"
)

// Copy  copies a message into an existing message struct
// It will allocate a labelset if none
func (m *Message) Copy() (tm *Message) {
	nm := &Message{}

	nm.Time = m.Time
	nm.Labels = make(map[string]string, len(m.Labels))

	for k, v := range m.Labels {
		nm.Labels[k] = v
	}

	//for k := range nm.Labels {
	//		if _, ok := m.Labels[k]; !ok {
	//			delete(nm.Labels, k)
	//		}
	//	}

	nm.Text = m.Text
	nm.Setheader = m.Setheader
	nm.ControlMessage = m.ControlMessage

	return nm
}

// ID Returns a unique string message ID for a message.
// The StreamID and Index must be populated
func (m *Message) ID() (string, error) {
	id, err := ulid.Parse(m.StreamID)
	return fmt.Sprintf("%s-%d", id, m.Index), err
}
