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

package indexer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/QubitProducts/logspray/ql"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes"
)

type readWriteAtCloser interface {
	io.WriterAt
	io.ReaderAt
	io.Closer
}

// ShardFile represents an individual stream of data in a shard.
type ShardFile struct {
	fn string
	id string

	sync.RWMutex
	file        readWriteAtCloser // file used wh
	headersSent bool
	labels      map[string]string
	offset      int64 // This is the current end of the file
}

// ShardFileIterator retries messages from a shar file
type ShardFileIterator struct {
	file    *os.File
	hdr     *logspray.Message
	sentHdr bool
	sr      *io.SectionReader
	err     error
	eof     int64
	off     int64

	next *logspray.Message
}

// Header
func (sfi *ShardFileIterator) Header() *logspray.Message {
	return sfi.hdr
}

// Next
func (sfi *ShardFileIterator) Next() bool {
	if sfi.err != nil {
		return false
	}

	if !sfi.sentHdr {
		sfi.next = sfi.hdr
		sfi.sentHdr = true
		return true
	}

	sfi.off, _ = sfi.sr.Seek(0, io.SeekCurrent)
	sfi.next, sfi.err = readMessageFromFile(sfi.sr)
	if sfi.err != nil {
		return false
	}

	return true
}

// Offset returns the current offset from the start of the file
func (sfi *ShardFileIterator) Offset() int64 {
	return sfi.off
}

// Err returns any error
func (sfi *ShardFileIterator) Err() error {
	if errors.Is(sfi.err, io.EOF) {
		return nil
	}
	return sfi.err
}

// Close
func (sfi *ShardFileIterator) Close() error {
	return sfi.file.Close()
}

// Message()
func (sfi *ShardFileIterator) Message() *logspray.Message {
	return sfi.next
}

// OpenShardFileIterator opens a file
func OpenShardFileIterator(fn string) (*ShardFileIterator, error) {
	file, err := os.Open(fn)
	if err != nil {
		return nil, err
	}

	eof, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	sr := io.NewSectionReader(file, 0, eof)

	hdr, err := readMessageFromFile(sr)
	if err != nil {
		return nil, fmt.Errorf("failed to read file header %s, %w", fn, err)
	}

	return &ShardFileIterator{
		file: file,
		sr:   sr,
		hdr:  hdr,
		eof:  eof,
	}, nil
}

// Search searches the shard file for messages in the provided time range,
// matched by matcher, and passes them to msgFunc. If the reverse is true the
// file will be searched in reverse order
func (s *ShardFile) Search(ctx context.Context, msgFunc logspray.MessageFunc, matcher ql.MatchFunc, from, to time.Time, reverse bool) error {
	var sr *io.SectionReader
	if s.file != nil { // this is an active shard file
		s.RLock()
		glog.V(3).Infof("Searching active shard file %v from %v to %v", s.fn, from, to)
		sr = io.NewSectionReader(s.file, 0, s.offset)
		s.RUnlock()
	} else { // this is an archived shard file
		s.Lock()
		glog.V(3).Infof("Searching archived shard file %v from %v to %v", s.fn, from, to)
		file, err := os.Open(s.fn)
		if err != nil {
			s.Unlock()
			return err
		}
		defer file.Close()
		if s.offset == 0 {
			s.offset, err = file.Seek(0, io.SeekEnd)
			if err != nil {
				s.Unlock()
				return err
			}
			_, err = file.Seek(0, io.SeekStart)
			if err != nil {
				s.Unlock()
				return err
			}
		}
		sr = io.NewSectionReader(file, 0, s.offset)
		s.Unlock()
	}

	hdr, err := readMessageFromFile(sr)
	if err != nil {
		return fmt.Errorf("failed to read file header %s, %w", s.fn, err)
	}
	err = msgFunc(hdr)
	if err != nil {
		return err
	}
	if matcher != nil {
		if !matcher(hdr, nil, true) {
			return nil
		}
	}
	if reverse {
		sr.Seek(0, io.SeekEnd)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			var nmsg *logspray.Message
			var err error
			if !reverse {
				nmsg, err = readMessageFromFile(sr)
			} else {
				nmsg, err = readMessageFromFileEnd(sr)
			}

			if err == io.EOF {
				return nil
			}
			if err != nil {
				return fmt.Errorf("failed to read message from %s, %w", s.fn, err)
			}

			if nmsg.Time == nil {
				continue
			}
			t, _ := ptypes.Timestamp(nmsg.Time)
			if t.Before(from) || t.After(to) {
				continue
			}
			if matcher != nil {
				if !matcher(hdr, nmsg, false) {
					continue
				}
			}
			err = msgFunc(nmsg)
			if err != nil {
				return err
			}
		}
	}
}

func (s *ShardFile) writeMessageToFile(ctx context.Context, m *logspray.Message) error {
	s.Lock()
	defer s.Unlock()
	var err error

	if s.file == nil {
		dir := filepath.Dir(s.fn)
		err := os.MkdirAll(dir, 0777)
		newWriter, err := os.Create(s.fn)
		if err != nil {
			return err
		}
		hm := &logspray.Message{
			ControlMessage: logspray.Message_SETHEADER,
			StreamID:       m.StreamID,
			Time:           m.Time,
			Labels:         s.labels,
		}
		sz, err := writePBMessageToFile(newWriter, 0, hm)
		if err != nil {
			return fmt.Errorf("failed writing file header, %w", err)
		}
		s.file = newWriter
		s.offset = int64(sz)
	}

	sz, err := writePBMessageToFile(s.file, s.offset, m)
	if err != nil {
		return fmt.Errorf("failed writing to %s, %w", s.fn, err)
	}
	s.offset += int64(sz)

	return err
}

func writePBMessageToFile(w io.WriterAt, offset int64, msg *logspray.Message) (uint32, error) {
	if w == nil {
		return 0, nil
	}
	bs, err := proto.Marshal(msg)
	if err != nil {
		return 0, fmt.Errorf("marshal protobuf failed, %w", err)
	}

	// We use a uin16 for the size field, we'll drop anything that's
	// too big here.
	if len(bs) > 65535 {
		return 0, fmt.Errorf("marshaled protobuf is %d bytes too large", len(bs)-65535)
	}

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, uint16(len(bs)))
	buf.Write(bs)
	binary.Write(buf, binary.LittleEndian, uint16(len(bs)))

	n, err := w.WriteAt(buf.Bytes(), offset)
	if n != len(buf.Bytes()) || err != nil {
		return 0, fmt.Errorf("write pb to file failed, %w", err)
	}

	return uint32(n), nil
}

func readMessageFromFile(r *io.SectionReader) (*logspray.Message, error) {
	szbs := make([]byte, 2)
	szbuf := bytes.NewBuffer(szbs)
	n, err := r.Read(szbs)
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("failed read message size header, %w", err)
	}
	if n != len(szbs) {
		return nil, fmt.Errorf("short read for message size header")
	}

	szbytes := uint16(0)
	err = binary.Read(szbuf, binary.LittleEndian, &szbytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode message size, %w", err)
	}

	// We read the protobuf, and the trailing size bytes.
	pbbs := make([]byte, szbytes+2)
	n, err = r.Read(pbbs)
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("failed read message, %w", err)
	}
	if n != len(pbbs) {
		return nil, fmt.Errorf("short read for message")
	}

	trailSzbytes := uint16(0)
	szbuf = bytes.NewBuffer(pbbs[len(pbbs)-2:])
	err = binary.Read(szbuf, binary.LittleEndian, &trailSzbytes)
	if err != nil {
		return nil, fmt.Errorf("failed reading trail size, %w", err)
	}

	if szbytes != trailSzbytes {
		return nil, fmt.Errorf("header and trail size mismatch, %w", err)
	}

	msg := logspray.Message{}
	err = proto.Unmarshal(pbbs[:len(pbbs)-2], &msg)
	if err != nil {
		return nil, fmt.Errorf("failed to umarshal log message proto, %w", err)
	}

	return &msg, nil
}

// readMessageFromFileEnd tries to read a message before the current position
// of the io.ReadSeeker
func readMessageFromFileEnd(r *io.SectionReader) (*logspray.Message, error) {
	if off, _ := r.Seek(0, io.SeekCurrent); off == 0 {
		return nil, io.EOF
	}

	_, err := r.Seek(-2, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("could not seek back to message trailer, %w", err)
	}

	szbs := make([]byte, 2)
	szbuf := bytes.NewBuffer(szbs)
	n, err := r.Read(szbs)
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("failed read message size trailer, %w", err)
	}
	if n != len(szbs) {
		return nil, errors.New("short read for message size trailer")
	}

	szbytes := uint16(0)
	err = binary.Read(szbuf, binary.LittleEndian, &szbytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode message size trailer, %w", err)
	}

	start, err := r.Seek(-1*int64(4+szbytes), io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("could not seek back to message header, %w", err)
	}

	msg, err := readMessageFromFile(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read message, %w", err)
	}

	_, err = r.Seek(start, io.SeekStart)

	return msg, nil
}

// Close the backing files for a shard
func (s *ShardFile) Close() error {
	file := s.file
	s.file = nil
	if file != nil {
		return file.Close()
	}
	return nil
}
