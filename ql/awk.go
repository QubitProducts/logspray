package ql

import (
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/benhoyt/goawk/interp"
	"github.com/benhoyt/goawk/parser"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
)

type AWKQuery struct {
	labels  map[string]string
	strbuf  []byte
	currOff int

	out chan *logspray.Message

	next *logspray.Message
	err  error
}

func (ar *AWKQuery) labelValue(str string) string {
	return ar.labels[str]
}

func (ar *AWKQuery) setLabelValue(str, v string) {
	ar.labels[str] = v
}

func (ar *AWKQuery) Read(p []byte) (n int, err error) {
	if ar.err != nil {
		return 0, ar.err
	}

	// switch labels here
	if ar.currOff == 0 {
		// read the next line
		ar.labels["thing"] = fmt.Sprintf("%v", time.Now())
	}

	end := ar.currOff + len(p)
	if end > len(ar.strbuf) {
		end = len(ar.strbuf)
	}

	copy(p, ar.strbuf[ar.currOff:end])
	sent := end - ar.currOff

	// we've sent all the data
	if end == len(ar.strbuf) {
		ar.currOff = 0
	}

	return sent, nil
}

func (ar *AWKQuery) Write(p []byte) (n int, err error) {
	if "\n" == string(p) {
		return len(p), nil
	}
	var t *timestamp.Timestamp
	if ar.next != nil && ar.next.Time != nil {
		t = ar.next.Time
	} else {
		t = ptypes.TimestampNow()
	}
	ar.out <- &logspray.Message{
		Time:   t,
		Text:   string(p),
		Labels: ar.labels}
	return len(p), nil
}

func NewAWKQuery(exp, sep string) (*AWKQuery, error) {
	ar := &AWKQuery{
		currOff: 0,
		out:     make(chan *logspray.Message),
		labels:  map[string]string{},
		strbuf:  []byte("this is my message to you\n"),
	}
	fs := map[string]interface{}{
		"label":    ar.labelValue,
		"setLabel": ar.setLabelValue,
	}

	prg, err := parser.ParseProgram([]byte(exp), &parser.ParserConfig{
		Funcs: fs,
	})
	if err != nil {
		return nil, err
	}

	if sep == "" {
		sep = " "
	}

	cfg := &interp.Config{
		Stdin:        ar,
		Output:       ar,
		Funcs:        fs,
		NoExec:       false,
		NoFileWrites: false,
		NoFileReads:  false,
		Vars:         []string{"FS", sep},
	}

	go func() {
		ex, err := interp.ExecProgram(prg, cfg)
		if err != nil {
			log.Printf("interp err: %v", err)
		}
		log.Printf("interp ex: %v", ex)
	}()

	return ar, nil
}

func (ar *AWKQuery) Message() *logspray.Message {
	return ar.next
}

func (ar *AWKQuery) Err() error {
	if errors.Is(ar.err, io.EOF) {
		return nil
	}
	return ar.err
}

func (ar *AWKQuery) Next() bool {
	if ar.err != nil {
		return false
	}

	var ok bool
	ar.next, ok = <-ar.out
	return ok
}
