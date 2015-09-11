package kinesis

import (
	"testing"
	"text/template"
	"time"

	"github.com/gliderlabs/logspout/router"
)

var m = &router.Message{
	Data: "hello",
}

type fakeFlusher struct {
	flushFunc func()
	flushed   chan struct{}
}

func (f *fakeFlusher) flush(b buffer) error {
	if f.flushFunc == nil {
		close(f.flushed)
	} else {
		f.flushFunc()
	}

	return nil
}

var testLimits = limits{
	putRecords:     2,
	putRecordsSize: PutRecordsSizeLimit,
	recordSize:     RecordSizeLimit,
}

var tmpl, _ = template.New("").Parse("abc")

func TestWriter_Flush(t *testing.T) {
	b := newBuffer(tmpl, "abc")
	b.limits = &testLimits

	f := &fakeFlusher{
		flushed: make(chan struct{}),
	}

	w := newWriter(b, f)
	w.ticker = nil

	w.Start()

	w.write(m)
	w.write(m)
	w.write(m)

	select {
	case <-f.flushed:
	case <-time.After(time.Second):
		t.Fatal("Expected flush to be called")
	}
}

func TestWriter_PeriodicFlush(t *testing.T) {
	b := newBuffer(tmpl, "abc")
	b.limits = &testLimits

	f := &fakeFlusher{
		flushed: make(chan struct{}),
	}

	w := newWriter(b, f)

	ticker := make(chan time.Time)
	w.ticker = ticker

	w.Start()
	w.write(m)

	select {
	case ticker <- time.Now():
	default:
		t.Fatal("Couldn't send on stream.ticker channel")
	}

	select {
	case <-f.flushed:
	case <-time.After(time.Second):
		t.Fatal("Expected flush to be called")
	}
}

func TestWriter_BuffersChannelFull(t *testing.T) {
	b := newBuffer(tmpl, "abc")
	b.limits = &testLimits

	f := &fakeFlusher{
		flushed: make(chan struct{}),
		flushFunc: func() {
			<-time.After(time.Minute)
		},
	}

	w := newWriter(b, f)
	w.ticker = nil
	w.buffers = make(chan buffer)
	drop := make(chan struct{})
	w.dropBufferFunc = func(b buffer) {
		close(drop)
	}

	w.Start()
	go func() {
		b := newBuffer(tmpl, "abc")
		w.buffers <- *b
	}()

	w.write(m)
	w.write(m)
	w.write(m)

	select {
	case <-drop:
	case <-time.After(1 * time.Second):
		t.Fatal("Expected buffer to be dropped")
	}
}
