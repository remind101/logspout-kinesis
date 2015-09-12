package kinesis

import (
	"testing"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

var m = &router.Message{
	Data: "hello",
}

type fakeFlusher struct {
	flushFunc func()
	flushed   chan struct{}
}

func (f *fakeFlusher) flush(kinesis.PutRecordsInput) {
	if f.flushFunc == nil {
		close(f.flushed)
	} else {
		f.flushFunc()
	}
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

	w.start()

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

	w.start()
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

//func TestWriter_EmptyBuffer(t *testing.T) {}
// test that w.flusher.flush() is never called?

// func TestFlusher_EmptyBuffer(t *testing.T) {
// 	b := newBuffer(tmpl, "abc")
// 	f := newFlusher(nil)
// 	w := newWriter(b, f)

// 	err := w.flusher.flush(*b)
// 	if assert.Error(t, err, "An empty buffer error was expected.") {
// 		assert.Equal(t, err, &ErrEmptyBuffer{s: "abc"})
// 	}
// }
