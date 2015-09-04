package kinesis

import (
	"testing"
	"time"
)

type fakeBuffer struct {
	d [][]byte
}

type fakeFlusher struct {
	flushFunc func()
	flushed   chan struct{}
}

func (f *fakeFlusher) flush([][]byte) {
	if f.flushFunc == nil {
		close(f.flushed)
	} else {
		f.flushFunc()
	}
}

func (b *fakeBuffer) data() [][]byte {
	return b.d
}

func (b *fakeBuffer) add(d []byte) {
	b.d = append(b.d, d)
}

func (b *fakeBuffer) full() bool {
	return len(b.d) == 2
}

func TestStream_Flush(t *testing.T) {
	b := &fakeBuffer{}
	f := &fakeFlusher{
		flushed: make(chan struct{}),
	}

	s := newStream()
	s.newBuffer = func() buffer {
		return b
	}
	s.flusher = f
	s.ticker = nil

	s.start()
	s.add([]byte{'h'})
	s.add([]byte{'h'})

	select {
	case <-f.flushed:
	case <-time.After(time.Second):
		t.Fatal("Expected flush to be called")
	}
}

func TestStream_PeriodicFlush(t *testing.T) {
	b := &fakeBuffer{}
	f := &fakeFlusher{
		flushed: make(chan struct{}),
	}

	s := newStream()
	s.newBuffer = func() buffer {
		return b
	}
	s.flusher = f

	ticker := make(chan time.Time)
	s.ticker = ticker

	s.start()
	s.add([]byte{'h'})

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

func TestStream_BuffersChannelFull(t *testing.T) {
	b := &fakeBuffer{}
	f := &fakeFlusher{
		flushed: make(chan struct{}),
		flushFunc: func() {
			<-time.After(time.Minute)
		},
	}

	s := newStream()
	s.newBuffer = func() buffer {
		return b
	}
	s.flusher = f
	s.ticker = nil

	s.buffers = make(chan [][]byte)

	drop := make(chan struct{})
	s.droppedBuffer = func(buffer) {
		close(drop)
	}

	s.start()
	go func() {
		s.buffers <- make([][]byte, 0)
	}()

	s.add([]byte{'h'})
	s.add([]byte{'h'})

	select {
	case <-drop:
	case <-time.After(time.Second):
		t.Fatal("Expected buffer to be dropped")
	}
}
