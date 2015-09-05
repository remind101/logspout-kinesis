package kineprod

import (
	"testing"
	"time"
)

type fakeBuffer struct {
	d []byte
}

type fakeFlusher struct {
	flushFunc func()
	flushed   chan struct{}
}

func (f *fakeFlusher) flush([]byte) {
	if f.flushFunc == nil {
		close(f.flushed)
	} else {
		f.flushFunc()
	}
}

func (b *fakeBuffer) data() []byte {
	return b.d
}

func (b *fakeBuffer) add(d []byte) {
	b.d = append(b.d, d...)
}

func (b *fakeBuffer) full() bool {
	return len(b.d) == 2
}

func TestWriter_Flush(t *testing.T) {
	b := &fakeBuffer{}
	f := &fakeFlusher{
		flushed: make(chan struct{}),
	}

	w := newWriter()
	w.newBuffer = func() buffer {
		return b
	}
	w.flusher = f
	w.ticker = nil

	w.start()
	w.Write([]byte{'h'})
	w.Write([]byte{'h'})

	select {
	case <-f.flushed:
	case <-time.After(time.Second):
		t.Fatal("Expected flush to be called")
	}
}

func TestWriter_PeriodicFlush(t *testing.T) {
	b := &fakeBuffer{}
	f := &fakeFlusher{
		flushed: make(chan struct{}),
	}

	w := newWriter()
	w.newBuffer = func() buffer {
		return b
	}
	w.flusher = f

	ticker := make(chan time.Time)
	w.ticker = ticker

	w.start()
	w.Write([]byte{'h'})

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
	b := &fakeBuffer{}
	f := &fakeFlusher{
		flushed: make(chan struct{}),
		flushFunc: func() {
			<-time.After(time.Minute)
		},
	}

	w := newWriter()
	w.newBuffer = func() buffer {
		return b
	}
	w.flusher = f
	w.ticker = nil

	w.buffers = make(chan []byte)

	drop := make(chan struct{})
	w.droppedBuffer = func(buffer) {
		close(drop)
	}

	w.start()
	go func() {
		w.buffers <- make([]byte, 0)
	}()

	w.Write([]byte{'h'})
	w.Write([]byte{'h'})

	select {
	case <-drop:
	case <-time.After(time.Second):
		t.Fatal("Expected buffer to be dropped")
	}
}

// func TestStream_StreamNotReady(t *testing.T) {
// 	s := NewStream()
// 	s.Start()
// 	s.Write([]byte{'h'})

// 	select {
// 	case <- drop:
// 	case <-time.After(time.Second):
// 		t.Fatal("Expected messages to be dropped")
// 	}
// 	}
// }
