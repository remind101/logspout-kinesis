package kineprod

import (
	"time"

	"github.com/gliderlabs/logspout/router"
)

type writer struct {
	buffer         *buffer
	flusher        Flusher
	messages       chan *router.Message
	buffers        chan buffer
	dropBufferFunc func(buffer)
	ticker         <-chan time.Time
}

func newWriter(b *buffer, f Flusher) *writer {
	w := &writer{
		messages:       make(chan *router.Message),
		buffers:        make(chan buffer, 10),
		ticker:         time.NewTicker(time.Second).C,
		dropBufferFunc: dropBuffer,
		flusher:        f,
		buffer:         b,
	}

	return w
}

// TODO: comment
func (w *writer) Start() {
	go w.bufferMessages()
	go w.flushBuffers()
}

// TODO: lowercase the function
func (w *writer) Write(m *router.Message) {
	w.messages <- m
}

func (w *writer) bufferMessages() {
	flush := func() {
		select {
		case w.buffers <- *w.buffer:
		default:
			w.dropBufferFunc(*w.buffer)
		}
		w.buffer.reset()
	}

	for {
		select {
		case m := <-w.messages:
			if w.buffer.full(m) {
				flush()
			}

			w.buffer.add(m)
		case <-w.ticker:
			flush()
		}
	}
}

func (w *writer) flushBuffers() {
	for b := range w.buffers {
		err := w.flusher.flush(b)
		if err != nil {
			debugLog("%s\n", err.Error())
		}
	}
}

func dropBuffer(b buffer) {
	debugLog("buffer dropped! items: %d, byteSize: %d\n", b.count, b.byteSize)
}
