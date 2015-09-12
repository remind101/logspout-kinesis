package kinesis

import (
	"time"

	"github.com/gliderlabs/logspout/router"
)

type writer struct {
	buffer   *buffer
	flusher  Flusher
	messages chan *router.Message
	ticker   <-chan time.Time
}

func newWriter(b *buffer, f Flusher) *writer {
	w := &writer{
		messages: make(chan *router.Message),
		ticker:   time.NewTicker(time.Second).C,
		flusher:  f,
		buffer:   b,
	}

	return w
}

func (w *writer) start() {
	go w.flusher.start()
	go w.bufferMessages()
}

func (w *writer) write(m *router.Message) {
	w.messages <- m
}

func (w *writer) bufferMessages() {
	flush := func() {
		w.flusher.flush(*w.buffer.input)
		w.buffer.reset()
	}

	for {
		select {
		case m := <-w.messages:
			if w.buffer.full(m) {
				flush()
			}

			ErrorHandler(w.buffer.add(m))
		case <-w.ticker:
			if !w.buffer.empty() {
				flush()
			} else {
				debug("buffer is empty, stream: %s", *w.buffer.input.StreamName)
			}
		}
	}
}
