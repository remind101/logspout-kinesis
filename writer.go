package kinesis

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

// Start starts the goroutines consuming, dropping and flushing messages.
func (w *writer) Start() {
	go w.bufferMessages()
	go w.flushBuffers()
}

func (w *writer) write(m *router.Message) {
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
			if empErr, ok := err.(*ErrEmptyBuffer); ok {
				debug("%s\n", empErr.Error())
			} else {
				logErr(err)
			}
		}
	}
}

func dropBuffer(b buffer) {
	debug("buffer dropped! items: %d, byteSize: %d\n", b.count, b.byteSize)
}
