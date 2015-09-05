package kineprod

import "time"

type buffer interface {
	add([]byte)
	full() bool
	data() []byte
}

type writer struct {
	messages      chan []byte
	buffers       chan []byte
	ticker        <-chan time.Time
	newBuffer     func() buffer
	droppedBuffer func(buffer)
	flusher       flusher
}

// TODO: comment
func newWriter() *writer {
	w := &writer{
		messages: make(chan []byte),
		buffers:  make(chan []byte, 10),
		ticker:   time.NewTicker(time.Second).C,
	}

	return w
}

// TODO: comment
func (w *writer) start() {
	go w.bufferMessages()
	go w.flushBuffers()
}

// TODO: comment
func (w *writer) Write(d []byte) (n int, err error) {
	w.messages <- d
	return
}

func (w *writer) bufferMessages() {
	var b buffer

	flush := func() {
		select {
		case w.buffers <- b.data():
		default:
			w.droppedBuffer(b)
		}
		b = nil
	}

	for {
		if b == nil {
			b = w.newBuffer()
		}

		select {
		case m := <-w.messages:
			b.add(m)

			if b.full() {
				flush()
			}
		case <-w.ticker:
			flush()
		}
	}
}

func (w *writer) flushBuffers() {
	for b := range w.buffers {
		w.flusher.flush(b)
	}
}
