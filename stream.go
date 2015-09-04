package kinesis

import (
	"fmt"
	"time"
)

type buffer interface {
	add([]byte)
	full() bool
	data() [][]byte
}

type flusher interface {
	flush([][]byte)
}

type stream struct {
	messages      chan []byte
	buffers       chan [][]byte
	ticker        <-chan time.Time
	newBuffer     func() buffer
	droppedBuffer func(buffer)
	flusher       flusher
}

func newStream() *stream {
	s := &stream{
		messages: make(chan []byte),
		buffers:  make(chan [][]byte, 10),
		ticker:   time.NewTicker(time.Second).C,
	}

	return s
}

func (s *stream) start() {
	go s.bufferMessages()
	go s.flushBuffers()
}

func (s *stream) add(d []byte) {
	s.messages <- d
}

func (s *stream) bufferMessages() {
	var b buffer

	flush := func() {
		select {
		case s.buffers <- b.data():
		default:
			fmt.Println("default select flush")
			s.droppedBuffer(b)
		}
		fmt.Printf("flush")
		b = nil
	}

	for {
		if b == nil {
			b = s.newBuffer()
		}

		select {
		case m := <-s.messages:
			fmt.Println("messages select")
			b.add(m)

			if b.full() {
				flush()
			}
		case <-s.ticker:
			fmt.Println("ticker")
			flush()
		}
	}
}

func (s *stream) flushBuffers() {
	for b := range s.buffers {
		s.flusher.flush(b)
	}
}
