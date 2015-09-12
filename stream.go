package kinesis

import (
	"fmt"
	"log"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

type ErrStreamNotReady struct {
	s string
}

func (e *ErrStreamNotReady) Error() string {
	return fmt.Sprintf("not ready, stream: %s", e.s)
}

// Stream represents a stream that will send messages to its writer.
type Stream struct {
	client     Client
	name       string
	tags       *map[string]*string
	Writer     *writer
	ready      bool
	readyWrite chan bool
	err        error
	errChan    chan error
}

// NewStream instantiates a new stream.
func NewStream(name string, tags *map[string]*string, pKeyTmpl *template.Template) *Stream {
	client := &client{
		kinesis: kinesis.New(&aws.Config{}),
	}

	writer := newWriter(
		newBuffer(pKeyTmpl, name),
		newFlusher(client.kinesis),
	)

	s := &Stream{
		client:     client,
		name:       name,
		tags:       tags,
		Writer:     writer,
		readyWrite: make(chan bool),
		errChan:    make(chan error),
	}

	return s
}

// Start runs the goroutines making calls to create and tag the stream on
// AWS.
func (s *Stream) Start() {
	go s.start()
}

func (s *Stream) start() {
	if err := s.create(); err != nil {
		s.errChan <- err
		ErrorHandler(err)
		return
	}

	if err := s.tag(); err != nil {
		s.errChan <- err
		ErrorHandler(err)
		return
	}

	s.readyWrite <- true
	log.Printf("ready! stream: %s", s.name)
}

// Write sends the message to the writer if the stream is ready
// i.e created and tagged.
func (s *Stream) Write(m *router.Message) error {
	select {
	case s.ready = <-s.readyWrite:
	case s.err = <-s.errChan:
	default:
	}

	switch {
	case s.err != nil:
		return s.err
	case s.ready:
		s.Writer.write(m)
		return nil
	default:
		return &ErrStreamNotReady{s: s.name}
	}
}

func (s *Stream) create() error {
	created, err := s.client.Create(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(1),
		StreamName: aws.String(s.name),
	})

	if err != nil {
		return err
	}

	if created {
		return nil
	} else {
		debug("need to create stream: %s", s.name)
		for {
			status := s.client.Status(&kinesis.DescribeStreamInput{
				StreamName: aws.String(s.name),
			})
			if status == "ACTIVE" {
				return nil
			} else {
				// wait a bit
				time.Sleep(4 * time.Second)
			}
			debug("stream %s status: %s", s.name, status)
		}
	}
}

func (s *Stream) tag() error {
	err := s.client.Tag(&kinesis.AddTagsToStreamInput{
		StreamName: aws.String(s.name),
		Tags:       *s.tags,
	})

	if err != nil {
		return err
	}

	return nil
}
