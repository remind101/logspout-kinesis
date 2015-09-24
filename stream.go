package kinesis

import (
	"errors"
	"fmt"
	"log"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

// ErrMissingProcess is returned when the process environment variable is doesn't match.
var ErrMissingProcess = errors.New("the process is empty, check your template KINESIS_PROCESS_TEMPLATE")

// StreamNotReadyError is returned while the stream is being created.
type StreamNotReadyError struct {
	Stream string
}

func (e *StreamNotReadyError) Error() string {
	return fmt.Sprintf("not ready, stream: %s", e.Stream)
}

// Stream represents a stream that will send messages to its writer.
type Stream struct {
	client     Client
	name       string
	tags       *map[string]*string
	writers    map[string]*writer
	pTmpl      *template.Template
	pKeyTmpl   *template.Template
	ready      bool
	readyWrite chan bool
	err        error
	errChan    chan error
}

// NewStream instantiates a new stream.
func NewStream(name string, tags *map[string]*string, pKeyTmpl *template.Template, pTmpl *template.Template) *Stream {
	client := &client{
		kinesis: kinesis.New(&aws.Config{}),
	}

	s := &Stream{
		client:     client,
		name:       name,
		tags:       tags,
		writers:    make(map[string]*writer),
		pKeyTmpl:   pKeyTmpl,
		pTmpl:      pTmpl,
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
		return s.write(m)
	default:
		return &StreamNotReadyError{Stream: s.name}
	}
}

func (s *Stream) write(m *router.Message) error {
	process, err := process(s.pTmpl, m)
	if err != nil {
		return err
	}

	if w, ok := s.writers[process]; ok {
		w.write(m)
		return nil
	}

	w := newWriter(
		newBuffer(s.pKeyTmpl, s.name),
		newFlusher(s.client.Kinesis()),
	)
	w.start()
	s.writers[process] = w
	w.write(m)
	return nil
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
	}

	debug("need to create stream: %s", s.name)
	for {
		status := s.client.Status(&kinesis.DescribeStreamInput{
			StreamName: aws.String(s.name),
		})
		if status == "ACTIVE" {
			return nil
		}
		time.Sleep(4 * time.Second) // wait a bit
		debug("stream %s status: %s", s.name, status)
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

func process(tmpl *template.Template, m *router.Message) (string, error) {
	process, err := executeTmpl(tmpl, m)
	if err != nil {
		return "", err
	}

	if process == "" {
		return "", ErrMissingProcess
	}

	return process, nil
}
