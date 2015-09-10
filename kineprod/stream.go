package kineprod

import (
	"errors"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

var ErrStreamNotReady = errors.New("stream is not ready")

// TODO: comment
type Stream struct {
	client     Client
	name       string
	tags       *map[string]*string
	Writer     *writer
	ready      bool
	readyWrite chan bool
	readyTag   chan bool
}

// TODO: comment
func New(name string, tags *map[string]*string, pKeyTmpl *template.Template) *Stream {
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
		ready:      false,
		readyWrite: make(chan bool),
		readyTag:   make(chan bool),
	}

	return s
}

// TODO: commment
func (s *Stream) Start() {
	go s.create()
	go s.tag()
}

// TODO: commment
func (s *Stream) Write(m *router.Message) error {
	if s.ready {
		s.Writer.Write(m)
		return nil
	}

	select {
	case <-s.readyWrite:
		s.ready = true
	default:
		return ErrStreamNotReady
	}

	return nil
}

func (s *Stream) create() {
	created, err := s.client.Create(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(1),
		StreamName: aws.String(s.name),
	})

	if err != nil {
		logErr(err)
	}

	if created {
		s.readyTag <- true
	} else {
		for {
			status := s.client.Status(&kinesis.DescribeStreamInput{
				StreamName: aws.String(s.name),
			})
			if status == "ACTIVE" {
				s.readyTag <- true
				break
			} else {
				// wait a bit
				time.Sleep(4 * time.Second)
			}
		}
	}
}

func (s *Stream) tag() {
	// wait to be created...
	<-s.readyTag

	tagged, err := s.client.Tag(&kinesis.AddTagsToStreamInput{
		StreamName: aws.String(s.name),
		Tags:       *s.tags,
	})
	if !tagged {
		panic(err)
	}

	s.readyWrite <- true
}
