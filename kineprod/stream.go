package kineprod

import (
	"errors"
	"sync"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

var ErrStreamNotReady = errors.New("stream is not ready")

// TODO: comment
type Stream struct {
	client Client
	name   string
	// tags       map[string]*string
	Writer     *writer
	readyWrite bool
	wMutex     sync.Mutex
	readyTag   chan bool
}

// TODO: comment
func New(name string, pKeyTmpl *template.Template) *Stream {
	client := &client{
		kinesis: kinesis.New(&aws.Config{}),
	}

	writer := newWriter(
		newBuffer(pKeyTmpl, name),
		newFlusher(client.kinesis),
	)

	s := &Stream{
		client: client,
		name:   name,
		// tags:       compileTags(m),
		Writer:     writer,
		readyWrite: false,
		wMutex:     sync.Mutex{},
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
	s.wMutex.Lock()
	defer s.wMutex.Unlock()

	if !s.readyWrite {
		return ErrStreamNotReady
	}

	s.Writer.Write(m)

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
	// // wait to be created...
	// <-s.readyTag
	// _, err := s.Client.Tag(s.Tags)
	// if err != nil {
	// 	logErr(err)
	// }

	// s.wMutex.lock()
	// s.readyWrite = true
	// s.wMutex.Unlock()
}

// func compileTags(m) *tags {

// }
