package kineprod

import (
	"errors"
	"sync"
	"text/template"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

var ErrStreamNotReady = errors.New("stream is not ready")

// TODO: comment
type Stream struct {
	client *kinesis.Kinesis
	name   string
	// tags       map[string]*string
	Writer     *writer
	readyWrite bool
	wMutex     sync.Mutex
	readyTag   chan bool
}

// TODO: comment
func New(name string, pKeyTmpl *template.Template) *Stream {
	client := kinesis.New(&aws.Config{})
	writer := newWriter(newBuffer(pKeyTmpl, name), newFlusher(client))

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
	// resp, err := s.client.Create(s.name)
	// if err != nil {
	// 	// already exists
	// 	s.readyTag <- true
	// } else {
	// 	for {
	// 		// checkStatus
	// 		if created {
	// 			s.readyTag <- true
	// 			break
	// 		} else {
	// 			// wait a bit
	// 			time.Sleep(4 * time.Second)
	// 		}
	// 	}
	// }
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
