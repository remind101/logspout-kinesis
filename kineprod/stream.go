package kineprod

import (
	"errors"
	"io"
	"sync"

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
	writers    map[string]io.Writer
	readyWrite bool
	wMutex     *sync.Mutex
	readyTag   chan bool
	// pKeyTmpl   *template.Template
}

// TODO: comment
func New(name string, m *router.Message) *Stream {
	s := &Stream{
		client: kinesis.New(&aws.Config{}),
		name:   name,
		// tags:       compileTags(m),
		writers:    make(map[string]io.Writer),
		readyWrite: false,
		readyTag:   make(chan bool),
		// pKeyTmpl:   pkeyTempl("ENV_VAR"),
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

	// pKey := partitionKey(s.pKeyTmpl, m)
	pKey := "bob"
	if w, ok := s.writers[pKey]; ok {
		w.Write([]byte(m.Data))
	} else {
		w := newWriter()
		w.start()
		s.writers[pKey] = w
		w.Write([]byte(m.Data))
	}

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
