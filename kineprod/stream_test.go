package kineprod

import (
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

type fakeClient struct {
	created bool
	status  string
	tagged  bool
	mutex   sync.Mutex
}

func (f *fakeClient) Create(input *kinesis.CreateStreamInput) (bool, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.created, nil
}

func (f *fakeClient) Status(input *kinesis.DescribeStreamInput) string {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.status
}

func TestStream_StreamNotReady(t *testing.T) {
	m := &router.Message{
		Data: "hello",
	}

	s := New("abc", &template.Template{})
	s.client = &fakeClient{
		created: false,
	}
	s.Start()
	err := s.Write(m)

	if err == nil {
		t.Fatalf("Expected error: %s", err.Error())
	}
}

func TestStream_StreamCreationAlreadyExists(t *testing.T) {
	s := New("abc", &template.Template{})
	s.client = &fakeClient{
		created: true,
	}
	s.Start()

	select {
	case <-s.readyTag:
	case <-time.After(time.Second):
		t.Fatal("Expected stream to be created, and tag() to be called")
	}
}

func TestStream_StreamCreating(t *testing.T) {
	s := New("abc", &template.Template{})
	fk := &fakeClient{
		created: false,
		status:  "CREATING",
		mutex:   sync.Mutex{},
	}
	s.client = fk
	s.Start()

	err := s.Write(m)
	if err == nil {
		t.Fatalf("Expected error: %s", err.Error())
	}

	fk.mutex.Lock()
	fk.status = "ACTIVE"
	fk.mutex.Unlock()

	select {
	case <-s.readyTag:
	case <-time.After(time.Second):
		t.Fatal("Expected stream to be active, and tag() to be called")
	}
}

// func TestStream_StreamCreatedButNotTagged(t *testing.T) {
// 	m := &router.Message{
// 		Data: "hello",
// 	}

// 	s := New("abc", &template.Template{})
// 	fk := &fakeClient{
// 		created: true,
// 	}
// 	s.client = fk

// 	s.Start()
// 	err := s.Write(m)
// 	if err == nil {
// 		t.Fatalf("Expected error: %s", ErrStreamNotReady.Error())
// 	}

// 	fk.tagged = true

// 	err = s.Write(m)
// 	if err != nil {
// 		t.Fatalf("Expected successful write, error: %s", ErrStreamNotReady.Error())
// 	}
// }
