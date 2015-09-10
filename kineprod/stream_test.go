package kineprod

import (
	"testing"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

type fakeClient struct {
	created bool
	tagged  bool
}

func (f *fakeClient) Create(input *kinesis.CreateStreamInput) (bool, error) {
	if f.created {
		return true, nil
	}
	return false, nil
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
		t.Fatalf("Expected error: %s", ErrStreamNotReady.Error())
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

// func TestStream_StreamTagging(t *testing.T) {}
