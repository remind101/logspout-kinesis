package kinesis

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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

func (f *fakeClient) Tag(input *kinesis.AddTagsToStreamInput) (bool, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.tagged, nil
}

// TODO: implement optional stream creation
// func TestStream_CreationDeactivated(t *testing.T) {

// }

// TODO: implement optional stream tagging
// func TestStream_TaggingDeactivated(t *testing.T) {

// }

func TestStream_StreamNotReady(t *testing.T) {
	m := &router.Message{
		Data: "hello",
	}

	s := NewStream("abc", nil, nil)
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
	s := NewStream("abc", nil, nil)
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
	s := NewStream("abc", nil, nil)
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

func TestStream_StreamCreatedButNotTagged(t *testing.T) {
	m := &router.Message{
		Data: "hello",
	}

	tags := make(map[string]*string)
	tags["name"] = aws.String("kinesis-test")

	s := NewStream("abc", &tags, tmpl)
	fk := &fakeClient{
		created: true,
	}
	s.client = fk

	b := newBuffer(tmpl, s.name)
	b.limits = &testLimits
	f := &fakeFlusher{
		flushed: make(chan struct{}),
	}
	s.Writer = newWriter(b, f)

	s.Start()
	s.Writer.Start()

	err := s.Write(m)
	if err == nil {
		t.Fatalf("Expected error: %s", err.Error())
	}

	fk.mutex.Lock()
	fk.tagged = true
	fk.mutex.Unlock()

	timeout := make(chan bool)
	go func() {
		time.Sleep(1 * time.Second)
		timeout <- true
	}()

	for {
		err = s.Write(m)
		if err == nil {
			break
		}

		select {
		case <-timeout:
			break
		default:
		}
	}

	err = s.Write(m)
	if err != nil {
		t.Fatalf("Expected successful write, error: %s", err.Error())
	}
}
