package kinesis

import (
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
	"github.com/stretchr/testify/assert"
)

type fakeClient struct {
	created bool
	status  string
	err     error
	mutex   sync.Mutex
}

func (f *fakeClient) Create(input *kinesis.CreateStreamInput) (bool, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.created, f.err
}

func (f *fakeClient) Status(input *kinesis.DescribeStreamInput) string {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.status
}

func (f *fakeClient) Tag(input *kinesis.AddTagsToStreamInput) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.err
}

func (f *fakeClient) PutRecords(inp *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	return nil, nil
}

// TODO: implement optional stream creation
// func TestStream_CreationDeactivated(t *testing.T) {

// }

// TODO: implement optional stream tagging
// func TestStream_TaggingDeactivated(t *testing.T) {

// }

func TestStream_CreateAlreadyExists(t *testing.T) {
	s := NewStream("abc", nil, nil)
	s.client = &fakeClient{
		created: true,
	}

	err := s.create()
	assert.Nil(t, err)
}

func TestStream_CreateStatusActive(t *testing.T) {
	s := NewStream("abc", nil, nil)
	s.client = &fakeClient{
		created: false,
		status:  "ACTIVE",
	}

	err := s.create()
	assert.Nil(t, err)
}

func TestStream_CreateError(t *testing.T) {
	s := NewStream("abc", nil, nil)
	s.client = &fakeClient{
		created: false,
		err:     awserr.New("RequestError", "500", nil),
	}

	err := s.create()
	assert.NotNil(t, err)
}

func TestStream_WriteStreamNotReady(t *testing.T) {
	m := &router.Message{
		Data: "hello",
		Container: &docker.Container{
			ID: "123",
		},
	}

	s := NewStream("abc", nil, nil)
	s.client = &fakeClient{
		created: false,
	}
	s.Start()
	err := s.Write(m)

	if assert.Error(t, err, "A stream not ready error was expected") {
		assert.Equal(t, err, &StreamNotReadyError{Stream: "abc"})
	}
}

func TestStream_WriteStreamBecomesReady(t *testing.T) {
	tmpl, _ := template.New("").Parse("abc")
	tags := make(map[string]*string)
	tags["name"] = aws.String("kinesis-test")

	m := &router.Message{
		Data: "hello",
		Container: &docker.Container{
			ID: "123",
		},
	}

	s := NewStream("abc", &tags, tmpl)
	fk := &fakeClient{
		created: false,
		status:  "CREATING",
		mutex:   sync.Mutex{},
	}
	s.client = fk
	s.Start()

	err := s.Write(m)
	if assert.Error(t, err, "A stream not ready error was expected") {
		assert.Equal(t, err, &StreamNotReadyError{Stream: "abc"})
	}

	fk.mutex.Lock()
	fk.status = "ACTIVE"
	fk.mutex.Unlock()

	timeout := make(chan bool)
	go func() {
		time.Sleep(1 * time.Second)
		timeout <- true
	}()

	// trying to write until we succeed or timeout
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

	assert.Nil(t, err)
}
