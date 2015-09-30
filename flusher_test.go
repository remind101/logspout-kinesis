package kinesis

import (
	"testing"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
)

func TestFlusher_FlushFull(t *testing.T) {
	drop := make(chan struct{})
	f := &flusher{
		inputs: make(chan kinesis.PutRecordsInput, 0),
		dropInputFunc: func(input kinesis.PutRecordsInput) {
			close(drop)
		},
	}

	go func() {
		f.inputs <- kinesis.PutRecordsInput{}
	}()

	f.flush(kinesis.PutRecordsInput{})

	select {
	case <-drop:
	case <-time.After(1 * time.Second):
		t.Fatal("Expected input to be dropped")
	}
}

func TestFlusher_IntegrationInputsChannelFull(t *testing.T) {
	drop := make(chan struct{})
	f := &fakeFlusher{
		inputs: make(chan kinesis.PutRecordsInput, 0),
		dropInputFunc: func(input kinesis.PutRecordsInput) {
			close(drop)
		},
		flushFunc: func() {
			<-time.After(time.Minute)
		},
	}

	streamName := "abc"
	tmpl, _ := template.New("").Parse(streamName)
	tags := make(map[string]*string)
	tags["name"] = aws.String("kinesis-test")

	m := &router.Message{
		Data: "hello",
		Container: &docker.Container{
			ID: "123",
		},
	}

	s := NewStream(streamName, &tags, tmpl)

	w := newWriter(
		newBuffer(tmpl, streamName),
		f,
	)
	w.ticker = nil
	w.buffer.limits = &testLimits

	s.writers[m.Container.ID] = w
	s.client = &fakeClient{
		created: true,
		err:     nil,
	}

	s.writers[m.Container.ID].start()
	s.ready = true
	s.Start()

	go func() {
		for {
			f.inputs <- kinesis.PutRecordsInput{}
		}
	}()
	s.Write(m)
	s.Write(m)
	s.Write(m)

	select {
	case <-drop:
	case <-time.After(1 * time.Second):
		t.Fatal("Expected input to be dropped")
	}
}
