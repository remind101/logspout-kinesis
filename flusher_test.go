package kinesis

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
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

	tags := make(map[string]*string)
	tags["name"] = aws.String("kinesis-test")

	s := NewStream("abc", &tags, tmpl)
	s.writer.buffer.limits = &testLimits
	s.writer.ticker = nil
	s.writer.flusher = f
	s.client = &fakeClient{
		created: true,
		err:     nil,
	}
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
