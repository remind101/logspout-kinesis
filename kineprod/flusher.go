package kineprod

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

var ErrEmptyBuffer = errors.New("buffer is empty")

type Flusher interface {
	flush(b buffer) error
}

type flusher struct {
	client *kinesis.Kinesis
}

func newFlusher(client *kinesis.Kinesis) Flusher {
	return &flusher{
		client: client,
	}
}

func (f flusher) flush(b buffer) error {
	if b.count == 0 {
		return ErrEmptyBuffer
	}

	_, err := f.client.PutRecords(&b.input)
	if err != nil {
		return err
	}

	return nil
}
