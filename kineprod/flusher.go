package kineprod

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

type ErrEmptyBuffer struct{}

func (e *ErrEmptyBuffer) Error() string {
	return fmt.Sprintf("buffer is empty")
}

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
		return &ErrEmptyBuffer{}
	}

	_, err := f.client.PutRecords(&b.input)
	if err != nil {
		return err
	}

	debug("buffer flushed, stream name: %s, length: %d",
		*b.input.StreamName, len(b.input.Records))

	return nil
}
