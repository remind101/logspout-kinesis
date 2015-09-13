package kinesis

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

type ErrDroppedInput struct {
	Stream string
	Count  int
}

func (e *ErrDroppedInput) Error() string {
	return fmt.Sprintf("input dropped! stream: %s, # items: %d", e.Stream, e.Count)
}

type Flusher interface {
	start()
	flush(input kinesis.PutRecordsInput)
	flushInputs()
}

type flusher struct {
	client        *kinesis.Kinesis
	inputs        chan kinesis.PutRecordsInput
	dropInputFunc func(kinesis.PutRecordsInput)
}

func newFlusher(client *kinesis.Kinesis) Flusher {
	return &flusher{
		client:        client,
		inputs:        make(chan kinesis.PutRecordsInput, 10),
		dropInputFunc: dropInput,
	}
}

func (f *flusher) start() {
	f.flushInputs()
}

func (f *flusher) flush(input kinesis.PutRecordsInput) {
	select {
	case f.inputs <- input:
	default:
		f.dropInputFunc(input)
	}
}

func (f *flusher) flushInputs() {
	for inp := range f.inputs {
		_, err := f.client.PutRecords(&inp)
		if err != nil {
			ErrorHandler(err)
		}

		debug("buffer flushed, stream: %s, length: %d",
			*inp.StreamName, len(inp.Records))
	}
}

func dropInput(input kinesis.PutRecordsInput) {
	ErrorHandler(&ErrDroppedInput{
		Stream: *input.StreamName,
		Count:  len(input.Records),
	})
}
