package kinesis

import (
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Drainer struct {
	Buffer *recordBuffer
}

func newDrainer(client *kinesis.Kinesis, streamName string) (*Drainer, error) {
	buffer, err := newRecordBuffer(client, streamName)
	if err != nil {
		return nil, err
	}

	d := &Drainer{Buffer: buffer}
	// Every second, we flush the buffer
	go d.Drain()

	return d, nil
}

// Drain flushes the buffer every second.
func (d *Drainer) Drain() {
	for _ = range time.Tick(time.Second * 1) {
		logErr(d.Buffer.Flush())
	}
}
