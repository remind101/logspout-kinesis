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

func (d *Drainer) Drain() {
	ticker := time.NewTicker(time.Second * 1)

	for {
		logErr(d.Buffer.Flush())
		<-ticker.C
	}
}
