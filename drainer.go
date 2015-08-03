package kinesis

import (
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Drainer struct {
	Buffer *recordBuffer
}

func newDrainer(client *kinesis.Kinesis, streamName string) *Drainer {
	d := &Drainer{Buffer: newRecordBuffer(client, streamName)}

	// Every second, we flush the buffer
	go d.Drain()

	return d
}

func (d *Drainer) Drain() {
	ticker := time.NewTicker(time.Second * 1)

	for {
		logErr(d.Buffer.Flush())
		<-ticker.C
	}
}
