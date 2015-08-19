package kinesis

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

type Drainer struct {
	Buffer *recordBuffer
}

func newDrainer(a *KinesisAdapter, streamName string, m *router.Message) {
	buffer, err := newRecordBuffer(a.Client, streamName)
	if err != nil {
		logErr(err)
	}

	d := &Drainer{Buffer: buffer}
	go d.Drain()

	if os.Getenv("KINESIS_STREAM_CREATION") == "true" {
		createStream(a, d, streamName, m)
	} else {
		a.addDrainer(streamName, d)
	}
}

// Drain flushes the buffer every second.
func (d *Drainer) Drain() {
	for _ = range time.Tick(time.Second * 1) {
		logErr(d.Buffer.Flush())
	}
}

func createStream(a *KinesisAdapter, d *Drainer, streamName string, m *router.Message) {
	_, err := a.Client.CreateStream(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(1),
		StreamName: aws.String(streamName),
	})

	if err != nil {
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			if reqErr.Code() == "ResourceInUseException" {
				logErr(tagStream(a, streamName, m))
				a.addDrainer(streamName, d)
			} else {
				logErr(err)
			}
		} else {
			logErr(err)
		}
	} else {
		debugLog("kinesis: need to create stream for %s", streamName)
		waitForActive(a, d, m)
	}
}

func waitForActive(a *KinesisAdapter, d *Drainer, m *router.Message) {
	streamName := *d.Buffer.input.StreamName
	var streamStatus string

	params := &kinesis.DescribeStreamInput{StreamName: aws.String(streamName)}
	resp := &kinesis.DescribeStreamOutput{}
	for {
		resp, _ = a.Client.DescribeStream(params)
		if streamStatus = *resp.StreamDescription.StreamStatus; streamStatus == "ACTIVE" {
			logErr(tagStream(a, streamName, m))
			a.addDrainer(streamName, d)
			break
		} else {
			time.Sleep(4 * time.Second)
		}

		debugLog("kinesis: status for stream %s: %s", streamName, streamStatus)
	}
}

func tagStream(a *KinesisAdapter, streamName string, m *router.Message) error {
	if tagKey := os.Getenv("KINESIS_STREAM_TAG_KEY"); tagKey != "" {
		tmpl, err := compileTmpl("KINESIS_STREAM_TAG_VALUE")
		if err != nil {
			return err
		}

		tagValue, err := executeTmpl(tmpl, m)
		if err != nil {
			return err
		}

		if tagValue == "" {
			return fmt.Errorf("The tag value is empty, check your template KINESIS_STREAM_TAG_VALUE.")
		}

		tags := map[string]*string{
			tagKey: aws.String(tagValue),
		}

		params := &kinesis.AddTagsToStreamInput{
			StreamName: aws.String(streamName),
			Tags:       tags,
		}

		_, err = a.Client.AddTagsToStream(params)
		return err
	}

	return nil
}
