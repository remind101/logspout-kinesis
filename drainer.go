package kinesis

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Drainer struct {
	Buffer *recordBuffer
}

func newDrainer(a *KinesisAdapter, streamName string) {
	buffer, err := newRecordBuffer(a.Client, streamName)
	if err != nil {
		logErr(err)
	}
	d := &Drainer{Buffer: buffer}

	fmt.Printf("\n\n\n\n\n\n\nSTREAM NAME : %s\n\n\n\n\n", streamName)
	_, err = a.Client.CreateStream(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(1),
		StreamName: aws.String(streamName),
	})

	// If an error occur, return. Unless it's because the stream already exists.
	if err != nil {
		fmt.Println(err)
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			fmt.Println(reqErr)
			if reqErr.Code() == "ResourceInUseException" {
				a.addDrainer(streamName, d)
			} else {
				logErr(err)
			}
		} else {
			logErr(err)
		}
	} else {
		// Wait for the stream to be active.
		go waitForActive(a, d)
	}

	go d.Drain()
}

// Drain flushes the buffer every second.
func (d *Drainer) Drain() {
	for _ = range time.Tick(time.Second * 1) {
		logErr(d.Buffer.Flush())
	}
}

func waitForActive(a *KinesisAdapter, d *Drainer) {
	resp := &kinesis.DescribeStreamOutput{}
	// timeout := make(chan bool, 30)
	streamName := *d.Buffer.input.StreamName

	params := &kinesis.DescribeStreamInput{StreamName: aws.String(streamName)}
	for {
		resp, _ = a.Client.DescribeStream(params)
		if streamStatus := *resp.StreamDescription.StreamStatus; streamStatus == "ACTIVE" {
			log.Printf("kinesis: STREAM '%s' STATUS: %s\n", streamName, streamStatus)
			a.addDrainer(streamName, d)
			break
		} else {
			log.Printf("kinesis: STREAM '%s' STATUS: %s\n", streamName, streamStatus)
			time.Sleep(4 * time.Second)
			// timeout <- true
		}
	}
}
