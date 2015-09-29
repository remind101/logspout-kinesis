package kinesis

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Client is a wrapper for the AWS Kinesis client.
type Client interface {
	Create(*kinesis.CreateStreamInput) (bool, error)
	Status(*kinesis.DescribeStreamInput) string
	Tag(*kinesis.AddTagsToStreamInput) error
	PutRecords(inp *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

type client struct {
	kinesis *kinesis.Kinesis
}

func (c *client) Create(input *kinesis.CreateStreamInput) (bool, error) {
	_, err := c.kinesis.CreateStream(input)

	if err != nil {
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			if reqErr.Code() == "ResourceInUseException" {
				return true, nil
			}
			return false, err
		}
		return false, err
	}

	return false, nil
}

func (c *client) Status(input *kinesis.DescribeStreamInput) string {
	resp, _ := c.kinesis.DescribeStream(input)
	return *resp.StreamDescription.StreamStatus
}

func (c *client) Tag(input *kinesis.AddTagsToStreamInput) error {
	_, err := c.kinesis.AddTagsToStream(input)
	if err != nil {
		return err
	}

	return nil
}

func (c *client) PutRecords(inp *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	return c.kinesis.PutRecords(inp)
}
