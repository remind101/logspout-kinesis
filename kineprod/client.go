package kineprod

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Client interface {
	Create(*kinesis.CreateStreamInput) (bool, error)
	Status(*kinesis.DescribeStreamInput) string
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
			} else {
				return false, err
			}
		} else {
			return false, err
		}
	}

	return false, nil
}

func (c *client) Status(input *kinesis.DescribeStreamInput) string {
	resp, _ := c.kinesis.DescribeStream(input)
	return *resp.StreamDescription.StreamStatus
}
