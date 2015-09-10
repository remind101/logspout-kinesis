package kineprod

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Client interface {
	Create(input *kinesis.CreateStreamInput) (bool, error)
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
