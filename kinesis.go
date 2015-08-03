package kinesis

import (
	"bytes"
	"errors"
	"log"
	"os"
	"text/template"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewKinesisAdapter, "kinesis")
}

type KinesisAdapter struct {
	Client   *kinesis.Kinesis
	Drainers map[string]*Drainer
}

func NewKinesisAdapter(route *router.Route) (router.LogAdapter, error) {
	drainers := make(map[string]*Drainer)
	client := kinesis.New(&aws.Config{})

	return &KinesisAdapter{
		Client:   client,
		Drainers: drainers,
	}, nil
}

func (a *KinesisAdapter) Stream(logstream chan *router.Message) {
	for {
		m, open := <-logstream
		d, err := a.findDrainer(m)
		if err != nil {
			logErr(err)
		}

		if !open {
			logErr(d.Buffer.Flush())
			break
		}

		logErr(d.Buffer.Add(m))
	}
}

func (a *KinesisAdapter) findDrainer(m *router.Message) (*Drainer, error) {
	var d *Drainer
	var ok bool

	streamName, err := streamName(m)
	if err != nil {
		return nil, err
	}

	if d, ok = a.Drainers[streamName]; !ok {
		d = newDrainer(a.Client, streamName)

		a.Drainers[streamName] = d
	}

	return d, nil
}

func streamName(m *router.Message) (string, error) {
	streamTmplString := os.Getenv("KINESIS_STREAM_TEMPLATE")
	if streamTmplString == "" {
		return "", errors.New("The stream name template is missing. Please set the KINESIS_STREAM_TEMPLATE env variable")
	}

	streamTmpl, err := template.New("kinesisStream").Parse(streamTmplString)
	if err != nil {
		return "", err
	}

	var streamName bytes.Buffer
	err = streamTmpl.Execute(&streamName, m)
	if err != nil {
		return "", err
	}

	return streamName.String(), nil

}

func logErr(err error) {
	if err != nil {
		log.Println("kinesis: ", err.Error())
	}
}
