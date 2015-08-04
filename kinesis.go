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
	Client     *kinesis.Kinesis
	Drainers   map[string]*Drainer
	StreamTmpl *template.Template
}

func NewKinesisAdapter(route *router.Route) (router.LogAdapter, error) {
	drainers := make(map[string]*Drainer)
	client := kinesis.New(&aws.Config{})
	tmpl, err := streamTmpl()
	if err != nil {
		return nil, err
	}

	return &KinesisAdapter{
		Client:     client,
		Drainers:   drainers,
		StreamTmpl: tmpl,
	}, nil
}

func (a *KinesisAdapter) Stream(logstream chan *router.Message) {
	for {
		m, open := <-logstream
		if !open {
			log.Println("kinesis: Channel is closed, flushing all the buffers")
			logErrs(a.FlushAll())
			break
		}

		d, err := a.findDrainer(m)
		if err != nil {
			logErr(err)
			break
		}

		logErr(d.Buffer.Add(m))
	}
}

func (a *KinesisAdapter) findDrainer(m *router.Message) (*Drainer, error) {
	var d *Drainer
	var ok bool

	streamName, err := streamName(a.StreamTmpl, m)
	if err != nil {
		return nil, err
	}

	if d, ok = a.Drainers[streamName]; !ok {
		d, err = newDrainer(a.Client, streamName)
		if err != nil {
			return nil, err
		}

		a.Drainers[streamName] = d
	}

	return d, nil
}

// FlushAll flushes all the kinesis buffers one by one.
func (a *KinesisAdapter) FlushAll() []error {
	var err []error

	for _, d := range a.Drainers {
		err = append(err, d.Buffer.Flush())
	}

	return err
}

func streamTmpl() (*template.Template, error) {
	streamTmplString := os.Getenv("KINESIS_STREAM_TEMPLATE")
	if streamTmplString == "" {
		return nil, errors.New("The stream name template is missing. Please set the KINESIS_STREAM_TEMPLATE env variable")
	}

	streamTmpl, err := template.New("kinesisStream").Parse(streamTmplString)
	if err != nil {
		return nil, err
	}

	return streamTmpl, nil
}

func streamName(tmpl *template.Template, m *router.Message) (string, error) {
	var streamName bytes.Buffer
	err := tmpl.Execute(&streamName, m)
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

func logErrs(err []error) {
	if err != nil {
		for _, e := range err {
			log.Println("kinesis: ", e.Error())
		}
	}
}
