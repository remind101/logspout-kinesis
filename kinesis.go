package kinesis

import (
	"log"
	"sync"
	"text/template"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

var mutex sync.Mutex

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
	tmpl, err := compileTmpl("KINESIS_STREAM_TEMPLATE")
	if err != nil {
		return nil, err
	}

	return &KinesisAdapter{
		Client:     client,
		Drainers:   drainers,
		StreamTmpl: tmpl,
	}, nil
}

var foo = make(map[string]bool)

func (a *KinesisAdapter) Stream(logstream chan *router.Message) {
	for {
		m, open := <-logstream
		if !open {
			log.Println("kinesis: Channel is closed, flushing all the buffers")
			logErrs(a.FlushAll())
			break
		}

		streamName, err := executeTmpl(a.StreamTmpl, m)
		if err != nil {
			logErr(err)
			break
		}

		if d, ok := a.findDrainer(streamName); ok {
			logErr(d.Buffer.Add(m))
		} else {
			if !foo[streamName] {
				go newDrainer(a, streamName)
				foo[streamName] = true
			}
		}
	}
}

// FlushAll flushes all the kinesis buffers one by one.
func (a *KinesisAdapter) FlushAll() []error {
	var err []error

	for _, d := range a.Drainers {
		err = append(err, d.Buffer.Flush())
	}

	return err
}

func (a *KinesisAdapter) findDrainer(streamName string) (*Drainer, bool) {
	mutex.Lock()
	defer mutex.Unlock()

	d, ok := a.Drainers[streamName]

	return d, ok
}

func (a *KinesisAdapter) addDrainer(streamName string, d *Drainer) {
	mutex.Lock()
	defer mutex.Unlock()

	a.Drainers[streamName] = d
}
