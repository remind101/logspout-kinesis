package kinesis

import (
	"log"
	"sync"
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
	Launched   map[string]bool
	StreamTmpl *template.Template
	mutex      sync.Mutex
}

func NewKinesisAdapter(route *router.Route) (router.LogAdapter, error) {
	drainers := make(map[string]*Drainer)
	launched := make(map[string]bool)
	client := kinesis.New(&aws.Config{})
	tmpl, err := compileTmpl("KINESIS_STREAM_TEMPLATE")
	if err != nil {
		return nil, err
	}

	return &KinesisAdapter{
		Client:     client,
		Drainers:   drainers,
		Launched:   launched,
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

		streamName, err := executeTmpl(a.StreamTmpl, m)
		if err != nil {
			logErr(err)
			break
		}

		if streamName == "" {
			debugLog("The stream name is empty, couldn't match the template. Skipping the log.\n")
			continue
		}

		if d, ok := a.findDrainer(streamName); ok {
			logErr(d.Buffer.Add(m))
		} else {
			if _, ok := a.Launched[streamName]; !ok {
				go newDrainer(a, streamName, m)
				a.Launched[streamName] = true
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
	a.mutex.Lock()
	defer a.mutex.Unlock()

	d, ok := a.Drainers[streamName]

	return d, ok
}

func (a *KinesisAdapter) addDrainer(streamName string, d *Drainer) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	log.Printf("kinesis: added drainer %s\n", streamName)
	a.Drainers[streamName] = d
}
