package kinesis

import (
	"text/template"

	"github.com/gliderlabs/logspout/router"
	"github.com/remind101/logspout-kinesis/kineprod"
)

func init() {
	router.AdapterFactories.Register(NewKinesisAdapter, "kinesis")
}

type KinesisAdapter struct {
	Streams    map[string]*kineprod.Stream
	StreamTmpl *template.Template
	PKeyTmpl   *template.Template
}

func NewKinesisAdapter(route *router.Route) (router.LogAdapter, error) {
	sTmpl, err := compileTmpl("KINESIS_STREAM_TEMPLATE")
	if err != nil {
		return nil, err
	}

	pTmpl, err := compileTmpl("KINESIS_PARTITION_KEY_TEMPLATE")
	if err != nil {
		return nil, err
	}

	streams := make(map[string]*kineprod.Stream)

	return &KinesisAdapter{
		Streams:    streams,
		StreamTmpl: sTmpl,
		PKeyTmpl:   pTmpl,
	}, nil
}

func (a *KinesisAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		sn, err := executeTmpl(a.StreamTmpl, m)
		if err != nil {
			logErr(err)
			break
		}

		if sn == "" {
			debugLog("The stream name is empty, couldn't match the template. Skipping the log.\n")
			continue
		}

		if s, ok := a.Streams[sn]; ok {
			logErr(s.Write(m))
		} else {
			s := kineprod.New(sn, a.PKeyTmpl)
			s.Start()
			s.Writer.Start()
			a.Streams[sn] = s
		}
	}
}
