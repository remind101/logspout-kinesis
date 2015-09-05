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
}

func NewKinesisAdapter(route *router.Route) (router.LogAdapter, error) {
	streams := make(map[string]*kineprod.Stream)
	tmpl, err := compileTmpl("KINESIS_STREAM_TEMPLATE")
	if err != nil {
		return nil, err
	}

	return &KinesisAdapter{
		Streams:    streams,
		StreamTmpl: tmpl,
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
			s := kineprod.New(sn, m)
			s.Start()
			a.Streams[sn] = s
		}
	}
}
