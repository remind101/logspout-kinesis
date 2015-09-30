package kinesis

import (
	"errors"
	"log"
	"os"
	"text/template"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewAdapter, "kinesis")
}

var (
	// ErrorHandler handles the reporting of an error.
	ErrorHandler = logErr

	// ErrMissingTagKey is returned when the tag key environment variable doesn't match.
	ErrMissingTagKey = errors.New("the tag key is empty, check your template KINESIS_STREAM_TAG_KEY")

	// ErrMissingTagValue is returned when the tag value environment variable doesn't match.
	ErrMissingTagValue = errors.New("the tag value is empty, check your template KINESIS_STREAM_TAG_VALUE")
)

// Adapter represents the logspout adapter for Kinesis.
type Adapter struct {
	Streams    map[string]*Stream
	StreamTmpl *template.Template
	TagTmpl    *template.Template
	PKeyTmpl   *template.Template
}

// NewAdapter creates a kinesis adapter. Called during init.
func NewAdapter(route *router.Route) (router.LogAdapter, error) {
	sTmpl, err := compileTmpl("KINESIS_STREAM_TEMPLATE")
	if err != nil {
		return nil, err
	}

	tagTmpl, err := compileTmpl("KINESIS_STREAM_TAG_VALUE")
	if err != nil {
		return nil, err
	}

	pKeyTmpl, err := compileTmpl("KINESIS_PARTITION_KEY_TEMPLATE")
	if err != nil {
		return nil, err
	}

	streams := make(map[string]*Stream)

	return &Adapter{
		Streams:    streams,
		StreamTmpl: sTmpl,
		TagTmpl:    tagTmpl,
		PKeyTmpl:   pKeyTmpl,
	}, nil
}

// Stream handles the routing of a message to Kinesis.
func (a *Adapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		sn, err := executeTmpl(a.StreamTmpl, m)
		if err != nil {
			ErrorHandler(err)
			break
		}

		if sn == "" {
			debug("the stream name is empty, couldn't match the template. Skipping the log.")
			continue
		}

		if s, ok := a.Streams[sn]; ok {
			ErrorHandler(s.Write(m))
		} else {
			tags, err := tags(a.TagTmpl, m)
			if err != nil {
				ErrorHandler(err)
				break
			}

			s := NewStream(sn, tags, a.PKeyTmpl)
			s.Start()
			a.Streams[sn] = s
		}
	}
}

func tags(tmpl *template.Template, m *router.Message) (*map[string]*string, error) {
	tagKey := os.Getenv("KINESIS_STREAM_TAG_KEY")
	if tagKey == "" {
		return nil, ErrMissingTagKey
	}

	tagValue, err := executeTmpl(tmpl, m)
	if err != nil {
		return nil, err
	}

	if tagValue == "" {
		return nil, ErrMissingTagValue
	}

	return &map[string]*string{
		tagKey: aws.String(tagValue),
	}, nil
}

func logErr(err error) {
	if err != nil {
		log.Println("kinesis:", err.Error())
	}
}

func debug(format string, p ...interface{}) {
	if os.Getenv("KINESIS_DEBUG") == "true" {
		log.Printf("kinesis: "+format, p...)
	}
}
