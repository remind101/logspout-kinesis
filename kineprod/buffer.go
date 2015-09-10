package kineprod

import (
	"errors"
	"text/template"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
	"github.com/pborman/uuid"
)

// PutRecordsLimit is the maximum number of records allowed for a PutRecords request.
var PutRecordsLimit int = 500

// PutRecordsSizeLimit is the maximum allowed size per PutRecords request.
var PutRecordsSizeLimit int = 5 * 1024 * 1024 // 5MB

// RecordSizeLimit is the maximum allowed size per record.
var RecordSizeLimit int = 1 * 1024 * 1024 // 1MB

var ErrRecordTooBig = errors.New("data byte size is over the limit")

type buffer struct {
	ct       int
	byteSize int
	pKeyTmpl *template.Template
	inp      *kinesis.PutRecordsInput
	limits   map[string]int
}

func newBuffer(tmpl *template.Template, sn string) *buffer {
	limits := map[string]int{
		"PutRecordsLimit":     PutRecordsLimit,
		"PutRecordsSizeLimit": PutRecordsSizeLimit,
		"RecordSizeLimit":     RecordSizeLimit,
	}

	return &buffer{
		pKeyTmpl: tmpl,
		inp: &kinesis.PutRecordsInput{
			StreamName: aws.String(sn),
			Records:    make([]*kinesis.PutRecordsRequestEntry, 0),
		},
		limits: limits,
	}
}

func (b *buffer) add(m *router.Message) error {
	dataLen := len(m.Data)

	//
	if dataLen > b.limits["RecordSizeLimit"] {
		return ErrRecordTooBig
	}

	var pKey string
	if pKey = executeTmpl(b.pKeyTmpl, m); pKey == "" {
		// We default to a uuid if the template didn't match.
		pKey = uuid.New()
	}

	// Add to count
	b.ct += 1

	// Add data and partition key size to byteSize
	b.byteSize += dataLen + len(pKey)

	// Add record
	b.inp.Records = append(b.inp.Records, &kinesis.PutRecordsRequestEntry{
		Data:         []byte(m.Data),
		PartitionKey: aws.String(pKey),
	})

	return nil
}

func (b *buffer) full(m *router.Message) bool {
	// Adding this event would make our request have too many records.
	if b.ct+1 > b.limits["PutRecordsLimit"] {
		return true
	}

	// Adding this event would make our request too large.
	if b.byteSize+len(m.Data) > b.limits["PutRecordsSizeLimit"] {
		return true
	}

	return false
}

func (b *buffer) count() int {
	return b.ct
}

func (b *buffer) input() *kinesis.PutRecordsInput {
	return b.inp
}

func (b *buffer) reset() {
	b.ct = 0
	b.byteSize = 0
	b.inp.Records = make([]*kinesis.PutRecordsRequestEntry, 0)
}