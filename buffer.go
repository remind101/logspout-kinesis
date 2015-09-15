package kinesis

import (
	"errors"
	"text/template"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
	"github.com/pborman/uuid"
)

// ErrRecordTooBig is raised when a record is too big to be sent.
var ErrRecordTooBig = errors.New("data byte size is over the limit")

type limits struct {
	putRecords     int
	putRecordsSize int
	recordSize     int
}

const (
	// PutRecordsLimit is the maximum number of records allowed for a PutRecords request.
	PutRecordsLimit int = 500

	// PutRecordsSizeLimit is the maximum allowed size per PutRecords request.
	PutRecordsSizeLimit int = 5 * 1024 * 1024 // 5MB

	// RecordSizeLimit is the maximum allowed size per record.
	RecordSizeLimit int = 1 * 1024 * 1024 // 1MB
)

type buffer struct {
	count    int
	byteSize int
	pKeyTmpl *template.Template
	input    *kinesis.PutRecordsInput
	limits   *limits
}

func newBuffer(tmpl *template.Template, sn string) *buffer {
	return &buffer{
		pKeyTmpl: tmpl,
		input: &kinesis.PutRecordsInput{
			StreamName: aws.String(sn),
			Records:    make([]*kinesis.PutRecordsRequestEntry, 0),
		},
		limits: &limits{
			putRecords:     PutRecordsLimit,
			putRecordsSize: PutRecordsSizeLimit,
			recordSize:     RecordSizeLimit,
		},
	}
}

func (b *buffer) add(m *router.Message) error {
	dataLen := len(m.Data)

	// This record is too large, we can't submit it to kinesis.
	if dataLen > b.limits.recordSize {
		return ErrRecordTooBig
	}

	pKey, err := executeTmpl(b.pKeyTmpl, m)
	if err != nil {
		return err
	}

	// We default to a uuid if the template didn't match.
	if pKey == "" {
		pKey = uuid.New()
		debug("the partition key is an empty string, defaulting to a uuid %s", pKey)
	}

	// Add to count
	b.count++

	// Add data and partition key size to byteSize
	b.byteSize += dataLen + len(pKey)

	// Add record
	b.input.Records = append(b.input.Records, &kinesis.PutRecordsRequestEntry{
		Data:         []byte(m.Data),
		PartitionKey: aws.String(pKey),
	})

	debug("record added, stream: %s, partition key: %s, length: %d",
		*b.input.StreamName, pKey, len(b.input.Records))

	return nil
}

func (b *buffer) full(m *router.Message) bool {
	// Adding this event would make our request have too many records.
	if b.count+1 > b.limits.putRecords {
		return true
	}

	// Adding this event would make our request too large.
	if b.byteSize+len(m.Data) > b.limits.putRecordsSize {
		return true
	}

	return false
}

func (b *buffer) empty() bool {
	return b.count == 0
}

func (b *buffer) reset() {
	b.count = 0
	b.byteSize = 0
	b.input.Records = make([]*kinesis.PutRecordsRequestEntry, 0)

	debug("buffer reset, stream: %s", *b.input.StreamName)
}
