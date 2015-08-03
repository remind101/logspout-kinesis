package kinesis

import (
	"bytes"
	"errors"
	"fmt"
	"html/template"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
)

// PutRecordsLimit is the maximum number of records allowed for a PutRecords request.
var PutRecordsLimit = 500

// RecordSizeLimit is the maximum allowed size per record.
var RecordSizeLimit int = 1 * 1024 * 1024 // 1MB

// PutRecordsSizeLimit is the maximum allowed size per PutRecords request.
var PutRecordsSizeLimit int = 5 * 1024 * 1024 // 5MB

type recordBuffer struct {
	client   *kinesis.Kinesis
	pKeyTmpl *template.Template
	input    *kinesis.PutRecordsInput
	count    int
	byteSize int
}

func newRecordBuffer(client *kinesis.Kinesis, streamName string) (*recordBuffer, error) {
	pKeyTmpl, err := pKeyTmpl()
	if err != nil {
		return nil, err
	}

	input := &kinesis.PutRecordsInput{
		StreamName: aws.String(streamName),
		Records:    make([]*kinesis.PutRecordsRequestEntry, 0),
	}

	return &recordBuffer{
		client:   client,
		pKeyTmpl: pKeyTmpl,
		input:    input,
	}, nil
}

func pKeyTmpl() (*template.Template, error) {
	pKeyTmplString := os.Getenv("KINESIS_PARTITION_KEY_TEMPLATE")
	if pKeyTmplString == "" {
		return nil, errors.New("The partition key template is missing. Please set the KINESIS_PARTITION_KEY_TEMPLATE env variable")
	}

	pKeyTmpl, err := template.New("kinesisPartitionKey").Parse(pKeyTmplString)
	if err != nil {
		return nil, err
	}

	return pKeyTmpl, nil
}

func (r *recordBuffer) Add(m *router.Message) error {
	data := m.Data
	dataLen := len(data)

	// This record is too large, we can't submit it to kinesis.
	if dataLen > RecordSizeLimit {
		return errors.New(fmt.Sprintf("recordBuffer.Add: log data byte size (%d) is over the limit.", dataLen))
	}

	// Adding this event would make our request have too many records. Flush first.
	if r.count+1 > PutRecordsLimit {
		err := r.Flush()
		if err != nil {
			return err
		}
	}

	// Adding this event would make our request too large. Flush first.
	if r.byteSize+dataLen > PutRecordsSizeLimit {
		err := r.Flush()
		if err != nil {
			return err
		}
	}

	// Partition key
	pKey, err := pKey(r.pKeyTmpl, m)
	if err != nil {
		return err
	}

	// Add to count
	r.count += 1

	// Add data and partition key size to byteSize
	r.byteSize += dataLen + len(pKey)

	// Add record
	r.input.Records = append(r.input.Records, &kinesis.PutRecordsRequestEntry{
		Data:         []byte(data),
		PartitionKey: aws.String(pKey),
	})

	return nil
}

func (r *recordBuffer) Flush() error {
	if r.count == 0 {
		return nil
	}

	defer r.reset()

	_, err := r.client.PutRecords(r.input)
	if err != nil {
		return err
	}

	return nil
}

func (r *recordBuffer) reset() {
	r.count = 0
	r.byteSize = 0
	r.input.Records = make([]*kinesis.PutRecordsRequestEntry, 0)
}

func pKey(tmpl *template.Template, m *router.Message) (string, error) {
	var pKey bytes.Buffer
	err := tmpl.Execute(&pKey, m)
	if err != nil {
		return "", err
	}

	return pKey.String(), nil
}
