package kinesis

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"text/template"

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

// Prevent Flush() and reset() race condition.
var mutex sync.Mutex

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

type recordSizeLimitError struct {
	caller string
	length int
}

func (e *recordSizeLimitError) Error() string {
	return fmt.Sprintf("%s: log data byte size (%d) is over the limit.", e.caller, e.length)
}

// Add fills the buffer with new data, or flushes it if one of the limit
// has hit.
func (r *recordBuffer) Add(m *router.Message) error {
	data := m.Data
	dataLen := len(data)

	// This record is too large, we can't submit it to kinesis.
	if dataLen > RecordSizeLimit {
		return &recordSizeLimitError{
			caller: "recordBuffer.Add",
			length: dataLen,
		}
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

	log.Printf("kinesis: record added, stream name: %s, partition key: %s, length: %d\n",
		*r.input.StreamName, pKey, len(r.input.Records))

	return nil
}

// Flush flushes the buffer.
func (r *recordBuffer) Flush() error {
	mutex.Lock()

	if r.count == 0 {
		return nil
	}

	defer r.reset()

	_, err := r.client.PutRecords(r.input)
	if err != nil {
		return err
	}

	log.Printf("kinesis: buffer flushed, stream name: %s, length: %d\n",
		*r.input.StreamName, len(r.input.Records))

	mutex.Unlock()
	return nil
}

func (r *recordBuffer) reset() {
	mutex.Lock()
	defer mutex.Unlock()

	r.count = 0
	r.byteSize = 0
	r.input.Records = make([]*kinesis.PutRecordsRequestEntry, 0)

	log.Printf("kinesis: buffer reset, stream name: %s, length: %d\n",
		*r.input.StreamName, len(r.input.Records))
}

func pKey(tmpl *template.Template, m *router.Message) (string, error) {
	var pKey bytes.Buffer
	err := tmpl.Execute(&pKey, m)
	if err != nil {
		return "", err
	}

	return pKey.String(), nil
}
