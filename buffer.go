package kinesis

import (
	"fmt"
	"sync"
	"text/template"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gliderlabs/logspout/router"
	"github.com/pborman/uuid"
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
	mutex    sync.Mutex
}

func newRecordBuffer(client *kinesis.Kinesis, streamName string) (*recordBuffer, error) {
	pKeyTmpl, err := compileTmpl("KINESIS_PARTITION_KEY_TEMPLATE")
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

	defer r.mutex.Unlock()
	r.mutex.Lock()

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
	pKey, err := executeTmpl(r.pKeyTmpl, m)
	if err != nil {
		return err
	}

	// We default to a uuid if the template didn't match.
	if pKey == "" {
		pKey = uuid.New()
		debugLog("The partition key is an empty string, defaulting to a uuid %s\n", pKey)
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

	debugLog("kinesis: record added, stream name: %s, partition key: %s, length: %d",
		*r.input.StreamName, pKey, len(r.input.Records))

	return nil
}

// Flush flushes the buffer.
func (r *recordBuffer) Flush() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.count == 0 {
		debugLog("kinesis: nothing to flush, stream name: %s", *r.input.StreamName)
		return nil
	}

	defer r.reset()

	resp, err := r.client.PutRecords(r.input)
	if err != nil {
		return err
	}

	debugLog("kinesis: buffer flushed, stream name: %s, records sent: %d, records failed: %d",
		*r.input.StreamName, len(r.input.Records), *resp.FailedRecordCount)

	return nil
}

func (r *recordBuffer) reset() {
	r.count = 0
	r.byteSize = 0
	r.input.Records = make([]*kinesis.PutRecordsRequestEntry, 0)

	debugLog("kinesis: buffer reset, stream name: %s, length: %d",
		*r.input.StreamName, len(r.input.Records))
}
