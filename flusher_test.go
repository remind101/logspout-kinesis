package kinesis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlusher_EmptyBuffer(t *testing.T) {
	b := newBuffer(tmpl, "abc")
	f := newFlusher(nil)
	w := newWriter(b, f)

	err := w.flusher.flush(*b)
	if assert.Error(t, err, "An empty buffer error was expected.") {
		assert.Equal(t, err, &ErrEmptyBuffer{s: "abc"})
	}
}
