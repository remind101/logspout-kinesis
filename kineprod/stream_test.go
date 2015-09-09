package kineprod

import (
	"testing"
	"text/template"

	"github.com/gliderlabs/logspout/router"
)

func TestStream_StreamNotReady(t *testing.T) {
	m := &router.Message{
		Data: "hello",
	}
	pKeyTmpl := &template.Template{}

	s := New("abc", pKeyTmpl)
	s.Start()
	err := s.Write(m)

	if err == nil {
		t.Fatalf("Expected error: %s", ErrStreamNotReady.Error())
	}
}

// type fakeStream struct {
// 	tagging chan bool
// }

// func (s *fakeStream) create() {
// 	close(s.tagging)
// }

// func TestStream_StreamCreationAlreadyExists(t *testing.T) {
// 	f := &fakeStream{
// 		tagging: make(chan bool),
// 	}

// 	f.Start()

// 	select {
// 	case <-f.tagging:
// 	case <-time.After(time.Second):
// 		t.Fatal("Expected tagging to be activated")
// 	}
// }

// // func TestStream_StreamTagging(t *testing.T) {}
