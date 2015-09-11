package kineprod

import (
	"bytes"
	"errors"
	"log"
	"os"
	"text/template"

	"github.com/gliderlabs/logspout/router"
)

var ErrEmptyTmpl = errors.New("the template is empty")

func executeTmpl(tmpl *template.Template, m *router.Message) (string, error) {
	if tmpl == nil {
		return "", ErrEmptyTmpl
	}

	var res bytes.Buffer
	err := tmpl.Execute(&res, m)
	if err != nil {
		return "", err
	}

	return res.String(), nil
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
