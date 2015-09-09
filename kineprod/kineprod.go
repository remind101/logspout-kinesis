package kineprod

import (
	"bytes"
	"log"
	"os"
	"text/template"

	"github.com/gliderlabs/logspout/router"
)

func executeTmpl(tmpl *template.Template, m *router.Message) string {
	var res bytes.Buffer
	err := tmpl.Execute(&res, m)
	if err != nil {
		return ""
	}

	return res.String()
}

func logErr(err error) {
	if err != nil {
		log.Println("kinesis: ", err.Error())
	}
}

func debugLog(format string, p ...interface{}) {
	if os.Getenv("KINESIS_DEBUG") == "true" {
		log.Printf(format, p...)
	}
}
