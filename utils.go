package kinesis

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"text/template"

	"github.com/gliderlabs/logspout/router"
)

type missingEnvVarError struct {
	envVar string
}

func (e *missingEnvVarError) Error() string {
	return fmt.Sprintf("Missing required %s environment variable.\n", e.envVar)
}

func compileTmpl(envVar string) (*template.Template, error) {
	tmplString := os.Getenv(envVar)
	if tmplString == "" {
		return nil, &missingEnvVarError{envVar: envVar}
	}

	tmpl, err := template.New("").Parse(tmplString)
	if err != nil {
		return nil, err
	}

	return tmpl, nil
}

func executeTmpl(tmpl *template.Template, m *router.Message) (string, error) {
	var res bytes.Buffer
	err := tmpl.Execute(&res, m)
	if err != nil {
		return "", err
	}

	return res.String(), nil
}

func logErr(err error) {
	if err != nil {
		log.Println("kinesis: ", err.Error())
	}
}

func logErrs(err []error) {
	if err != nil {
		for _, e := range err {
			logErr(e)
		}
	}
}
