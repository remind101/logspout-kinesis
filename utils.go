package kinesis

import (
	"bytes"
	"fmt"
	"os"
	"text/template"

	"github.com/gliderlabs/logspout/router"
)

type missingEnvVarError struct {
	envVar   string
	tmplName string
}

func (e *missingEnvVarError) Error() string {
	return fmt.Sprintf("The %s template is missing. Please set the %s env variable\n", e.tmplName, e.envVar)
}

func compileTmpl(envVar string, tmplName string) (*template.Template, error) {
	tmplString := os.Getenv(envVar)
	if tmplString == "" {
		return nil, &missingEnvVarError{envVar: envVar, tmplName: tmplName}
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
