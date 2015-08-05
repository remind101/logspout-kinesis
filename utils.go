package kinesis

import (
	"fmt"
	"os"
	"text/template"
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
