package kinesis

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"text/template"

	"github.com/gliderlabs/logspout/router"
)

var funcMap = template.FuncMap{
	"lookUp": lookUp,
}

// ErrEmptyTmpl is returned when the template is empty.
var ErrEmptyTmpl = errors.New("the template is empty")

// MissingEnvVarError is return when an environment variable is missing.
type MissingEnvVarError struct {
	EnvVar string
}

func (e *MissingEnvVarError) Error() string {
	return fmt.Sprintf("missing required %s environment variable", e.EnvVar)
}

func compileTmpl(envVar string) (*template.Template, error) {
	tmplString := os.Getenv(envVar)
	if tmplString == "" {
		return nil, &MissingEnvVarError{EnvVar: envVar}
	}

	tmpl, err := template.New("").Funcs(funcMap).Parse(tmplString)
	if err != nil {
		return nil, err
	}

	return tmpl, nil
}

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

// lookUp searches into an array of environment variable by key,
// and returns the value.
func lookUp(arr []string, key string) string {
	for _, v := range arr {
		parts := strings.Split(v, "=")
		if key == parts[0] {
			return parts[1]
		}
	}
	return ""
}
