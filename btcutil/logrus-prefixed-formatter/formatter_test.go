package prefixed_test

import (
	. "study/logger/logrus-prefixed-formatter"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func TestFormatter_SuppressErrorStackTraces(t *testing.T) {
	formatter := new(TextFormatter)
	formatter.ForceFormatting = true
	log := logrus.New()
	log.Formatter = formatter
	output := new(LogOutput)
	log.Out = output

	errFn := func() error {
		return errors.New("inner")
	}

	log.WithError(errors.Wrap(errFn(), "outer")).Error("test")
}

func TestFormatter_EscapesControlCharacters(t *testing.T) {
	formatter := new(TextFormatter)
	formatter.ForceFormatting = true
	log := logrus.New()
	log.Formatter = formatter
	output := new(LogOutput)
	log.Out = output

	log.WithField("test", "foo\nbar").Error("testing things")
}
