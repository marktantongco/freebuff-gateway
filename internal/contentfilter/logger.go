package contentfilter

import (
	"fmt"
	"log"
	"os"
)

// Simple logger that wraps the contentfilter needs
var (
	debugLog = log.New(os.Stdout, "DEBUG ", log.LstdFlags)
	infoLog  = log.New(os.Stdout, "INFO  ", log.LstdFlags)
	warnLog  = log.New(os.Stderr, "WARN  ", log.LstdFlags)
	errorLog = log.New(os.Stderr, "ERROR ", log.LstdFlags)
)

// Logger interface for contentfilter
type Logger interface {
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

// defaultLogger implements Logger
type defaultLogger struct{}

func (l *defaultLogger) Debugf(format string, v ...interface{}) {
	debugLog.Output(2, fmt.Sprintf(format, v...))
}

func (l *defaultLogger) Infof(format string, v ...interface{}) {
	infoLog.Output(2, fmt.Sprintf(format, v...))
}

func (l *defaultLogger) Warnf(format string, v ...interface{}) {
	warnLog.Output(2, fmt.Sprintf(format, v...))
}

func (l *defaultLogger) Errorf(format string, v ...interface{}) {
	errorLog.Output(2, fmt.Sprintf(format, v...))
}

// Package-level logger
var logger Logger = &defaultLogger{}

// SetLogger allows setting a custom logger
func SetLogger(l Logger) {
	if l != nil {
		logger = l
	}
}
