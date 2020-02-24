package logging

import (
	"crypto/rand"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/gommon/log"
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

// init initializes the logger
func init() {
	logger.SetFormatter(&logrus.JSONFormatter{})

	// Only log the warning severity or above.
	// Default log level
	logger.SetLevel(logrus.DebugLevel)

	EnvLogLevel := os.Getenv("LOG_LEVEL")
	if EnvLogLevel == "debug" {
		logger.SetLevel(logrus.DebugLevel)
	} else if EnvLogLevel == "info" {
		logger.SetLevel(logrus.InfoLevel)
	} else if EnvLogLevel == "warn" {
		logger.SetLevel(logrus.WarnLevel)
	}
}

// WithField log message with field
func WithField(key string, value interface{}) *logrus.Entry {
	return logger.WithField(key, value)
}

// Warn log message
func Warn(msg string) {
	logger.Warn(msg)
}

// Warnf log message
func Warnf(msg string, args ...interface{}) {
	logger.Warnf(msg, args...)
}

// Info log message
func Info(msg string) {
	logger.Info(msg)
}

// Infof log message
func Infof(msg string, args ...interface{}) {
	logger.Infof(msg, args...)
}

// Debug log message
func Debug(msg string) {
	logger.Debug(msg)
}

// Debugf log message
func Debugf(msg string, args ...interface{}) {
	logger.Debugf(msg, args...)
}

// Error log message with fields
func Error(trace string, err error) {
	logger.WithFields(logrus.Fields{
		"line": trace,
	}).Error(err)
}

// Fatal log message
func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

// Fatalf log message
func Fatalf(msg string, args ...interface{}) {
	logger.Fatalf(msg, args...)
}

// Panic log message
func Panic(args ...interface{}) {
	logger.Panic(args...)
}

// Panicf log message
func Panicf(msg string, args ...interface{}) {
	logger.Panicf(msg, args...)
}

// Println log message
func Println(args ...interface{}) {
	logger.Println(args...)
}

// Printf ...
func Printf(msg string, args ...interface{}) {
	logger.Printf(msg, args...)
}

// WithFields logs a message with fields
func WithFields(fields logrus.Fields) *logrus.Entry {
	return logger.WithFields(fields)
}

// WithError logs a message with the specified error
func WithError(err error) *logrus.Entry {
	return logger.WithField("error", err)
}

// Trace returns the source code line and function name (of the calling function)
func Trace() (line string) {
	pc := make([]uintptr, 15)
	n := runtime.Callers(2, pc)
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()

	return fmt.Sprintf("%s,:%d %s\n", frame.File, frame.Line, frame.Function)
}

// StripSpecialChars strips newlines and tabs from a string
func StripSpecialChars(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '\t', '\n':
			return ' '
		default:
			return r
		}
	}, s)
}

// GenerateUUID is function to generate our own uuid if the google uuid throws error
func GenerateUUID() string {
	log.Info("entering func generateUUID")
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Error(Trace(), err)
		return ""
	}
	theUUID := fmt.Sprintf("%x-%x-%x-%x-%x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return theUUID
}

// GetRequestID is function to generate uuid as request id if client doesn't pass X-REQUEST-ID request header
func GetRequestID(requestIDParams *string) string {
	log.Debug("entering func getRequestID")
	//generate UUID as request ID if it doesn't exist in request header
	if requestIDParams == nil || *requestIDParams == "" {
		theUUID, err := uuid.NewUUID()
		newUUID := ""
		if err == nil {
			newUUID = theUUID.String()
		} else {
			newUUID = GenerateUUID()
		}
		requestIDParams = &newUUID
	}
	return *requestIDParams
}
