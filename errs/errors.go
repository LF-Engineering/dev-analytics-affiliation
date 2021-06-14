package errs

import (
	"github.com/pkg/errors"
)

// HTTP status codes
const (
	OK              = "200"
	Created         = "201"
	ErrBadRequest   = "400"
	ErrUnauthorized = "401"
	ErrForbidden    = "403"
	ErrNotFound     = "404"
	ErrServerError  = "500"
	ErrConflict     = "409"
)

// AffsError is an error type that also holds a status code
type AffsError struct {
	Message string
	Status  string
}

// CodedError represents an error with a code
type CodedError interface {
	Code() string
	error
}

// Error implements the error interface
func (m AffsError) Error() string {
	return m.Message
}

// Code implements coded response interface in swagger
func (m AffsError) Code() string {
	return m.Status
}

// StatusText returns the status code in human text
func StatusText(code string) string {
	switch code {
	case OK:
		return "OK"
	case Created:
		return "CREATED"
	case ErrBadRequest:
		return "BAD REQUEST"
	case ErrUnauthorized:
		return "UNAUTHORIZED"
	case ErrForbidden:
		return "FORBIDDEN"
	case ErrNotFound:
		return "NOT FOUND"
	case ErrServerError:
		return "INTERNAL SERVER ERROR"
	default:
		return code
	}
}

// New - returns a new AffsError based on error provided + given status code
func New(err error, status string) error {
	return AffsError{
		Status:  status,
		Message: err.Error(),
	}
}

// Wrap creates a new error with the same static code if present
func Wrap(err error, message string) error {
	e, ok := err.(CodedError)
	if !ok {
		return errors.Wrap(err, message)
	}
	return AffsError{
		Status:  e.Code(),
		Message: errors.Wrap(err, message).Error(),
	}
}
