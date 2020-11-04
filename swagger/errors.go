package swagger

import (
	"strings"

	"github.com/LF-Engineering/dev-analytics-affiliation/errs"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/health"
	"github.com/LF-Engineering/dev-analytics-affiliation/shared"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
)

// ErrorResponse wraps the error in the api standard models.ErrorResponse object
func ErrorResponse(err error) *models.ErrorResponse {
	cd := ""
	if e, ok := err.(errs.CodedError); ok {
		cd = e.Code()
	}
	errMsg := err.Error()
	for redacted := range shared.GRedacted {
		if len(redacted) > 3 {
			errMsg = strings.Replace(errMsg, redacted, "[redacted]", -1)
		}
	}
	return &models.ErrorResponse{
		Code:    cd,
		Message: errMsg,
	}
}

// ErrorHandler is a convenience method for returning the appropriate response based on the error
func ErrorHandler(label string, err error) middleware.Responder {
	logrus.WithError(err).Error(label)
	e, ok := err.(errs.CodedError)
	if !ok {
		return health.NewGetHealthBadRequest().WithPayload(ErrorResponse(err))
	}

	switch e.Code() {
	case errs.ErrBadRequest:
		return health.NewGetHealthBadRequest().WithPayload(ErrorResponse(err))
	case errs.ErrUnauthorized:
		return health.NewGetHealthUnauthorized().WithPayload(ErrorResponse(err))
	case errs.ErrForbidden:
		return health.NewGetHealthForbidden().WithPayload(ErrorResponse(err))
	case errs.ErrNotFound:
		return health.NewGetHealthNotFound().WithPayload(ErrorResponse(err))
	default:
		return health.NewGetHealthInternalServerError().WithPayload(ErrorResponse(err))
	}
}
