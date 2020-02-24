package swagger

import (
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/health"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
)

type codedResponse interface {
	Code() string
}

// ErrorResponse wraps the error in the api standard models.ErrorResponse object
func ErrorResponse(err error) *models.ErrorResponse {
	cd := ""
	if e, ok := err.(codedResponse); ok {
		cd = e.Code()
	}

	return &models.ErrorResponse{
		Code:    cd,
		Message: err.Error(),
	}
}

// ErrorHandler is a convenience method for returning the appropriate response based on the error
func ErrorHandler(label string, err error) middleware.Responder {
	logrus.WithError(err).Error(label)
	return health.NewGetHealthBadRequest().WithPayload(ErrorResponse(err))
}
