package health

import (
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/health"
	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
	"github.com/LF-Engineering/dev-analytics-affiliation/swagger"
	"github.com/go-openapi/runtime/middleware"
)

// Configure setups handlers on api with Service
func Configure(api *operations.DevAnalyticsAffiliationAPI, service Service) {

	api.HealthGetHealthHandler = health.GetHealthHandlerFunc(func(params health.GetHealthParams) middleware.Responder {
		log.Info("entered GetHealthHandler")
		var nilRequestID *string
		requestID := log.GetRequestID(nilRequestID)
		service.SetServiceRequestID(requestID)
		result, err := service.GetHealth(params.HTTPRequest.Context())
		if err != nil {
			return health.NewGetHealthBadRequest().WithPayload(swagger.ErrorResponse(err))
		}
		return health.NewGetHealthOK().WithXREQUESTID(requestID).WithPayload(result)
	})
}
