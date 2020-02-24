package affiliation

import (
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/affiliation"
	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
	"github.com/LF-Engineering/dev-analytics-affiliation/swagger"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
)

// Configure setups handlers on api with Service
func Configure(api *operations.DevAnalyticsAffiliationAPI, service Service) {

	api.AffiliationPutOrgDomainHandler = affiliation.PutOrgDomainHandlerFunc(
		func(params affiliation.PutOrgDomainParams) middleware.Responder {
			log.Info("entering PutOrgDomainHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutOrgDomainHandlerFunc")

			result, err := service.PutOrgDomain(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutOrgDomain", err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      result,
			}).Info("PutOrgDomainHandlerFunc")

			//return affiliation.NewPutOrgDomainOK().WithXREQUESTID(requestID).WithPayload(result)
			return affiliation.NewPutOrgDomainOK().WithXREQUESTID(requestID)
		},
	)
}
