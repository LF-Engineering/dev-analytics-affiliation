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

			return affiliation.NewPutOrgDomainOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutMergeUniqueIdentitiesHandler = affiliation.PutMergeUniqueIdentitiesHandlerFunc(
		func(params affiliation.PutMergeUniqueIdentitiesParams) middleware.Responder {
			log.Info("entering PutMergeUniqueIdentitiesHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutMergeUniqueIdentitiesHandlerFunc")

			result, err := service.PutMergeUniqueIdentities(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutMergeUniqueIdentities", err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      result,
			}).Info("PutMergeUniqueIdentitiesHandlerFunc")

			return affiliation.NewPutMergeUniqueIdentitiesOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutMoveIdentityHandler = affiliation.PutMoveIdentityHandlerFunc(
		func(params affiliation.PutMoveIdentityParams) middleware.Responder {
			log.Info("entering PutMoveIdentityHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutMoveIdentityHandlerFunc")

			result, err := service.PutMoveIdentity(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutMoveIdentity", err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      result,
			}).Info("PutMoveIdentityHandlerFunc")

			return affiliation.NewPutMoveIdentityOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
}
