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
	api.AffiliationPutMergeProfilesHandler = affiliation.PutMergeProfilesHandlerFunc(
		func(params affiliation.PutMergeProfilesParams) middleware.Responder {
			log.Info("entering PutMergeProfilesHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutMergeProfilesHandlerFunc")

			result, err := service.PutMergeProfiles(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutMergeProfiles", err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      result,
			}).Info("PutMergeProfilesHandlerFunc")

			return affiliation.NewPutMergeProfilesOK().WithXREQUESTID(requestID).WithPayload(result)
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
