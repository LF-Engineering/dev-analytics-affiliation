package affiliation

import (
	"fmt"
	"strings"

	"net/http"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/affiliation"
	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
	"github.com/LF-Engineering/dev-analytics-affiliation/swagger"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
)

// Configure setups handlers on api with Service
func Configure(api *operations.DevAnalyticsAffiliationAPI, service Service) {
	requestInfo := func(r *http.Request) string {
		agent := ""
		hdr := r.Header
		if hdr != nil {
			uAgentAry, ok := hdr["User-Agent"]
			if ok {
				agent = strings.Join(uAgentAry, ", ")
			}
		}
		if agent != "" {
			return fmt.Sprintf("Request IP: %s, Request Agent: %s", r.RemoteAddr, agent)
		}
		return fmt.Sprintf("Request IP: %s", r.RemoteAddr)
	}
	api.AffiliationGetListOrganizationsHandler = affiliation.GetListOrganizationsHandlerFunc(
		func(params affiliation.GetListOrganizationsParams) middleware.Responder {
			log.Info("GetListOrganizationsHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetListOrganizationsHandlerFunc: " + info)

			result, err := service.GetListOrganizations(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetListOrganizationsHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      result,
			}).Info("GetListOrganizationsHandlerFunc(ok): " + info)

			return affiliation.NewGetListOrganizationsOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetMatchingBlacklistHandler = affiliation.GetMatchingBlacklistHandlerFunc(
		func(params affiliation.GetMatchingBlacklistParams) middleware.Responder {
			log.Info("GetMatchingBlacklistHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetMatchingBlacklistHandlerFunc: " + info)

			result, err := service.GetMatchingBlacklist(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetMatchingBlacklistHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      result,
			}).Info("GetMatchingBlacklistHandlerFunc(ok): " + info)

			return affiliation.NewGetMatchingBlacklistOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPostMatchingBlacklistHandler = affiliation.PostMatchingBlacklistHandlerFunc(
		func(params affiliation.PostMatchingBlacklistParams) middleware.Responder {
			log.Info("PostMatchingBlacklistHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PostMatchingBlacklistHandlerFunc: " + info)

			result, err := service.PostMatchingBlacklist(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PostMatchingBlacklistHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      result,
			}).Info("PostMatchingBlacklistHandlerFunc(ok): " + info)

			return affiliation.NewPostMatchingBlacklistOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationDeleteMatchingBlacklistHandler = affiliation.DeleteMatchingBlacklistHandlerFunc(
		func(params affiliation.DeleteMatchingBlacklistParams) middleware.Responder {
			log.Info("DeleteMatchingBlacklistHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("DeleteMatchingBlacklistHandlerFunc: " + info)

			result, err := service.DeleteMatchingBlacklist(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("DeleteMatchingBlacklistHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      result,
			}).Info("DeleteMatchingBlacklistHandlerFunc(ok): " + info)

			return affiliation.NewDeleteMatchingBlacklistOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutOrgDomainHandler = affiliation.PutOrgDomainHandlerFunc(
		func(params affiliation.PutOrgDomainParams) middleware.Responder {
			log.Info("PutOrgDomainHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutOrgDomainHandlerFunc: " + info)

			result, err := service.PutOrgDomain(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutOrgDomainHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      result,
			}).Info("PutOrgDomainHandlerFunc(ok): " + info)

			return affiliation.NewPutOrgDomainOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutMergeUniqueIdentitiesHandler = affiliation.PutMergeUniqueIdentitiesHandlerFunc(
		func(params affiliation.PutMergeUniqueIdentitiesParams) middleware.Responder {
			log.Info("PutMergeUniqueIdentitiesHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutMergeUniqueIdentitiesHandlerFunc: " + info)

			result, err := service.PutMergeUniqueIdentities(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutMergeUniqueIdentitiesHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      result,
			}).Info("PutMergeUniqueIdentitiesHandlerFunc(ok): " + info)

			return affiliation.NewPutMergeUniqueIdentitiesOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutMoveIdentityHandler = affiliation.PutMoveIdentityHandlerFunc(
		func(params affiliation.PutMoveIdentityParams) middleware.Responder {
			log.Info("PutMoveIdentityHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutMoveIdentityHandlerFunc: " + info)

			result, err := service.PutMoveIdentity(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutMoveIdentityHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      result,
			}).Info("PutMoveIdentityHandlerFunc(ok): " + info)

			return affiliation.NewPutMoveIdentityOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
}
