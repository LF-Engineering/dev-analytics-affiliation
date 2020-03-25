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
	maxPayload := 0x8000
	logPayload := func(payload interface{}) interface{} {
		s := fmt.Sprintf("%+v", payload)
		l := len(s)
		if l > maxPayload {
			half := maxPayload >> 1
			s = s[0:half] + "(...)" + s[len(s)-half:]
			return s
		}
		return payload
	}
	api.AffiliationGetTopContributorsHandler = affiliation.GetTopContributorsHandlerFunc(
		func(params affiliation.GetTopContributorsParams) middleware.Responder {
			log.Info("GetTopContributorsHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetTopContributorsHandlerFunc: " + info)

			result, err := service.GetTopContributors(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetTopContributorsHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetTopContributorsHandlerFunc(ok): " + info)

			return affiliation.NewGetTopContributorsOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetUnaffiliatedHandler = affiliation.GetUnaffiliatedHandlerFunc(
		func(params affiliation.GetUnaffiliatedParams) middleware.Responder {
			log.Info("GetUnaffiliatedHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetUnaffiliatedHandlerFunc: " + info)

			result, err := service.GetUnaffiliated(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetUnaffiliatedHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetUnaffiliatedHandlerFunc(ok): " + info)

			return affiliation.NewGetUnaffiliatedOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
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
				"Payload":      logPayload(result),
			}).Info("GetListOrganizationsHandlerFunc(ok): " + info)

			return affiliation.NewGetListOrganizationsOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetFindOrganizationByIDHandler = affiliation.GetFindOrganizationByIDHandlerFunc(
		func(params affiliation.GetFindOrganizationByIDParams) middleware.Responder {
			log.Info("GetFindOrganizationByIDHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetFindOrganizationByIDHandlerFunc: " + info)

			result, err := service.GetFindOrganizationByID(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetFindOrganizationByIDHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetFindOrganizationByIDHandlerFunc(ok): " + info)

			return affiliation.NewGetFindOrganizationByIDOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetFindOrganizationByNameHandler = affiliation.GetFindOrganizationByNameHandlerFunc(
		func(params affiliation.GetFindOrganizationByNameParams) middleware.Responder {
			log.Info("GetFindOrganizationByNameHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetFindOrganizationByNameHandlerFunc: " + info)

			result, err := service.GetFindOrganizationByName(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetFindOrganizationByNameHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetFindOrganizationByNameHandlerFunc(ok): " + info)

			return affiliation.NewGetFindOrganizationByNameOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationDeleteOrganizationHandler = affiliation.DeleteOrganizationHandlerFunc(
		func(params affiliation.DeleteOrganizationParams) middleware.Responder {
			log.Info("DeleteOrganizationHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("DeleteOrganizationHandlerFunc: " + info)

			result, err := service.DeleteOrganization(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("DeleteOrganizationHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("DeleteOrganizationHandlerFunc(ok): " + info)

			return affiliation.NewDeleteOrganizationOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPostAddOrganizationHandler = affiliation.PostAddOrganizationHandlerFunc(
		func(params affiliation.PostAddOrganizationParams) middleware.Responder {
			log.Info("PostAddOrganizationHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PostAddOrganizationHandlerFunc: " + info)

			result, err := service.PostAddOrganization(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PostAddOrganizationHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PostAddOrganizationHandlerFunc(ok): " + info)

			return affiliation.NewPostAddOrganizationOK().WithXREQUESTID(requestID).WithPayload(result)
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
				"Payload":      logPayload(result),
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
				"Payload":      logPayload(result),
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
				"Payload":      logPayload(result),
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
				"Payload":      logPayload(result),
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
				"Payload":      logPayload(result),
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
				"Payload":      logPayload(result),
			}).Info("PutMoveIdentityHandlerFunc(ok): " + info)

			return affiliation.NewPutMoveIdentityOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
}
