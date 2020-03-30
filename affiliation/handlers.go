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
	api.AffiliationGetListOrganizationsDomainsHandler = affiliation.GetListOrganizationsDomainsHandlerFunc(
		func(params affiliation.GetListOrganizationsDomainsParams) middleware.Responder {
			log.Info("GetListOrganizationsDomainsHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetListOrganizationsDomainsHandlerFunc: " + info)

			result, err := service.GetListOrganizationsDomains(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetListOrganizationsDomainsHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetListOrganizationsDomainsHandlerFunc(ok): " + info)

			return affiliation.NewGetListOrganizationsDomainsOK().WithXREQUESTID(requestID).WithPayload(result)
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
	api.AffiliationDeleteProfileHandler = affiliation.DeleteProfileHandlerFunc(
		func(params affiliation.DeleteProfileParams) middleware.Responder {
			log.Info("DeleteProfileHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("DeleteProfileHandlerFunc: " + info)

			result, err := service.DeleteProfile(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("DeleteProfileHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("DeleteProfileHandlerFunc(ok): " + info)

			return affiliation.NewDeleteProfileOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPostUnarchiveProfileHandler = affiliation.PostUnarchiveProfileHandlerFunc(
		func(params affiliation.PostUnarchiveProfileParams) middleware.Responder {
			log.Info("PostUnarchiveProfileHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PostUnarchiveProfileHandlerFunc: " + info)

			result, err := service.PostUnarchiveProfile(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PostUnarchiveProfileHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PostUnarchiveProfileHandlerFunc(ok): " + info)

			return affiliation.NewPostUnarchiveProfileOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPostAddUniqueIdentityHandler = affiliation.PostAddUniqueIdentityHandlerFunc(
		func(params affiliation.PostAddUniqueIdentityParams) middleware.Responder {
			log.Info("PostAddUniqueIdentityHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PostAddUniqueIdentityHandlerFunc: " + info)

			result, err := service.PostAddUniqueIdentity(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PostAddUniqueIdentityHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PostAddUniqueIdentityHandlerFunc(ok): " + info)

			return affiliation.NewPostAddUniqueIdentityOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPostAddIdentityHandler = affiliation.PostAddIdentityHandlerFunc(
		func(params affiliation.PostAddIdentityParams) middleware.Responder {
			log.Info("PostAddIdentityHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PostAddIdentityHandlerFunc: " + info)

			result, err := service.PostAddIdentity(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PostAddIdentityHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PostAddIdentityHandlerFunc(ok): " + info)

			return affiliation.NewPostAddIdentityOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationDeleteIdentityHandler = affiliation.DeleteIdentityHandlerFunc(
		func(params affiliation.DeleteIdentityParams) middleware.Responder {
			log.Info("DeleteIdentityHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("DeleteIdentityHandlerFunc: " + info)

			result, err := service.DeleteIdentity(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("DeleteIdentityHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("DeleteIdentityHandlerFunc(ok): " + info)

			return affiliation.NewDeleteIdentityOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetProfileHandler = affiliation.GetProfileHandlerFunc(
		func(params affiliation.GetProfileParams) middleware.Responder {
			log.Info("GetProfileHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetProfileHandlerFunc: " + info)

			result, err := service.GetProfile(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetProfileHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetProfileHandlerFunc(ok): " + info)

			return affiliation.NewGetProfileOK().WithXREQUESTID(requestID).WithPayload(result)
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
	api.AffiliationPutEditOrganizationHandler = affiliation.PutEditOrganizationHandlerFunc(
		func(params affiliation.PutEditOrganizationParams) middleware.Responder {
			log.Info("PutEditOrganizationHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutEditOrganizationHandlerFunc: " + info)

			result, err := service.PutEditOrganization(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutEditOrganizationHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutEditOrganizationHandlerFunc(ok): " + info)

			return affiliation.NewPutEditOrganizationOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutEditProfileHandler = affiliation.PutEditProfileHandlerFunc(
		func(params affiliation.PutEditProfileParams) middleware.Responder {
			log.Info("PutEditProfileHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutEditProfileHandlerFunc: " + info)

			result, err := service.PutEditProfile(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutEditProfileHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutEditProfileHandlerFunc(ok): " + info)

			return affiliation.NewPutEditProfileOK().WithXREQUESTID(requestID).WithPayload(result)
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
	api.AffiliationDeleteOrgDomainHandler = affiliation.DeleteOrgDomainHandlerFunc(
		func(params affiliation.DeleteOrgDomainParams) middleware.Responder {
			log.Info("DeleteOrgDomainHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("DeleteOrgDomainHandlerFunc: " + info)

			result, err := service.DeleteOrgDomain(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("DeleteOrgDomainHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("DeleteOrgDomainHandlerFunc(ok): " + info)

			return affiliation.NewDeleteOrgDomainOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetListProfilesHandler = affiliation.GetListProfilesHandlerFunc(
		func(params affiliation.GetListProfilesParams) middleware.Responder {
			log.Info("GetListProfilesHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetListProfilesHandlerFunc: " + info)

			result, err := service.GetListProfiles(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetListProfilesHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetListProfilesHandlerFunc(ok): " + info)

			return affiliation.NewGetListProfilesOK().WithXREQUESTID(requestID).WithPayload(result)
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
	api.AffiliationGetProfileEnrollmentsHandler = affiliation.GetProfileEnrollmentsHandlerFunc(
		func(params affiliation.GetProfileEnrollmentsParams) middleware.Responder {
			log.Info("GetProfileEnrollmentsHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetProfileEnrollmentsHandlerFunc: " + info)

			result, err := service.GetProfileEnrollments(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetProfileEnrollmentsHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetProfileEnrollmentsHandlerFunc(ok): " + info)

			return affiliation.NewGetProfileEnrollmentsOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPostAddEnrollmentHandler = affiliation.PostAddEnrollmentHandlerFunc(
		func(params affiliation.PostAddEnrollmentParams) middleware.Responder {
			log.Info("PostAddEnrollmentHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PostAddEnrollmentHandlerFunc: " + info)

			result, err := service.PostAddEnrollment(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PostAddEnrollmentHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PostAddEnrollmentHandlerFunc(ok): " + info)

			return affiliation.NewPostAddEnrollmentOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationDeleteEnrollmentsHandler = affiliation.DeleteEnrollmentsHandlerFunc(
		func(params affiliation.DeleteEnrollmentsParams) middleware.Responder {
			log.Info("DeleteEnrollmentsHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("DeleteEnrollmentsHandlerFunc: " + info)

			result, err := service.DeleteEnrollments(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("DeleteEnrollmentsHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("DeleteEnrollmentsHandlerFunc(ok): " + info)

			return affiliation.NewDeleteEnrollmentsOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutMergeEnrollmentsHandler = affiliation.PutMergeEnrollmentsHandlerFunc(
		func(params affiliation.PutMergeEnrollmentsParams) middleware.Responder {
			log.Info("PutMergeEnrollmentsHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutMergeEnrollmentsHandlerFunc: " + info)

			result, err := service.PutMergeEnrollments(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutMergeEnrollmentsHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutMergeEnrollmentsHandlerFunc(ok): " + info)

			return affiliation.NewPutMergeEnrollmentsOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
}
