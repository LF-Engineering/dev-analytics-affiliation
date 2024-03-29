package affiliation

import (
	"fmt"
	"html"
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
		method := r.Method
		path := html.EscapeString(r.URL.Path)
		if hdr != nil {
			uAgentAry, ok := hdr["User-Agent"]
			if ok {
				agent = strings.Join(uAgentAry, ", ")
			}
		}
		if agent != "" {
			return fmt.Sprintf("Request IP: %s, Request Agent: %s, method: %s, path: %s", r.RemoteAddr, agent, method, path)
		}
		return fmt.Sprintf("Request IP: %s, method: %s, path: %s", r.RemoteAddr, method, path)
	}
	maxPayload := 0x1000
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationGetTopContributorsHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewGetTopContributorsNotAcceptable().WithPayload(nil)
			}
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
	api.AffiliationGetTopContributorsCSVHandler = affiliation.GetTopContributorsCSVHandlerFunc(
		func(params affiliation.GetTopContributorsCSVParams) middleware.Responder {
			log.Info("GetTopContributorsCSVHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetTopContributorsCSVHandlerFunc: " + info)

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationGetTopContributorsCSVHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewGetTopContributorsCSVNotAcceptable().WithPayload(nil)
			}
			result, err := service.GetTopContributorsCSV(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetTopContributorsCSVHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetTopContributorsCSVHandlerFunc(ok): " + info)

			return affiliation.NewGetTopContributorsCSVOK().WithXREQUESTID(requestID).WithPayload(result)
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationGetUnaffiliatedHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewGetUnaffiliatedNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationGetListOrganizationsHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewGetListOrganizationsNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationGetListOrganizationsDomainsHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewGetListOrganizationsDomainsNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationGetFindOrganizationByIDHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewGetFindOrganizationByIDNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationGetFindOrganizationByNameHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewGetFindOrganizationByNameNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationDeleteOrganizationHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewDeleteOrganizationNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationDeleteProfileHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewDeleteProfileNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPostUnarchiveProfileHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPostUnarchiveProfileNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPostAddUniqueIdentityHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPostAddUniqueIdentityNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPostAddIdentityHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPostAddIdentityNotAcceptable().WithPayload(nil)
			}
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
	api.AffiliationPostAddIdentitiesHandler = affiliation.PostAddIdentitiesHandlerFunc(
		func(params affiliation.PostAddIdentitiesParams) middleware.Responder {
			log.Info("PostAddIdentitiesHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PostAddIdentitiesHandlerFunc: " + info)

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPostAddIdentitiesHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPostAddIdentitiesNotAcceptable().WithPayload(nil)
			}
			result, err := service.PostAddIdentities(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PostAddIdentitiesHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PostAddIdentitiesHandlerFunc(ok): " + info)

			return affiliation.NewPostAddIdentitiesOK().WithXREQUESTID(requestID).WithPayload(result)
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationDeleteIdentityHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewDeleteIdentityNotAcceptable().WithPayload(nil)
			}
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
	api.AffiliationGetIdentityHandler = affiliation.GetIdentityHandlerFunc(
		func(params affiliation.GetIdentityParams) middleware.Responder {
			log.Info("GetIdentityHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetIdentityHandlerFunc: " + info)

			result, err := service.GetIdentity(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetIdentityHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetIdentityHandlerFunc(ok): " + info)

			return affiliation.NewGetIdentityOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetIdentityByUserHandler = affiliation.GetIdentityByUserHandlerFunc(
		func(params affiliation.GetIdentityByUserParams) middleware.Responder {
			log.Info("GetIdentityByUserHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetIdentityByUserHandlerFunc: " + info)

			result, err := service.GetIdentityByUser(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetIdentityByUserHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetIdentityHandlerFunc(ok): " + info)

			return affiliation.NewGetIdentityByUserOK().WithXREQUESTID(requestID).WithPayload(result)
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
	api.AffiliationGetProfileByUsernameHandler = affiliation.GetProfileByUsernameHandlerFunc(
		func(params affiliation.GetProfileByUsernameParams) middleware.Responder {
			log.Info("GetProfileByUsernameHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetProfileByUsernameHandlerFunc: " + info)

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationGetProfileByUsernameHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewGetProfileByUsernameNotAcceptable().WithPayload(nil)
			}
			result, err := service.GetProfileByUsername(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetProfileByUsernameHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetProfileByUsernameHandlerFunc(ok): " + info)

			return affiliation.NewGetProfileByUsernameOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetProfileNestedHandler = affiliation.GetProfileNestedHandlerFunc(
		func(params affiliation.GetProfileNestedParams) middleware.Responder {
			log.Info("GetProfileNestedHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetProfileNestedHandlerFunc: " + info)

			result, err := service.GetProfileNested(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetProfileNestedHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetProfileNestedHandlerFunc(ok): " + info)

			return affiliation.NewGetProfileNestedOK().WithXREQUESTID(requestID).WithPayload(result)
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPostAddOrganizationHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPostAddOrganizationNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPutEditOrganizationHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPutEditOrganizationNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPutEditProfileHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPutEditProfileNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationGetMatchingBlacklistHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewGetMatchingBlacklistNotAcceptable().WithPayload(nil)
			}

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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPostMatchingBlacklistHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPostMatchingBlacklistNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationDeleteMatchingBlacklistHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewDeleteMatchingBlacklistNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPutOrgDomainHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPutOrgDomainNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationDeleteOrgDomainHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewDeleteOrgDomainNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationGetListProfilesHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewGetListProfilesNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPutMergeUniqueIdentitiesHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPutMergeUniqueIdentitiesNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPutMoveIdentityHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPutMoveIdentityNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationGetProfileEnrollmentsHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewGetProfileEnrollmentsNotAcceptable().WithPayload(nil)
			}
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPostAddEnrollmentHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPostAddEnrollmentNotAcceptable().WithPayload(nil)
			}
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
	api.AffiliationPutEditEnrollmentHandler = affiliation.PutEditEnrollmentHandlerFunc(
		func(params affiliation.PutEditEnrollmentParams) middleware.Responder {
			log.Info("PutEditEnrollmentHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutEditEnrollmentHandlerFunc: " + info)

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPutEditEnrollmentHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPutEditEnrollmentNotAcceptable().WithPayload(nil)
			}
			result, err := service.PutEditEnrollment(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutEditEnrollmentHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutEditEnrollmentHandlerFunc(ok): " + info)

			return affiliation.NewPutEditEnrollmentOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutEditEnrollmentByIDHandler = affiliation.PutEditEnrollmentByIDHandlerFunc(
		func(params affiliation.PutEditEnrollmentByIDParams) middleware.Responder {
			log.Info("PutEditEnrollmentByIDHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutEditEnrollmentByIDHandlerFunc: " + info)

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPutEditEnrollmentByIDHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPutEditEnrollmentByIDNotAcceptable().WithPayload(nil)
			}
			result, err := service.PutEditEnrollmentByID(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutEditEnrollmentByIDHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutEditEnrollmentByIDHandlerFunc(ok): " + info)

			return affiliation.NewPutEditEnrollmentByIDOK().WithXREQUESTID(requestID).WithPayload(result)
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationDeleteEnrollmentsHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewDeleteEnrollmentsNotAcceptable().WithPayload(nil)
			}
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
	api.AffiliationDeleteEnrollmentHandler = affiliation.DeleteEnrollmentHandlerFunc(
		func(params affiliation.DeleteEnrollmentParams) middleware.Responder {
			log.Info("DeleteEnrollmentHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("DeleteEnrollmentHandlerFunc: " + info)

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationDeleteEnrollmentHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewDeleteEnrollmentNotAcceptable().WithPayload(nil)
			}
			result, err := service.DeleteEnrollment(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("DeleteEnrollmentHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("DeleteEnrollmentHandlerFunc(ok): " + info)

			return affiliation.NewDeleteEnrollmentOK().WithXREQUESTID(requestID).WithPayload(result)
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

			projectSlugs := params.ProjectSlugs
			params.ProjectSlugs = service.SkipDisabledProjects(params.ProjectSlugs)
			if len(params.ProjectSlugs) == 0 {
				log.Info("AffiliationPutMergeEnrollmentsHandler: all projects " + projectSlugs + " are disabled")
				return affiliation.NewPutMergeEnrollmentsNotAcceptable().WithPayload(nil)
			}
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
	api.AffiliationGetAllAffiliationsHandler = affiliation.GetAllAffiliationsHandlerFunc(
		func(params affiliation.GetAllAffiliationsParams) middleware.Responder {
			log.Info("GetAllAffiliationsHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetAllAffiliationsHandlerFunc: " + info)

			result, err := service.GetAllAffiliations(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetAllAffiliationsHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetAllAffiliationsHandlerFunc(ok): " + info)

			return affiliation.NewGetAllAffiliationsOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPostBulkUpdateHandler = affiliation.PostBulkUpdateHandlerFunc(
		func(params affiliation.PostBulkUpdateParams) middleware.Responder {
			log.Info("PostBulkUpdateHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PostBulkUpdateHandlerFunc: " + info)

			result, err := service.PostBulkUpdate(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PostBulkUpdateHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PostBulkUpdateHandlerFunc(ok): " + info)

			return affiliation.NewPostBulkUpdateOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutMergeAllHandler = affiliation.PutMergeAllHandlerFunc(
		func(params affiliation.PutMergeAllParams) middleware.Responder {
			log.Info("PutMergeAllHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutMergeAllHandlerFunc: " + info)

			result, err := service.PutMergeAll(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutMergeAllHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutMergeAllHandlerFunc(ok): " + info)

			return affiliation.NewPutMergeAllOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutSyncSfProfilesHandler = affiliation.PutSyncSfProfilesHandlerFunc(
		func(params affiliation.PutSyncSfProfilesParams) middleware.Responder {
			log.Info("PutSyncSfProfilesHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutSyncSfProfilesHandlerFunc: " + info)

			result, err := service.PutSyncSfProfiles(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutSyncSfProfilesHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutSyncSfProfilesHandlerFunc(ok): " + info)

			return affiliation.NewPutSyncSfProfilesOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutHideEmailsHandler = affiliation.PutHideEmailsHandlerFunc(
		func(params affiliation.PutHideEmailsParams) middleware.Responder {
			log.Info("PutHideEmailsHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutHideEmailsHandlerFunc: " + info)

			result, err := service.PutHideEmails(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutHideEmailsHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutHideEmailsHandlerFunc(ok): " + info)

			return affiliation.NewPutHideEmailsOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutMapOrgNamesHandler = affiliation.PutMapOrgNamesHandlerFunc(
		func(params affiliation.PutMapOrgNamesParams) middleware.Responder {
			log.Info("PutMapOrgNamesHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutMapOrgNamesHandlerFunc: " + info)

			result, err := service.PutMapOrgNames(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutMapOrgNamesHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutMapOrgNamesHandlerFunc(ok): " + info)

			return affiliation.NewPutMapOrgNamesOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetListProjectsHandler = affiliation.GetListProjectsHandlerFunc(
		func(params affiliation.GetListProjectsParams) middleware.Responder {
			log.Info("GetListProjectsHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetListProjectsHandlerFunc: " + info)

			result, err := service.GetListProjects(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetListProjectsHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetListProjectsHandlerFunc(ok): " + info)

			return affiliation.NewGetListProjectsOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutDetAffRangeHandler = affiliation.PutDetAffRangeHandlerFunc(
		func(params affiliation.PutDetAffRangeParams) middleware.Responder {
			log.Info("PutDetAffRangeHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutDetAffRangeHandlerFunc: " + info)

			result, err := service.PutDetAffRange(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutDetAffRangeHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutDetAffRangeHandlerFunc(ok): " + info)

			return affiliation.NewPutDetAffRangeOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetListSlugMappingsHandler = affiliation.GetListSlugMappingsHandlerFunc(
		func(params affiliation.GetListSlugMappingsParams) middleware.Responder {
			log.Info("GetListSlugMappingsHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetListSlugMappingsHandlerFunc: " + info)

			result, err := service.GetListSlugMappings(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetListSlugMappingsHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetListSlugMappingsHandlerFunc(ok): " + info)

			return affiliation.NewGetListSlugMappingsOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetSlugMappingHandler = affiliation.GetSlugMappingHandlerFunc(
		func(params affiliation.GetSlugMappingParams) middleware.Responder {
			log.Info("GetSlugMappingHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetSlugMappingHandlerFunc: " + info)

			result, err := service.GetSlugMapping(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetSlugMappingHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetSlugMappingHandlerFunc(ok): " + info)

			return affiliation.NewGetSlugMappingOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPostAddSlugMappingHandler = affiliation.PostAddSlugMappingHandlerFunc(
		func(params affiliation.PostAddSlugMappingParams) middleware.Responder {
			log.Info("PostAddSlugMappingHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PostAddSlugMappingHandlerFunc: " + info)

			result, err := service.PostAddSlugMapping(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PostAddSlugMappingHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PostAddSlugMappingHandlerFunc(ok): " + info)

			return affiliation.NewPostAddSlugMappingOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationDeleteSlugMappingHandler = affiliation.DeleteSlugMappingHandlerFunc(
		func(params affiliation.DeleteSlugMappingParams) middleware.Responder {
			log.Info("DeleteSlugMappingHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("DeleteSlugMappingHandlerFunc: " + info)

			result, err := service.DeleteSlugMapping(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("DeleteSlugMappingHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("DeleteSlugMappingHandlerFunc(ok): " + info)

			return affiliation.NewDeleteSlugMappingOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutEditSlugMappingHandler = affiliation.PutEditSlugMappingHandlerFunc(
		func(params affiliation.PutEditSlugMappingParams) middleware.Responder {
			log.Info("PutEditSlugMappingHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutEditSlugMappingHandlerFunc: " + info)

			result, err := service.PutEditSlugMapping(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutEditSlugMappingHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutEditSlugMappingHandlerFunc(ok): " + info)

			return affiliation.NewPutEditSlugMappingOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationPutCacheTopContributorsHandler = affiliation.PutCacheTopContributorsHandlerFunc(
		func(params affiliation.PutCacheTopContributorsParams) middleware.Responder {
			log.Info("PutCacheTopContributorsHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("PutCacheTopContributorsHandlerFunc: " + info)

			result, err := service.PutCacheTopContributors(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("PutCacheTopContributorsHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("PutCacheTopContributorsHandlerFunc(ok): " + info)

			return affiliation.NewPutCacheTopContributorsOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetAffiliationSingleHandler = affiliation.GetAffiliationSingleHandlerFunc(
		func(params affiliation.GetAffiliationSingleParams) middleware.Responder {
			log.Info("GetAffiliationSingleHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetAffiliationSingleHandlerFunc: " + info)

			if service.IsProjectSkipped(params.ProjectSlug) {
				log.Info("AffiliationGetAffiliationSingleHandler: project " + params.ProjectSlug + " is disabled")
				return affiliation.NewGetAffiliationSingleNotAcceptable().WithPayload(nil)
			}
			result, err := service.GetAffiliationSingle(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetAffiliationSingleHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetAffiliationSingleHandlerFunc(ok): " + info)

			return affiliation.NewGetAffiliationSingleOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetAffiliationMultipleHandler = affiliation.GetAffiliationMultipleHandlerFunc(
		func(params affiliation.GetAffiliationMultipleParams) middleware.Responder {
			log.Info("GetAffiliationMultipleHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetAffiliationMultipleHandlerFunc: " + info)

			if service.IsProjectSkipped(params.ProjectSlug) {
				log.Info("AffiliationGetAffiliationMultipleHandler: project " + params.ProjectSlug + " is disabled")
				return affiliation.NewGetAffiliationMultipleNotAcceptable().WithPayload(nil)
			}
			result, err := service.GetAffiliationMultiple(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetAffiliationMultipleHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetAffiliationMultipleHandlerFunc(ok): " + info)

			return affiliation.NewGetAffiliationMultipleOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
	api.AffiliationGetAffiliationBothHandler = affiliation.GetAffiliationBothHandlerFunc(
		func(params affiliation.GetAffiliationBothParams) middleware.Responder {
			log.Info("GetAffiliationBothHandlerFunc")
			ctx := params.HTTPRequest.Context()

			var nilRequestID *string
			requestID := log.GetRequestID(nilRequestID)
			service.SetServiceRequestID(requestID)

			info := requestInfo(params.HTTPRequest)
			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
			}).Info("GetAffiliationBothHandlerFunc: " + info)

			if service.IsProjectSkipped(params.ProjectSlug) {
				log.Info("AffiliationGetAffiliationBothHandler: project " + params.ProjectSlug + " is disabled")
				return affiliation.NewGetAffiliationBothNotAcceptable().WithPayload(nil)
			}
			result, err := service.GetAffiliationBoth(ctx, &params)
			if err != nil {
				return swagger.ErrorHandler("GetAffiliationBothHandlerFunc(error): "+info, err)
			}

			log.WithFields(logrus.Fields{
				"X-REQUEST-ID": requestID,
				"Payload":      logPayload(result),
			}).Info("GetAffiliationBothHandlerFunc(ok): " + info)

			return affiliation.NewGetAffiliationBothOK().WithXREQUESTID(requestID).WithPayload(result)
		},
	)
}
