// This file is safe to edit. Once it exists it will not be overwritten

package restapi

import (
	"crypto/tls"
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/runtime/yamlpc"
	"github.com/rs/cors"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/affiliation"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/health"
)

//go:generate swagger generate server --target ../../gen --name DevAnalyticsAffiliation --spec ../../swagger/dev-analytics-affiliation.yaml --exclude-main

func configureFlags(api *operations.DevAnalyticsAffiliationAPI) {
	// api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{ ... }
}

func configureAPI(api *operations.DevAnalyticsAffiliationAPI) http.Handler {
	// configure the api here
	api.ServeError = errors.ServeError

	// Set your custom logger if needed. Default one is log.Printf
	// Expected interface func(string, ...interface{})
	//
	// Example:
	// api.Logger = log.Printf

	api.JSONConsumer = runtime.JSONConsumer()
	api.YamlConsumer = yamlpc.YAMLConsumer()

	api.BinProducer = runtime.ByteStreamProducer()
	api.JSONProducer = runtime.JSONProducer()
	api.YamlProducer = yamlpc.YAMLProducer()

	if api.AffiliationDeleteEnrollmentsHandler == nil {
		api.AffiliationDeleteEnrollmentsHandler = affiliation.DeleteEnrollmentsHandlerFunc(func(params affiliation.DeleteEnrollmentsParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.DeleteEnrollments has not yet been implemented")
		})
	}
	if api.AffiliationDeleteIdentityHandler == nil {
		api.AffiliationDeleteIdentityHandler = affiliation.DeleteIdentityHandlerFunc(func(params affiliation.DeleteIdentityParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.DeleteIdentity has not yet been implemented")
		})
	}
	if api.AffiliationDeleteMatchingBlacklistHandler == nil {
		api.AffiliationDeleteMatchingBlacklistHandler = affiliation.DeleteMatchingBlacklistHandlerFunc(func(params affiliation.DeleteMatchingBlacklistParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.DeleteMatchingBlacklist has not yet been implemented")
		})
	}
	if api.AffiliationDeleteOrgDomainHandler == nil {
		api.AffiliationDeleteOrgDomainHandler = affiliation.DeleteOrgDomainHandlerFunc(func(params affiliation.DeleteOrgDomainParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.DeleteOrgDomain has not yet been implemented")
		})
	}
	if api.AffiliationDeleteOrganizationHandler == nil {
		api.AffiliationDeleteOrganizationHandler = affiliation.DeleteOrganizationHandlerFunc(func(params affiliation.DeleteOrganizationParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.DeleteOrganization has not yet been implemented")
		})
	}
	if api.AffiliationDeleteProfileHandler == nil {
		api.AffiliationDeleteProfileHandler = affiliation.DeleteProfileHandlerFunc(func(params affiliation.DeleteProfileParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.DeleteProfile has not yet been implemented")
		})
	}
	if api.AffiliationGetAllAffiliationsHandler == nil {
		api.AffiliationGetAllAffiliationsHandler = affiliation.GetAllAffiliationsHandlerFunc(func(params affiliation.GetAllAffiliationsParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetAllAffiliations has not yet been implemented")
		})
	}
	if api.AffiliationGetFindOrganizationByIDHandler == nil {
		api.AffiliationGetFindOrganizationByIDHandler = affiliation.GetFindOrganizationByIDHandlerFunc(func(params affiliation.GetFindOrganizationByIDParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetFindOrganizationByID has not yet been implemented")
		})
	}
	if api.AffiliationGetFindOrganizationByNameHandler == nil {
		api.AffiliationGetFindOrganizationByNameHandler = affiliation.GetFindOrganizationByNameHandlerFunc(func(params affiliation.GetFindOrganizationByNameParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetFindOrganizationByName has not yet been implemented")
		})
	}
	if api.HealthGetHealthHandler == nil {
		api.HealthGetHealthHandler = health.GetHealthHandlerFunc(func(params health.GetHealthParams) middleware.Responder {
			return middleware.NotImplemented("operation health.GetHealth has not yet been implemented")
		})
	}
	if api.AffiliationGetListOrganizationsHandler == nil {
		api.AffiliationGetListOrganizationsHandler = affiliation.GetListOrganizationsHandlerFunc(func(params affiliation.GetListOrganizationsParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetListOrganizations has not yet been implemented")
		})
	}
	if api.AffiliationGetListOrganizationsDomainsHandler == nil {
		api.AffiliationGetListOrganizationsDomainsHandler = affiliation.GetListOrganizationsDomainsHandlerFunc(func(params affiliation.GetListOrganizationsDomainsParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetListOrganizationsDomains has not yet been implemented")
		})
	}
	if api.AffiliationGetListProfilesHandler == nil {
		api.AffiliationGetListProfilesHandler = affiliation.GetListProfilesHandlerFunc(func(params affiliation.GetListProfilesParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetListProfiles has not yet been implemented")
		})
	}
	if api.AffiliationGetMatchingBlacklistHandler == nil {
		api.AffiliationGetMatchingBlacklistHandler = affiliation.GetMatchingBlacklistHandlerFunc(func(params affiliation.GetMatchingBlacklistParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetMatchingBlacklist has not yet been implemented")
		})
	}
	if api.AffiliationGetProfileHandler == nil {
		api.AffiliationGetProfileHandler = affiliation.GetProfileHandlerFunc(func(params affiliation.GetProfileParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetProfile has not yet been implemented")
		})
	}
	if api.AffiliationGetProfileEnrollmentsHandler == nil {
		api.AffiliationGetProfileEnrollmentsHandler = affiliation.GetProfileEnrollmentsHandlerFunc(func(params affiliation.GetProfileEnrollmentsParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetProfileEnrollments has not yet been implemented")
		})
	}
	if api.AffiliationGetTopContributorsHandler == nil {
		api.AffiliationGetTopContributorsHandler = affiliation.GetTopContributorsHandlerFunc(func(params affiliation.GetTopContributorsParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetTopContributors has not yet been implemented")
		})
	}
	if api.AffiliationGetTopContributorsCSVHandler == nil {
		api.AffiliationGetTopContributorsCSVHandler = affiliation.GetTopContributorsCSVHandlerFunc(func(params affiliation.GetTopContributorsCSVParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetTopContributorsCSV has not yet been implemented")
		})
	}
	if api.AffiliationGetUnaffiliatedHandler == nil {
		api.AffiliationGetUnaffiliatedHandler = affiliation.GetUnaffiliatedHandlerFunc(func(params affiliation.GetUnaffiliatedParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.GetUnaffiliated has not yet been implemented")
		})
	}
	if api.AffiliationPostAddEnrollmentHandler == nil {
		api.AffiliationPostAddEnrollmentHandler = affiliation.PostAddEnrollmentHandlerFunc(func(params affiliation.PostAddEnrollmentParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PostAddEnrollment has not yet been implemented")
		})
	}
	if api.AffiliationPostAddIdentityHandler == nil {
		api.AffiliationPostAddIdentityHandler = affiliation.PostAddIdentityHandlerFunc(func(params affiliation.PostAddIdentityParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PostAddIdentity has not yet been implemented")
		})
	}
	if api.AffiliationPostAddOrganizationHandler == nil {
		api.AffiliationPostAddOrganizationHandler = affiliation.PostAddOrganizationHandlerFunc(func(params affiliation.PostAddOrganizationParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PostAddOrganization has not yet been implemented")
		})
	}
	if api.AffiliationPostAddUniqueIdentityHandler == nil {
		api.AffiliationPostAddUniqueIdentityHandler = affiliation.PostAddUniqueIdentityHandlerFunc(func(params affiliation.PostAddUniqueIdentityParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PostAddUniqueIdentity has not yet been implemented")
		})
	}
	if api.AffiliationPostBulkUpdateHandler == nil {
		api.AffiliationPostBulkUpdateHandler = affiliation.PostBulkUpdateHandlerFunc(func(params affiliation.PostBulkUpdateParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PostBulkUpdate has not yet been implemented")
		})
	}
	if api.AffiliationPostMatchingBlacklistHandler == nil {
		api.AffiliationPostMatchingBlacklistHandler = affiliation.PostMatchingBlacklistHandlerFunc(func(params affiliation.PostMatchingBlacklistParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PostMatchingBlacklist has not yet been implemented")
		})
	}
	if api.AffiliationPostUnarchiveProfileHandler == nil {
		api.AffiliationPostUnarchiveProfileHandler = affiliation.PostUnarchiveProfileHandlerFunc(func(params affiliation.PostUnarchiveProfileParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PostUnarchiveProfile has not yet been implemented")
		})
	}
	if api.AffiliationPutEditEnrollmentHandler == nil {
		api.AffiliationPutEditEnrollmentHandler = affiliation.PutEditEnrollmentHandlerFunc(func(params affiliation.PutEditEnrollmentParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PutEditEnrollment has not yet been implemented")
		})
	}
	if api.AffiliationPutEditOrganizationHandler == nil {
		api.AffiliationPutEditOrganizationHandler = affiliation.PutEditOrganizationHandlerFunc(func(params affiliation.PutEditOrganizationParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PutEditOrganization has not yet been implemented")
		})
	}
	if api.AffiliationPutEditProfileHandler == nil {
		api.AffiliationPutEditProfileHandler = affiliation.PutEditProfileHandlerFunc(func(params affiliation.PutEditProfileParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PutEditProfile has not yet been implemented")
		})
	}
	if api.AffiliationPutMergeEnrollmentsHandler == nil {
		api.AffiliationPutMergeEnrollmentsHandler = affiliation.PutMergeEnrollmentsHandlerFunc(func(params affiliation.PutMergeEnrollmentsParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PutMergeEnrollments has not yet been implemented")
		})
	}
	if api.AffiliationPutMergeUniqueIdentitiesHandler == nil {
		api.AffiliationPutMergeUniqueIdentitiesHandler = affiliation.PutMergeUniqueIdentitiesHandlerFunc(func(params affiliation.PutMergeUniqueIdentitiesParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PutMergeUniqueIdentities has not yet been implemented")
		})
	}
	if api.AffiliationPutMoveIdentityHandler == nil {
		api.AffiliationPutMoveIdentityHandler = affiliation.PutMoveIdentityHandlerFunc(func(params affiliation.PutMoveIdentityParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PutMoveIdentity has not yet been implemented")
		})
	}
	if api.AffiliationPutOrgDomainHandler == nil {
		api.AffiliationPutOrgDomainHandler = affiliation.PutOrgDomainHandlerFunc(func(params affiliation.PutOrgDomainParams) middleware.Responder {
			return middleware.NotImplemented("operation affiliation.PutOrgDomain has not yet been implemented")
		})
	}

	api.PreServerShutdown = func() {}

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// The TLS configuration before HTTPS server starts.
func configureTLS(tlsConfig *tls.Config) {
	// Make all necessary changes to the TLS configuration here.
}

// As soon as server is initialized but not run yet, this function will be called.
// If you need to modify a config, store server instance to stop it individually later, this is the place.
// This function can be called multiple times, depending on the number of serving schemes.
// scheme value will be set accordingly: "http", "https" or "unix"
func configureServer(s *http.Server, scheme, addr string) {
}

// The middleware configuration is for the handler executors. These do not apply to the swagger.json document.
// The middleware executes after routing but before authentication, binding and validation
func setupMiddlewares(handler http.Handler) http.Handler {
	return handler
}

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics
func setupGlobalMiddleware(handler http.Handler) http.Handler {
	//handleCORS := cors.Default().Handler
	//handleCORS := cors.AllowAll().Handler
	handleCORS := cors.New(cors.Options{
		AllowedOrigins: []string{
			"https://test.lfanalytics.io",
			"https://lfanalytics.io",
			//"https://*.lfanalytics.io",
		},
		AllowedMethods: []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodDelete,
			http.MethodPatch,
			http.MethodOptions,
			http.MethodHead,
		},
		AllowedHeaders: []string{
			"Accept",
			"Authorization",
			"Content-Type",
		},
		AllowCredentials: true,
		Debug:            true,
	}).Handler
	return handleCORS(handler)
}
