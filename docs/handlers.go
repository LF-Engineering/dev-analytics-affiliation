package docs

import (
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations"
	d "github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/docs"
	"github.com/go-openapi/runtime/middleware"
)

// Configure configures the documentation service
func Configure(api *operations.DevAnalyticsAffiliationAPI) {

	api.DocsGetDocHandler = d.GetDocHandlerFunc(func(params d.GetDocParams) middleware.Responder {
		return NewGetDocOK()
	})
}
