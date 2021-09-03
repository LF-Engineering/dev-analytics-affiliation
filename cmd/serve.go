//go:build !aws_lambda
// +build !aws_lambda

package cmd

import (
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations"
)

// Start function starts local services
func Start(api *operations.DevAnalyticsAffiliationAPI, portFlag int) error {
	server := restapi.NewServer(api)
	defer server.Shutdown() // nolint
	server.Port = portFlag
	server.ConfigureAPI()

	return server.Serve()
}
