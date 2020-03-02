// +build aws_lambda

package cmd

import (
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"
	"github.com/sirupsen/logrus"
)

// Start - AWS lambda entry
func Start(api *operations.DevAnalyticsAffiliationAPI, _ int) error {
	server := restapi.NewServer(api)
	server.ConfigureAPI()
	adapter := httpadapter.New(server.GetHandler())

	logrus.Debug("Starting Lambda")
	lambda.Start(adapter.Proxy)
	return nil
}
