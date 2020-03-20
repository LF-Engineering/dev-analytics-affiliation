package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/LF-Engineering/dev-analytics-affiliation/affiliation"
	"github.com/LF-Engineering/dev-analytics-affiliation/apidb"
	"github.com/LF-Engineering/dev-analytics-affiliation/elastic"
	"github.com/LF-Engineering/dev-analytics-affiliation/health"
	"github.com/LF-Engineering/dev-analytics-affiliation/shdb"

	"github.com/LF-Engineering/dev-analytics-affiliation/cmd"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
	"github.com/go-openapi/loads"

	_ "github.com/joho/godotenv/autoload"
)

var (
	// BuildStamp is a timestamp (injected by go) of the build time
	BuildStamp = "None"
	// GitHash is the tag for current hash the build represents
	GitHash = "None"
	// BuildHostName uname -a
	BuildHostName = "None"
	// BuildGoVersion go version
	BuildGoVersion = "None"
	host           = "None"
)

var err error

func initAPIDB() *sqlx.DB {
	dbURL := os.Getenv("API_DB_ENDPOINT")
	d, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		log.Panicf("unable to connect to API database: %v", err)
	}
	//d.SetMaxOpenConns(20)
	//d.SetMaxIdleConns(5)
	//d.SetConnMaxLifetime(15 * time.Minute)
	log.Println(fmt.Sprintf("%+v", d))
	log.Println("Initialized", "API DB", host)
	return d
}

func initSHDB() *sqlx.DB {
	dbURL := os.Getenv("SH_DB_ENDPOINT")
	if !strings.Contains(dbURL, "parseTime=true") {
		if strings.Contains(dbURL, "?") {
			dbURL += "&parseTime=true"
		} else {
			dbURL += "?parseTime=true"
		}
	}
	d, err := sqlx.Connect("mysql", dbURL)
	if err != nil {
		log.Panicf("unable to connect to affiliation database: %v", err)
	}
	//d.SetMaxOpenConns(20)
	//d.SetMaxIdleConns(5)
	//d.SetConnMaxLifetime(15 * time.Minute)
	log.Println(fmt.Sprintf("%+v", d))
	log.Println("Initialized", "Affiliation DB", host)
	return d
}

func initES() *elasticsearch.Client {
	config := elasticsearch.Config{
		Addresses: []string{os.Getenv("ELASTIC_URL")},
		Username:  os.Getenv("ELASTIC_USERNAME"),
		Password:  os.Getenv("ELASTIC_PASSWORD"),
	}
	client, err := elasticsearch.NewClient(config)
	if err != nil {
		log.Panicf("unable to connect to ElasticSearch: %v", err)
	}
	info, err := client.Info()
	if err != nil {
		log.Panicf("unable to get elasticsearch client info: %v", err)
	}
	log.Println(fmt.Sprintf("%+v", info))
	log.Println("Initialized", "ElasticSearch", host)
	return client
}

func main() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.DebugLevel)

	host, err = os.Hostname()
	if err != nil {
		log.Fatal("unable to get Hostname", err)
	}
	log.WithFields(logrus.Fields{
		"BuildTime":      BuildStamp,
		"GitHash":        GitHash,
		"BuildHost":      BuildHostName,
		"BuildGoVersion": BuildGoVersion,
		"RunningHost":    host,
	}).Info("Service Startup")

	var portFlag = flag.Int("port", 8080, "Port to listen for web requests on")

	swaggerSpec, err := loads.Analyzed(restapi.SwaggerJSON, "")
	if err != nil {
		logrus.Panicln("Invalid swagger file for initializing", err)
	}

	api := operations.NewDevAnalyticsAffiliationAPI(swaggerSpec)

	healthService := health.New()
	apiDBService := apidb.New(initAPIDB())
	shDBService := shdb.New(initSHDB())
	esService := elastic.New(initES())
	affiliationService := affiliation.New(apiDBService, shDBService, esService)

	health.Configure(api, healthService)
	affiliation.Configure(api, affiliationService)

	if err := cmd.Start(api, *portFlag); err != nil {
		logrus.Panicln(err)
	}
}
