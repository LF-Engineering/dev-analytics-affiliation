package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/LF-Engineering/dev-analytics-affiliation/affiliation"
	"github.com/LF-Engineering/dev-analytics-affiliation/apidb"
	"github.com/LF-Engineering/dev-analytics-affiliation/cmd"
	"github.com/LF-Engineering/dev-analytics-affiliation/docs"
	"github.com/LF-Engineering/dev-analytics-affiliation/elastic"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations"
	"github.com/LF-Engineering/dev-analytics-affiliation/health"
	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
	"github.com/LF-Engineering/dev-analytics-affiliation/platform"
	"github.com/LF-Engineering/dev-analytics-affiliation/shared"
	"github.com/LF-Engineering/dev-analytics-affiliation/shdb"
	"github.com/LF-Engineering/dev-analytics-affiliation/usersvc"
	orgservice "github.com/LF-Engineering/dev-analytics-libraries/orgs"
	userservice "github.com/LF-Engineering/dev-analytics-libraries/users"

	"github.com/LF-Engineering/dev-analytics-libraries/slack"
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
	shared.GRedacted[dbURL] = struct{}{}
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

func initSHDB(origin string) *sqlx.DB {
	dbURL := os.Getenv("SH_DB_ENDPOINT")
	shared.GRedacted[dbURL] = struct{}{}
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
	d.SetConnMaxLifetime(30 * time.Second)
	s := &shared.ServiceStruct{}
	_, err = s.ExecDB(d, "set @origin = ?", origin)
	if err != nil {
		log.Panicf("unable to set connection session origin: %v", err)
	}
	log.Println(fmt.Sprintf("%+v", d))
	log.Println("Initialized", "Affiliation DB", origin, host)
	return d
}

func initSHDBRO() *sqlx.DB {
	dbURL := os.Getenv("SH_DB_RO_ENDPOINT")
	if dbURL == "" {
		dbURL = os.Getenv("SH_DB_ENDPOINT")
	}
	shared.GRedacted[dbURL] = struct{}{}
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
	d.SetConnMaxLifetime(30 * time.Second)
	log.Println(fmt.Sprintf("%+v", d))
	log.Println("Initialized", "Affiliation RO DB", host)
	return d
}

func initES() (*elasticsearch.Client, string) {
	esURL := os.Getenv("ELASTIC_URL")
	esUsername := os.Getenv("ELASTIC_USERNAME")
	esPassword := os.Getenv("ELASTIC_PASSWORD")
	shared.GRedacted[esURL] = struct{}{}
	shared.GRedacted[esUsername] = struct{}{}
	shared.GRedacted[esPassword] = struct{}{}
	config := elasticsearch.Config{
		Addresses: []string{esURL},
		Username:  esUsername,
		Password:  esPassword,
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
	return client, esURL
}

func initOrg() *orgservice.Org {
	slackProvider := slack.New(os.Getenv("SLACK_WEBHOOK_URL"))

	orgClient, err := orgservice.NewClient(
		os.Getenv("PLATFORM_ORG_SERVICE_ENDPOINT"),
		os.Getenv("ELASTIC_CACHE_URL"),
		os.Getenv("ELASTIC_CACHE_USERNAME"),
		os.Getenv("ELASTIC_CACHE_PASSWORD"),
		os.Getenv("STAGE"),
		os.Getenv("AUTH0_GRANT_TYPE"),
		os.Getenv("AUTH0_CLIENT_ID"),
		os.Getenv("AUTH0_CLIENT_SECRET"),
		os.Getenv("AUTH0_AUDIENCE"),
		os.Getenv("AUTH0_TOKEN_ENDPOINT"),
		&slackProvider,
	)

	if err != nil {
		log.Panicf("unable to get org client info: %v", err)
	}

	log.Println("Initialized", "Org Service", host)
	return orgClient
}

func initUser() *userservice.Client {
	slackProvider := slack.New(os.Getenv("SLACK_WEBHOOK_URL"))

	usrClient, err := userservice.NewClient(
		os.Getenv("PLATFORM_USER_SERVICE_ENDPOINT"),
		os.Getenv("ELASTIC_CACHE_URL"),
		os.Getenv("ELASTIC_CACHE_USERNAME"),
		os.Getenv("ELASTIC_CACHE_PASSWORD"),
		os.Getenv("STAGE"),
		os.Getenv("AUTH0_GRANT_TYPE"),
		os.Getenv("AUTH0_CLIENT_ID"),
		os.Getenv("AUTH0_CLIENT_SECRET"),
		os.Getenv("AUTH0_AUDIENCE"),
		os.Getenv("AUTH0_TOKEN_ENDPOINT"),
		&slackProvider,
	)

	if err != nil {
		log.Panicf("unable to get user client info: %v", err)
	}

	log.Println("Initialized", "User Service", host)
	return usrClient
}

func setupEnv() {
	shared.GSQLOut = os.Getenv("DA_AFF_API_SQL_OUT") != ""
	shared.GSyncURL = os.Getenv("SYNC_URL")
	shared.GRedacted[shared.GSyncURL] = struct{}{}
	if shared.GSyncURL == "" {
		log.Fatal("setupEnv:", fmt.Errorf("SYNC_URL environment variable must be set"))
	}
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

	setupEnv()
	api := operations.NewDevAnalyticsAffiliationAPI(swaggerSpec)

	healthService := health.New()
	apiDBService := apidb.New(initAPIDB())
	daOrigin := "da-affiliation-api"
	gitdmOrigin := "gitdm"
	shDBRO := initSHDBRO()
	shDBServiceAPI := shdb.New(initSHDB(daOrigin), shDBRO, daOrigin)
	shDBServiceGitdm := shdb.New(initSHDB(gitdmOrigin), shDBRO, gitdmOrigin)
	esService := elastic.New(initES())
	organizationServiceAPI := platform.New(initOrg())
	userServiceAPI := usersvc.New(initUser())
	affiliationService := affiliation.New(apiDBService, shDBServiceAPI, shDBServiceGitdm, esService, organizationServiceAPI, userServiceAPI)

	health.Configure(api, healthService)
	affiliation.Configure(api, affiliationService)
	docs.Configure(api)

	// When redeploying this needs to be cleared
	affiliationService.ClearPrecacheRunning()

	if err := cmd.Start(api, *portFlag); err != nil {
		logrus.Panicln(err)
	}
}
