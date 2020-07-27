package main

import (
	"database/sql"
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
	"github.com/LF-Engineering/dev-analytics-affiliation/elastic"
	"github.com/LF-Engineering/dev-analytics-affiliation/health"
	"github.com/LF-Engineering/dev-analytics-affiliation/shared"
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

func initSHDB(origin string) *sqlx.DB {
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
	d.SetConnMaxLifetime(30 * time.Second)
	d.SetOnConn(
		&sql.OnConnParams{
			Enabled: [3]bool{true, true, true},
			SQLs:    []string{"set @origin = ?"},
			Args:    [][]interface{}{{origin}},
		},
	)
	s := &shared.ServiceStruct{}
	_, err = s.ExecDB(d, "set @origin = ?", origin)
	if err != nil {
		log.Panicf("unable to set connection session origin: %v", err)
	}
	log.Println(fmt.Sprintf("%+v", d))
	log.Println("Initialized", "Affiliation DB", origin, host)
	return d
}

func initES() (*elasticsearch.Client, string) {
	esURL := os.Getenv("ELASTIC_URL")
	config := elasticsearch.Config{
		Addresses: []string{esURL},
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
	return client, esURL
}

func setupEnv() {
	shared.GSQLOut = os.Getenv("DA_AFF_API_SQL_OUT") != ""
	shared.GSyncURL = os.Getenv("SYNC_URL")
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
	shDBServiceAPI := shdb.New(initSHDB(daOrigin), daOrigin)
	shDBServiceGitdm := shdb.New(initSHDB(gitdmOrigin), gitdmOrigin)
	esService := elastic.New(initES())
	affiliationService := affiliation.New(apiDBService, shDBServiceAPI, shDBServiceGitdm, esService)

	health.Configure(api, healthService)
	affiliation.Configure(api, affiliationService)

	if err := cmd.Start(api, *portFlag); err != nil {
		logrus.Panicln(err)
	}
}
