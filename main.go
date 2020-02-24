package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"database/sql"
	"github.com/LF-Engineering/dev-analytics-affiliation/affiliation"
	"github.com/LF-Engineering/dev-analytics-affiliation/apidb"
	"github.com/LF-Engineering/dev-analytics-affiliation/health"
	"github.com/LF-Engineering/dev-analytics-affiliation/shdb"
	"github.com/jmoiron/sqlx"

	"github.com/LF-Engineering/dev-analytics-affiliation/cmd"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
	"github.com/go-openapi/loads"

	_ "github.com/joho/godotenv/autoload"
	"github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var (
	// BuildStamp is a timestamp (injected by go) of the build time
	BuildStamp = "None"
	// GitHash is the tag for current hash the build represents
	GitHash = "None"
	host    = "None"
)

var err error

func initAPIDB() *sqlx.DB {
	dbURL := os.Getenv("API_DB_ENDPOINT")
	d, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		log.Panicf("unable to connect to database. Error: %v", err)
	}
	d.SetMaxOpenConns(20)
	d.SetMaxIdleConns(5)
	d.SetConnMaxLifetime(15 * time.Minute)
	log.Println("Initialized ", host)
	return d
}

// initSHDB - get MariaDB SH (Sorting Hat) database DSN
// Either provide full DSN via SH_DSN='shuser:shpassword@tcp(shhost:shport)/shdb?charset=utf8&parseTime=true'
// Or use some SH_ variables, only SH_PASS is required
// Defaults are: "shuser:required_pwd@tcp(localhost:3306)/shdb?charset=utf8"
// SH_DSN has higher priority; if set no SH_ varaibles are used
func initSHDB() *sql.DB {
	prefix := "SH_"
	//dsn := "shuser:"+os.Getenv("PASS")+"@/shdb?charset=utf8")
	dsn := os.Getenv(prefix + "DSN")
	if dsn == "" {
		pass := os.Getenv(prefix + "PASS")
		user := os.Getenv(prefix + "USR")
		if user == "" {
			user = os.Getenv(prefix + "USER")
		}
		proto := os.Getenv(prefix + "PROTO")
		if proto == "" {
			proto = "tcp"
		}
		host := os.Getenv(prefix + "HOST")
		if host == "" {
			host = "localhost"
		}
		port := os.Getenv(prefix + "PORT")
		if port == "" {
			port = "3306"
		}
		db := os.Getenv(prefix + "DB")
		if db == "" {
			log.Panicf("please specify database via %sDB=...", prefix)
		}
		params := os.Getenv(prefix + "PARAMS")
		if params == "" {
			params = "?charset=utf8&parseTime=true"
		}
		if params == "-" {
			params = ""
		}
		dsn = fmt.Sprintf(
			"%s:%s@%s(%s:%s)/%s%s",
			user,
			pass,
			proto,
			host,
			port,
			db,
			params,
		)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Panicf("unable to connect to database. Error: %v", err)
	}
	log.Println("Initialized ", host)
	return db
}

func main() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.DebugLevel)

	host, err = os.Hostname()
	if err != nil {
		log.Fatal("unable to get Hostname", err)
	}
	log.WithFields(logrus.Fields{
		"BuildTime": BuildStamp,
		"GitHash":   GitHash,
		"Host":      host,
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
	affiliationService := affiliation.New(apiDBService, shDBService)

	health.Configure(api, healthService)
	affiliation.Configure(api, affiliationService)

	if err := cmd.Start(api, *portFlag); err != nil {
		logrus.Panicln(err)
	}
}
