module github.com/LF-Engineering/dev-analytics-affiliation

go 1.15

require (
	github.com/LF-Engineering/dev-analytics-libraries v1.1.7
	github.com/asaskevich/govalidator v0.0.0-20210307081110-f21760c49a8d // indirect
	github.com/aws/aws-lambda-go v1.22.0
	github.com/awslabs/aws-lambda-go-api-proxy v0.9.0
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/elastic/go-elasticsearch/v7 v7.10.0
	github.com/go-openapi/errors v0.20.0
	github.com/go-openapi/loads v0.20.2
	github.com/go-openapi/runtime v0.19.27
	github.com/go-openapi/spec v0.20.3
	github.com/go-openapi/strfmt v0.20.1
	github.com/go-openapi/swag v0.19.15
	github.com/go-openapi/validate v0.20.2
	github.com/go-sql-driver/mysql v1.5.0
	github.com/go-swagger/go-swagger v0.27.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.2.0
	github.com/jessevdk/go-flags v1.5.0
	github.com/jmoiron/sqlx v1.3.1
	github.com/joho/godotenv v1.3.0
	github.com/labstack/gommon v0.3.0
	github.com/lib/pq v1.9.0
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/pkg/errors v0.9.1
	github.com/rs/cors v1.7.0
	github.com/sirupsen/logrus v1.7.0
	golang.org/x/net v0.0.0-20210331060903-cb1fcc7394e5
	golang.org/x/text v0.3.5
	gopkg.in/yaml.v2 v2.4.0
)

// replace github.com/LF-Engineering/dev-analytics-libraries => /root/dev/go/src/github.com/LF-Engineering/dev-analytics-libraries
