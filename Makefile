SERVICE = dev-analyics-affiliation
BUILD_TIME=`date -u '+%Y-%m-%d_%I:%M:%S%p'`
BUILD_COMMIT=`git rev-parse HEAD`
BUILD_HOSTNAME=`uname -a | sed "s/ /_/g"`
BUILD_GO_VERSION=`go version | sed "s/ /_/g"`
# Turn off race conditions detector
# LDFLAGS=-ldflags "-s -w -extldflags '-static' -X main.BuildStamp=$(BUILD_TIME) -X main.GitHash=$(BUILD_COMMIT) -X main.BuildHostName=$(BUILD_HOSTNAME) -X main.BuildGoVersion=$(BUILD_GO_VERSION)"
# For detecting race conditions
LDFLAGS=-ldflags "-s -w -extldflags '-static' -X main.BuildStamp=$(BUILD_TIME) -X main.GitHash=$(BUILD_COMMIT) -X main.BuildHostName=$(BUILD_HOSTNAME) -X main.BuildGoVersion=$(BUILD_GO_VERSION)" -race
GO_BIN_FILES=main.go
GO_FMT=gofmt -s -w
GO_VET=go vet
GO_LINT=golint -set_exit_status
GO_TEST=go test
GO_TEST_FILES=main_test.go
GO_STATIC=CGO_ENABLED=0

.PHONY: build clean deploy

generate: swagger

swagger: setup_dev clean
	swagger -q generate server -t gen -f swagger/dev-analytics-affiliation.yaml --exclude-main -A dev-analytics-affiliation

build: swagger deps
	env GOOS=linux GOARCH=amd64 go build -tags aws_lambda -o bin/$(SERVICE) -a $(LDFLAGS) .
	chmod +x bin/$(SERVICE)

docker: fmt
	${GO_STATIC} go build -o ./main -a $(LDFLAGS)

run: fmt vet lint
	go build -o ./main -a $(LDFLAGS)
	./main

fastrun: fmt
	go build -o ./main -a $(LDFLAGS)
	./main

clean:
	cp gen/restapi/configure_dev_analytics_affiliation.go .
	rm -rf ./bin ./gen
	mkdir gen
	mkdir gen/restapi
	mv configure_dev_analytics_affiliation.go gen/restapi

setup: setup_dev setup_deploy

setup_dev:
	go get github.com/go-swagger/go-swagger/cmd/swagger

setup_deploy:
	npm install serverless

deps:
	go mod tidy

deploy: clean build
	npm install serverless-domain-manager --save-dev
	sls -s ${STAGE} -r ${REGION} create_domain
	sls deploy -s ${STAGE} -r ${REGION} --verbose

fmt: ${GO_BIN_FILES}
	./for_each_go_file.sh "${GO_FMT}"

vet: ${GO_BIN_FILES}
	ERROR_EXIT_CODE=0 ./for_each_go_file.sh "${GO_VET}"

lint: ${GO_BIN_FILES}
	./for_each_go_file.sh "${GO_LINT}"

test:
	${GO_TEST} ${GO_TEST_FILES}
