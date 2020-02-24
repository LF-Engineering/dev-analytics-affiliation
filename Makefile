SERVICE = dev-analyics-affiliation
BUILD_TIME=`date -u '+%Y-%m-%d_%I:%M:%S%p'`
COMMIT=`git rev-parse HEAD`
LDFLAGS=-ldflags "-s -w -extldflags '-static' -X main.BuildStamp=$(BUILD_TIME) -X main.GitHash=$(COMMIT)"
GO_BIN_FILES=main.go
GO_FMT=gofmt -s -w

.PHONY: build clean deploy

generate: swagger

swagger: setup_dev clean
	swagger -q generate server -t gen -f swagger/dev-analytics-affiliation.yaml --exclude-main -A dev-analytics-affiliation

build: swagger deps
	go build -tags aws_lambda -o bin/$(SERVICE) -a $(LDFLAGS) .
	chmod +x bin/$(SERVICE)

run: fmt
	go run main.go

clean:
	rm -rf ./bin

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
