package affiliation

import (
	"context"
	"fmt"
	"strings"

	"encoding/base64"

	"github.com/dgrijalva/jwt-go"
	"github.com/pkg/errors"

	"github.com/LF-Engineering/dev-analytics-affiliation/apidb"
	"github.com/LF-Engineering/dev-analytics-affiliation/shdb"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/affiliation"
)

const (
	maxConcurrentRequests = 50
)

type Service interface {
	PutOrgDomain(ctx context.Context, in *affiliation.PutOrgDomainParams) (*models.PutOrgDomainOutput, error)
	SetServiceRequestID(requestID string)
	GetServiceRequestID() string
}

func (s *service) SetServiceRequestID(requestID string) {
	s.requestID = requestID
}

func (s *service) GetServiceRequestID() string {
	return s.requestID
}

type service struct {
	requestID string
	apiDB     apidb.Service
	shDB      shdb.Service
}

// New is a simple helper function to create a service instance
func New(apiDB apidb.Service, shDB shdb.Service) Service {
	return &service{
		apiDB: apiDB,
		shDB:  shDB,
	}
}

type Claims struct {
	jwt.StandardClaims
}

func (s *service) checkToken(tokenStr string) (err error) {
	if !strings.HasPrefix(tokenStr, "Bearer ") {
		err = fmt.Errorf("Authorization header should start with 'Bearer '")
		return
	}
	//claims := &Claims{}
	//t, parts, err := jwt.ParseUnverified(tokenStr[8:], claims)
	//fmt.Printf("t: %+v\n", t)
	//fmt.Printf("parts: %+v\n", parts)
	//fmt.Printf("err: %+v\n", err)
	//tkn, err := jwt.ParseWithClaims(tokenStr[7:], claims, func(t *jwt.Token) (interface{}, error) {
	tkn, err := jwt.Parse(tokenStr[7:], func(t *jwt.Token) (interface{}, error) {
		fmt.Printf("%+v\n", t)
		//kid := t.Header["kid"].(string)
		//verifyKey, err = jwt.ParseRSAPublicKeyFromPEM(verifyBytes)
		//return *rsa.PublicKey(kid), nil
		//kid := ""
		b64, err := base64.StdEncoding.DecodeString("aaa")
		if err != nil {
			return nil, err
		}
		verifyKey, err := jwt.ParseRSAPublicKeyFromPEM(b64)
		if err != nil {
			return nil, err
		}
		return verifyKey, nil
	})
	if err != nil {
		return
	}
	if !tkn.Valid {
		err = fmt.Errorf("invalid token")
		return
	}
	return
}

// PutOrgDomain: API params:
// /v1/affiliation/{orgName}/add_domain/{domain}[?overwrite=true][&is_top_domain=true]
// {orgName} - required path parameter:      organization to add domain to, must be URL encoded, for example 'The%20Microsoft%20company'
// {domain} - required path parameter:       domain to be added, for example 'microsoft.com'
// overwrite - optional query parameter:     if overwrite=true is set, all profiles found are force-updated/affiliated to the given organization
//                                           if overwite is not set, API will not change any profiles which already have any affiliation(s)
// is_top_domain - optional query parameter: if you specify is_top_domain=true it will set 'is_top_domain' DB column to true, else it will set false
func (s *service) PutOrgDomain(ctx context.Context, params *affiliation.PutOrgDomainParams) (*models.PutOrgDomainOutput, error) {
	err := s.checkToken(params.Authorization)
	if err != nil {
		return nil, errors.Wrap(err, "PutOrgDomain")
	}
	org := params.OrgName
	dom := params.Domain
	overwrite := false
	isTopDomain := false
	if params.Overwrite != nil {
		overwrite = *params.Overwrite
	}
	if params.IsTopDomain != nil {
		isTopDomain = *params.IsTopDomain
	}
	putOrgDomain, err := s.shDB.PutOrgDomain(org, dom, overwrite, isTopDomain)
	if err != nil {
		return nil, errors.Wrap(err, "PutOrgDomain")
	}
	return putOrgDomain, nil
}
