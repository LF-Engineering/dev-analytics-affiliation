package affiliation

import (
	"context"
	"fmt"
	"os"
	"strings"

	"encoding/json"
	"net/http"

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

type Jwks struct {
	Keys []JSONWebKeys `json:"keys"`
}

type JSONWebKeys struct {
	Alg string   `json:"alg"`
	Kty string   `json:"kty"`
	Kid string   `json:"kid"`
	Use string   `json:"use"`
	N   string   `json:"n"`
	E   string   `json:"e"`
	X5t string   `json:"e"`
	X5c []string `json:"x5c"`
}

func (s *service) getPemCert(token *jwt.Token) (string, error) {
	cert := ""
	resp, err := http.Get("https://" + os.Getenv("AUTH0_DOMAIN") + "/.well-known/jwks.json")
	if err != nil {
		return cert, err
	}
	defer resp.Body.Close()
	var jwks = Jwks{}
	err = json.NewDecoder(resp.Body).Decode(&jwks)
	if err != nil {
		return cert, err
	}
	for k := range jwks.Keys {
		if token.Header["kid"] == jwks.Keys[k].Kid {
			cert = "-----BEGIN CERTIFICATE-----\n" + jwks.Keys[k].X5c[0] + "\n-----END CERTIFICATE-----"
		}
	}
	if cert == "" {
		err := errors.New("Unable to find appropriate key.")
		return cert, err
	}
	return cert, nil
}

func (s *service) checkToken(tokenStr string) (err error) {
	if !strings.HasPrefix(tokenStr, "Bearer ") {
		err = fmt.Errorf("Authorization header should start with 'Bearer '")
		return
	}
	claims := &Claims{}
	tkn, err := jwt.ParseWithClaims(tokenStr[7:], claims, func(t *jwt.Token) (interface{}, error) {
		certStr, err := s.getPemCert(t)
		if err != nil {
			return nil, err
		}
		cert, err := jwt.ParseRSAPublicKeyFromPEM([]byte(certStr))
		return cert, err
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
