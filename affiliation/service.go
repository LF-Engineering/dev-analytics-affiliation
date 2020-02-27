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
	"github.com/LF-Engineering/dev-analytics-affiliation/elastic"
	"github.com/LF-Engineering/dev-analytics-affiliation/shdb"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/affiliation"
)

const (
	maxConcurrentRequests = 50
)

type Service interface {
	PutOrgDomain(ctx context.Context, in *affiliation.PutOrgDomainParams) (*models.PutOrgDomainOutput, error)
	PutMergeProfiles(ctx context.Context, in *affiliation.PutMergeProfilesParams) (*models.ProfileDataOutput, error)
	PutMoveProfile(ctx context.Context, in *affiliation.PutMoveProfileParams) (*models.ProfileDataOutput, error)
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
	es        elastic.Service
}

// New is a simple helper function to create a service instance
func New(apiDB apidb.Service, shDB shdb.Service, es elastic.Service) Service {
	return &service{
		apiDB: apiDB,
		shDB:  shDB,
		es:    es,
	}
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

func (s *service) getPemCert(token *jwt.Token, auth0Domain string) (string, error) {
	cert := ""
	resp, err := http.Get(auth0Domain + ".well-known/jwks.json")
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

func (s *service) checkToken(tokenStr string) (username string, err error) {
	if !strings.HasPrefix(tokenStr, "Bearer ") {
		err = fmt.Errorf("Authorization header should start with 'Bearer '")
		return
	}
	auth0Domain := os.Getenv("AUTH0_DOMAIN")
	if !strings.HasPrefix(auth0Domain, "https://") {
		auth0Domain = "https://" + auth0Domain
	}
	if !strings.HasSuffix(auth0Domain, "/") {
		auth0Domain = auth0Domain + "/"
	}
	token, err := jwt.ParseWithClaims(tokenStr[7:], jwt.MapClaims{}, func(t *jwt.Token) (interface{}, error) {
		certStr, err := s.getPemCert(t, auth0Domain)
		if err != nil {
			return nil, err
		}
		cert, err := jwt.ParseRSAPublicKeyFromPEM([]byte(certStr))
		return cert, err
	})
	if err != nil {
		return
	}
	if !token.Valid {
		err = fmt.Errorf("invalid token")
		return
	}
	checkIss := token.Claims.(jwt.MapClaims).VerifyIssuer(auth0Domain, true)
	if !checkIss {
		err = fmt.Errorf("invalid issuer: '%s' != '%s'", token.Claims.(jwt.MapClaims)["iss"], auth0Domain)
		return
	}
	aud := os.Getenv("AUTH0_CLIENT_ID")
	checkAud := token.Claims.(jwt.MapClaims).VerifyAudience(aud, true)
	if !checkAud {
		err = fmt.Errorf("invalid audience: '%s' != '%s'", token.Claims.(jwt.MapClaims)["aud"], aud)
		return
	}
	ucl := os.Getenv("AUTH0_USERNAME_CLAIM")
	iusername, ok := token.Claims.(jwt.MapClaims)[ucl]
	if !ok {
		err = fmt.Errorf("invalid user name claim: '%s', not present in %+v", ucl, token.Claims.(jwt.MapClaims))
		return
	}
	username, ok = iusername.(string)
	if !ok {
		err = fmt.Errorf("invalid user name: '%+v': is not string", iusername)
		return
	}
	return
}

// checkTokenAndPermission - validate JWT token from 'Authorization: Bearer xyz'
// and then check if authorized use can manage affiliations in given project
func (s *service) checkTokenAndPermission(iParams interface{}) (apiName, project, username string, err error) {
	// Extract params depending on API type
	auth := ""
	switch params := iParams.(type) {
	case *affiliation.PutOrgDomainParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutOrgDomain"
	case *affiliation.PutMergeProfilesParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutMergeProfiles"
	case *affiliation.PutMoveProfileParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutMoveProfile"
	default:
		err = errors.Wrap(fmt.Errorf("unknown params type"), "checkTokenAndPermission")
		return
	}
	// Validate JWT token, final outcome is the LFID of current authorized user
	username, err = s.checkToken(auth)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	// Check if that user can manage identities for given project/scope
	allowed, err := s.apiDB.CheckIdentityManagePermission(username, project)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	if !allowed {
		err = errors.Wrap(fmt.Errorf("user '%s' is not allowed to manage identities in '%s'", username, project), apiName)
		return
	}
	return
}

// PutOrgDomain: API params:
// /v1/affiliation/{orgName}/add_domain/{domain}/to_project/{projectSlug}[?overwrite=true][&is_top_domain=true]
// {orgName} - required path parameter:      organization to add domain to, must be URL encoded, for example 'The%20Microsoft%20company'
// {domain} - required path parameter:       domain to be added, for example 'microsoft.com'
// {projectSlug} - required path parameter:  project to modify affiliations (project slug URL encoded, can be prefixed with "/projects/")
// overwrite - optional query parameter:     if overwrite=true is set, all profiles found are force-updated/affiliated to the given organization
//                                           if overwite is not set, API will not change any profiles which already have any affiliation(s)
// is_top_domain - optional query parameter: if you specify is_top_domain=true it will set 'is_top_domain' DB column to true, else it will set false
func (s *service) PutOrgDomain(ctx context.Context, params *affiliation.PutOrgDomainParams) (*models.PutOrgDomainOutput, error) {
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	if err != nil {
		return nil, err
	}
	// Do the actual API call
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
		return nil, errors.Wrap(err, apiName)
	}
	putOrgDomain.User = username
	putOrgDomain.Scope = project
	return putOrgDomain, nil
}

// PutMergeProfiles: API
// ===========================================================================
// Merge two Profiles with fromUUID to toUUID and Merge Enrollments
// Use this function to join fromUUID unique identity into toUUID
// Identities and enrollments related to fromUUID will be assigned
// to toUUID. In addition, fromUUID will be removed (archived in a special table)
// Duplicated enrollments will be also removed from
// the registry while overlapped enrollments will be merged.
// This function also merges two profiles. When a field on toUUID
// profile is empty, it will be updated with the value on the
// profile of fromUUID. If any of the two unique identities was set
// as a bot, the new profile will also be set as a bot.
// When fromUUID and toUUID are equal, the action does not have any effect
// ===========================================================================
// NOTE: UUIDs used here refer to `profiles` and `profiles_archive` tables
// ===========================================================================
// /v1/affiliation/{projectSlug}/merge_profiles/{fromUUID}/{toUUID}:
// {projectSlug} - required path parameter:  project to modify affiliations (project slug URL encoded, can be prefixed with "/projects/")
// {fromUUID} - required path parameter: uuid to merge from, example "00029bc65f7fc5ba3dde20057770d3320ca51486"
// {toUUID} - required path parameter: uuid to merge into, example "00058697877808f6b4a8524ac6dcf39b544a0c87"
func (s *service) PutMergeProfiles(ctx context.Context, params *affiliation.PutMergeProfilesParams) (*models.ProfileDataOutput, error) {
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	if err != nil {
		return nil, err
	}
	// Do the actual API call
	fromUUID := params.FromUUID
	toUUID := params.ToUUID
	err = s.shDB.MergeProfiles(fromUUID, toUUID)
	if err != nil {
		return nil, errors.Wrap(err, apiName)
	}
	profileData, err := s.shDB.GetProfile(toUUID, nil)
	if err != nil {
		return nil, errors.Wrap(err, apiName)
	}
	fmt.Printf("api:%s project:%s user:%s from_uuid:%s to_uuid:%s\n", apiName, project, username, fromUUID, toUUID)
	return profileData, nil
}

// PutMoveProfile: API
// ===========================================================================
// Move Identity to New Profile | Unmerge Identities and Profiles
// This function shifts the identity identified by 'fromUUID' to
// the unique identity 'toUUID'.
// When 'toUUID' is the unique identity that is currently related
// to 'fromUUID', the action does not have any effect.
// In the case of 'fromUUID' and 'toUUID' have equal values and the
// unique identity does not exist, a new unique identity will be
// created and the identity will be moved to it.
// ===========================================================================
// NOTE: UUIDs used here refer to `identities` table
// ===========================================================================
// /v1/affiliation/{projectSlug}/move_profile/{fromUUID}/{toUUID}:
// {projectSlug} - required path parameter:  project to modify affiliations (project slug URL encoded, can be prefixed with "/projects/")
// {fromUUID} - required path parameter: uuid to move from, example "00029bc65f7fc5ba3dde20057770d3320ca51486"
// {toUUID} - required path parameter: uuid to move into, example "00058697877808f6b4a8524ac6dcf39b544a0c87"
func (s *service) PutMoveProfile(ctx context.Context, params *affiliation.PutMoveProfileParams) (*models.ProfileDataOutput, error) {
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	if err != nil {
		return nil, err
	}
	// Do the actual API call
	fromUUID := params.FromUUID
	toUUID := params.ToUUID
	err = s.shDB.MoveProfile(fromUUID, toUUID)
	if err != nil {
		return nil, errors.Wrap(err, apiName)
	}
	profileData, err := s.shDB.GetProfile(toUUID, nil)
	if err != nil {
		return nil, errors.Wrap(err, apiName)
	}
	fmt.Printf("api:%s project:%s user:%s from_uuid:%s to_uuid:%s\n", apiName, project, username, fromUUID, toUUID)
	return profileData, nil
}
