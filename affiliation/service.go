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
	"github.com/LF-Engineering/dev-analytics-affiliation/shared"
	"github.com/LF-Engineering/dev-analytics-affiliation/shdb"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/affiliation"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
)

const (
	maxConcurrentRequests = 50
)

// Service - API interface
type Service interface {
	shared.ServiceInterface
	// External methods
	GetMatchingBlacklist(ctx context.Context, in *affiliation.GetMatchingBlacklistParams) (*models.GetMatchingBlacklistOutput, error)
	PostMatchingBlacklist(ctx context.Context, in *affiliation.PostMatchingBlacklistParams) (*models.MatchingBlacklistOutput, error)
	DeleteMatchingBlacklist(ctx context.Context, in *affiliation.DeleteMatchingBlacklistParams) (*models.TextStatusOutput, error)
	GetListOrganizations(ctx context.Context, in *affiliation.GetListOrganizationsParams) (*models.GetListOrganizationsOutput, error)
	GetFindOrganizationByID(ctx context.Context, in *affiliation.GetFindOrganizationByIDParams) (*models.OrganizationDataOutput, error)
	GetFindOrganizationByName(ctx context.Context, in *affiliation.GetFindOrganizationByNameParams) (*models.OrganizationDataOutput, error)
	PostAddOrganization(ctx context.Context, in *affiliation.PostAddOrganizationParams) (*models.OrganizationDataOutput, error)
	PutOrgDomain(ctx context.Context, in *affiliation.PutOrgDomainParams) (*models.PutOrgDomainOutput, error)
	PutMergeUniqueIdentities(ctx context.Context, in *affiliation.PutMergeUniqueIdentitiesParams) (*models.ProfileDataOutput, error)
	PutMoveIdentity(ctx context.Context, in *affiliation.PutMoveIdentityParams) (*models.ProfileDataOutput, error)
	GetUnaffiliated(ctx context.Context, in *affiliation.GetUnaffiliatedParams) (*models.GetUnaffiliatedOutput, error)
	GetTopContributors(ctx context.Context, in *affiliation.GetTopContributorsParams) (*models.GetTopContributorsOutput, error)
	SetServiceRequestID(requestID string)
	GetServiceRequestID() string

	// Internal methods
	getPemCert(*jwt.Token, string) (string, error)
	checkToken(string) (string, error)
	checkTokenAndPermission(interface{}) (string, string, string, error)
}

func (s *service) SetServiceRequestID(requestID string) {
	s.requestID = requestID
}

func (s *service) GetServiceRequestID() string {
	return s.requestID
}

type service struct {
	shared.ServiceStruct
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

// Jwks - keys to get certificate data
type Jwks struct {
	Keys []JSONWebKeys `json:"keys"`
}

// JSONWebKeys keys to get certificate data
type JSONWebKeys struct {
	Alg string `json:"alg"`
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	//X5t string   `json:"e"`
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
	case *affiliation.GetMatchingBlacklistParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetMatchingBlacklist"
	case *affiliation.PostMatchingBlacklistParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PostMatchingBlacklist"
	case *affiliation.DeleteMatchingBlacklistParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "DeleteMatchingBlacklist"
	case *affiliation.GetListOrganizationsParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetListOrganizations"
	case *affiliation.GetFindOrganizationByIDParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetFindOrganizationByID"
	case *affiliation.GetFindOrganizationByNameParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetFindOrganizationByName"
	case *affiliation.PostAddOrganizationParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PostAddOrganization"
	case *affiliation.PutOrgDomainParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutOrgDomain"
	case *affiliation.PutMergeUniqueIdentitiesParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutMergeUniqueIdentities"
	case *affiliation.PutMoveIdentityParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutMoveIdentity"
	case *affiliation.GetUnaffiliatedParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetUnaffiliated"
	case *affiliation.GetTopContributorsParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetTopContributors"
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
	allowed, err := s.apiDB.CheckIdentityManagePermission(username, project, nil)
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

// GetListOrganizations: API params:
// /v1/affiliation/{projectSlug}/list_organizations[?q=xyz][&rows=100][&page=2]
// {projectSlug} - required path parameter: project to get organizations (project slug URL encoded, can be prefixed with "/projects/")
// q - optional query parameter: if you specify that parameter only organizations where name like '%q%' will be returned
// rows - optional query parameter: rows per page, if 0 no paging is used and page parameter is ignored, default 10
// page - optional query parameter: if set, it will return rows from a given page, default 1
func (s *service) GetListOrganizations(ctx context.Context, params *affiliation.GetListOrganizationsParams) (getListOrganizations *models.GetListOrganizationsOutput, err error) {
	q := ""
	if params.Q != nil {
		q = *params.Q
	}
	rows := int64(10)
	if params.Rows != nil {
		rows = *params.Rows
		if rows < 0 {
			rows = 0
		}
	}
	page := int64(1)
	if params.Page != nil {
		page = *params.Page
		if page < 1 {
			page = 1
		}
	}
	getListOrganizations = &models.GetListOrganizationsOutput{}
	log.Info(fmt.Sprintf("GetListOrganizations: q:%s rows:%d page:%d", q, rows, page))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		list := ""
		nOrgs := len(getListOrganizations.Organizations)
		if nOrgs > shared.LogListMax {
			list = fmt.Sprintf("%d", nOrgs)
		} else {
			list = fmt.Sprintf("%+v", s.ToLocalNestedOrganizations(getListOrganizations.Organizations))
		}
		log.Info(
			fmt.Sprintf(
				"GetListOrganizations(exit): q:%s rows:%d page:%d apiName:%s project:%s username:%s getListOrganizations:%s err:%v",
				q,
				rows,
				page,
				apiName,
				project,
				username,
				list,
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	getListOrganizations, err = s.shDB.GetListOrganizations(q, rows, page)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	getListOrganizations.User = username
	getListOrganizations.Scope = project
	return
}

// PostAddOrganization: API params:
// /v1/affiliation/{projectSlug}/add_organization/{orgName}
// {projectSlug} - required path parameter: project to add organization to (project slug URL encoded, can be prefixed with "/projects/")
// {orgName} - required path parameter: project to add organization to (project slug URL encoded, can be prefixed with "/projects/")
func (s *service) PostAddOrganization(ctx context.Context, params *affiliation.PostAddOrganizationParams) (organization *models.OrganizationDataOutput, err error) {
	organization = &models.OrganizationDataOutput{}
	orgName := params.OrgName
	log.Info(fmt.Sprintf("PostAddOrganization: orgName:%s", orgName))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PostAddOrganization(exit): orgName:%s apiName:%s project:%s username:%s organization:%+v err:%v",
				orgName,
				apiName,
				project,
				username,
				s.ToLocalOrganization(organization),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	organization, err = s.shDB.AddOrganization(
		&models.OrganizationDataOutput{
			Name: orgName,
		},
		true,
		nil,
	)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// GetFindOrganizationByID: API params:
// /v1/affiliation/{projectSlug}/find_organization_by_id/{orgID}
// {projectSlug} - required path parameter: project to get organization (project slug URL encoded, can be prefixed with "/projects/")
// {orgID} - required path parameter: organization ID to lookup for
func (s *service) GetFindOrganizationByID(ctx context.Context, params *affiliation.GetFindOrganizationByIDParams) (organization *models.OrganizationDataOutput, err error) {
	orgID := params.OrgID
	organization = &models.OrganizationDataOutput{}
	log.Info(fmt.Sprintf("GetFindOrganizationByID: orgID:%d", orgID))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetFindOrganizationByID(exit): orgID:%d apiName:%s project:%s username:%s getFindOrganizationByID:%s err:%v",
				orgID,
				apiName,
				project,
				username,
				s.ToLocalOrganization(organization),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	organization, err = s.shDB.GetOrganization(orgID, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// GetFindOrganizationByName: API params:
// /v1/affiliation/{projectSlug}/find_organization_by_name/{orgName}
// {projectSlug} - required path parameter: project to get organization (project slug URL encoded, can be prefixed with "/projects/")
// {orgname} - required path parameter: organization name to lookup for
func (s *service) GetFindOrganizationByName(ctx context.Context, params *affiliation.GetFindOrganizationByNameParams) (organization *models.OrganizationDataOutput, err error) {
	orgName := params.OrgName
	organization = &models.OrganizationDataOutput{}
	log.Info(fmt.Sprintf("GetFindOrganizationByName: orgName:%s", orgName))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetFindOrganizationByName(exit): orgName:%s apiName:%s project:%s username:%s getFindOrganizationByName:%s err:%v",
				orgName,
				apiName,
				project,
				username,
				s.ToLocalOrganization(organization),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	organization, err = s.shDB.GetOrganizationByName(orgName, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// GetMatchingBlacklist: API params:
// /v1/affiliation/{projectSlug}/matching_blacklist[?q=xyz][&rows=100][&page=2]
// {projectSlug} - required path parameter: project to get affiliations emails blacklist (project slug URL encoded, can be prefixed with "/projects/")
// q - optional query parameter: if you specify that parameter only emails like '%q%' will be returned
// rows - optional query parameter: rows per page, if 0 no paging is used and page parameter is ignored, default 10
// page - optional query parameter: if set, it will return rows from a given page, default 1
func (s *service) GetMatchingBlacklist(ctx context.Context, params *affiliation.GetMatchingBlacklistParams) (getMatchingBlacklist *models.GetMatchingBlacklistOutput, err error) {
	q := ""
	if params.Q != nil {
		q = *params.Q
	}
	rows := int64(10)
	if params.Rows != nil {
		rows = *params.Rows
		if rows < 0 {
			rows = 0
		}
	}
	page := int64(1)
	if params.Page != nil {
		page = *params.Page
		if page < 1 {
			page = 1
		}
	}
	log.Info(fmt.Sprintf("GetMatchingBlacklist: q:%s rows:%d page:%d", q, rows, page))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		list := ""
		nEmails := len(getMatchingBlacklist.Emails)
		if nEmails > shared.LogListMax {
			list = fmt.Sprintf("%d", nEmails)
		} else {
			list = fmt.Sprintf("%+v", s.ToLocalMatchingBlacklist(getMatchingBlacklist.Emails))
		}
		log.Info(
			fmt.Sprintf(
				"GetMatchingBlacklist(exit): q:%s rows:%d page:%d apiName:%s project:%s username:%s getMatchingBlacklist:%s err:%v",
				q,
				rows,
				page,
				apiName,
				project,
				username,
				list,
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	getMatchingBlacklist, err = s.shDB.GetMatchingBlacklist(q, rows, page)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	getMatchingBlacklist.User = username
	getMatchingBlacklist.Scope = project
	return
}

// PostMatchingBlacklist: API params:
// /v1/affiliation/{projectSlug}/matching_blacklist/{email}
// {projectSlug} - required path parameter: project to modify affiliations emails blacklist (project slug URL encoded, can be prefixed with "/projects/")
// {email} - required path parameter: email to be added to blacklisted emails
func (s *service) PostMatchingBlacklist(ctx context.Context, params *affiliation.PostMatchingBlacklistParams) (postMatchingBlacklist *models.MatchingBlacklistOutput, err error) {
	email := params.Email
	log.Info(fmt.Sprintf("PostMatchingBlacklist: email:%s", email))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PostMatchingBlacklist(exit): email:%s apiName:%s project:%s username:%s postMatchingBlacklist:%+v err:%v",
				email,
				apiName,
				project,
				username,
				postMatchingBlacklist,
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	postMatchingBlacklist, err = s.shDB.PostMatchingBlacklist(email)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// DeleteMatchingBlacklist: API params:
// /v1/affiliation/{projectSlug}/matching_blacklist/{email}
// {projectSlug} - required path parameter: project to modify affiliations emails blacklist (project slug URL encoded, can be prefixed with "/projects/")
// {email} - required path parameter: email to be deleted from blacklisted emails
func (s *service) DeleteMatchingBlacklist(ctx context.Context, params *affiliation.DeleteMatchingBlacklistParams) (status *models.TextStatusOutput, err error) {
	email := params.Email
	log.Info(fmt.Sprintf("DeleteMatchingBlacklist: email:%s", email))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteMatchingBlacklist(exit): email:%s apiName:%s project:%s username:%s status:%+v err:%v",
				email,
				apiName,
				project,
				username,
				status,
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	status, err = s.shDB.DeleteMatchingBlacklist(email)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// PutOrgDomain: API params:
// /v1/affiliation/{projectSlug}/add_domain/{orgName}/{domain}[?overwrite=true][&is_top_domain=true][&skip_enrollments=true]
// {orgName} - required path parameter:      organization to add domain to, must be URL encoded, for example 'The%20Microsoft%20company'
// {domain} - required path parameter:       domain to be added, for example 'microsoft.com'
// {projectSlug} - required path parameter:  project to modify affiliations (project slug URL encoded, can be prefixed with "/projects/")
// overwrite - optional query parameter:     if overwrite=true is set, all profiles found are force-updated/affiliated to the given organization
//                                           if overwite is not set, API will not change any profiles which already have any affiliation(s)
// is_top_domain - optional query parameter: if you specify is_top_domain=true it will set 'is_top_domain' DB column to true, else it will set false
// skip_enrollments - optional query parameter: if skip_enrollments=true is set, no enrollments will be modified/added/removed/touched
func (s *service) PutOrgDomain(ctx context.Context, params *affiliation.PutOrgDomainParams) (putOrgDomain *models.PutOrgDomainOutput, err error) {
	org := params.OrgName
	dom := params.Domain
	overwrite := false
	isTopDomain := false
	skipEnrollments := false
	if params.Overwrite != nil {
		overwrite = *params.Overwrite
	}
	if params.IsTopDomain != nil {
		isTopDomain = *params.IsTopDomain
	}
	log.Info(fmt.Sprintf("PutOrgDomain: org:%s dom:%s overwrite:%v isTopDomain:%v skipEnrollments", org, dom, overwrite, isTopDomain, skipEnrollments))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PutOrgDomain(exit): org:%s dom:%s overwrite:%v isTopDomain:%v skipEnrollments:%v apiName:%s project:%s username:%s putOrgDomain:%+v err:%v",
				org,
				dom,
				overwrite,
				isTopDomain,
				skipEnrollments,
				apiName,
				project,
				username,
				putOrgDomain,
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	putOrgDomain, err = s.shDB.PutOrgDomain(org, dom, overwrite, isTopDomain, skipEnrollments)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	putOrgDomain.User = username
	putOrgDomain.Scope = project
	return
}

// PutMergeUniqueIdentities: API
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
// /v1/affiliation/{projectSlug}/merge_unique_identities/{fromUUID}/{toUUID}[?archive=false]:
// {projectSlug} - required path parameter:  project to modify affiliations (project slug URL encoded, can be prefixed with "/projects/")
// {fromUUID} - required path parameter: uidentity/profile uuid to merge from, example "00029bc65f7fc5ba3dde20057770d3320ca51486"
// {toUUID} - required path parameter: uidentity/profile uuid to merge into, example "00058697877808f6b4a8524ac6dcf39b544a0c87"
// archive - optional query parameter: if archive=false it will not archive data to allow unmerge without data loss
func (s *service) PutMergeUniqueIdentities(ctx context.Context, params *affiliation.PutMergeUniqueIdentitiesParams) (profileData *models.ProfileDataOutput, err error) {
	fromUUID := params.FromUUID
	toUUID := params.ToUUID
	archive := true
	if params.Archive != nil {
		archive = *params.Archive
	}
	log.Info(fmt.Sprintf("PutMergeUniqueIdentities: fromUUID:%s toUUID:%s archive:%v", fromUUID, toUUID, archive))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PutMergeUniqueIdentities(exit): fromUUID:%s toUUID:%s apiName:%s project:%s username:%s profileData:%+v err:%v",
				fromUUID,
				toUUID,
				apiName,
				project,
				username,
				profileData,
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	err = s.shDB.MergeUniqueIdentities(fromUUID, toUUID, archive)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	profileData, err = s.shDB.GetProfile(toUUID, true, nil)
	if err != nil {
		err = errors.Wrap(err, "FIXME:"+apiName)
		return
	}
	return
}

// PutMoveIdentity: API
// ==================================================================================
// Move Identity to New Profile | Unmerge Identities and Profiles
// This function shifts the identity identified by 'fromID' to
// the unique identity/profile 'toUUID'.
// When 'toUUID' is the unique identity that is currently related
// to 'fromID', the action does not have any effect.
// In the case of 'fromID' and 'toUUID' have equal values and the
// unique identity does not exist, a new unique identity will be
// created and the identity will be moved to it (unmerged).
// ==================================================================================
// NOTE: fromID refers to `identities`.`id` while toUUID refers to `profiles`.`uuid`
// ==================================================================================
// /v1/affiliation/{projectSlug}/move_identity/{fromID}/{toUUID}[?archive=false]:
// {projectSlug} - required path parameter:  project to modify affiliations (project slug URL encoded, can be prefixed with "/projects/")
// {fromID} - required path parameter: identity id to move from, example "00029bc65f7fc5ba3dde20057770d3320ca51486"
// {toUUID} - required path parameter: uidentity/profile uuid to move into, example "00058697877808f6b4a8524ac6dcf39b544a0c87"
// archive - optional query parameter: if archive=false it will not attempt to restore data from archive
func (s *service) PutMoveIdentity(ctx context.Context, params *affiliation.PutMoveIdentityParams) (profileData *models.ProfileDataOutput, err error) {
	fromID := params.FromID
	toUUID := params.ToUUID
	archive := true
	if params.Archive != nil {
		archive = *params.Archive
	}
	log.Info(fmt.Sprintf("PutMoveIdentity: fromID:%s toUUID:%s archive:%v", fromID, toUUID, archive))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PutMoveIdentity(exit): fromID:%s toUUID:%s apiName:%s project:%s username:%s profileData:%+v err:%v",
				fromID,
				toUUID,
				apiName,
				project,
				username,
				profileData,
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	err = s.shDB.MoveIdentity(fromID, toUUID, archive)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	profileData, err = s.shDB.GetProfile(toUUID, true, nil)
	if err != nil {
		err = errors.Wrap(err, "FIXME:"+apiName)
		return
	}
	return
}

// GetUnaffiliated: API params:
// /v1/affiliation/{projectSlug}/unaffiliated[?q=xyz][&top_n=100]
// {projectSlug} - required path parameter: project to get top unaffiliated users (project slug URL encoded, can be prefixed with "/projects/")
// top_n - optional query parameter: return Top N unaffiliated users, 0 - means return all
func (s *service) GetUnaffiliated(ctx context.Context, params *affiliation.GetUnaffiliatedParams) (getUnaffiliated *models.GetUnaffiliatedOutput, err error) {
	topN := int64(10)
	if params.Topn != nil {
		topN = *params.Topn
		if topN < 0 {
			topN = 0
		}
	}
	getUnaffiliated = &models.GetUnaffiliatedOutput{}
	log.Info(fmt.Sprintf("GetUnaffiliated: top_n:%d", topN))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetUnaffiliated(exit): top_n:%d apiName:%s project:%s username:%s getUnaffiliated:%d err:%v",
				topN,
				apiName,
				project,
				username,
				len(getUnaffiliated.Unaffiliated),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	more := (topN + 10) * 10
	if topN == 0 {
		more = 0
	}
	getUnaffiliated, err = s.es.GetUnaffiliated(project, more)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	getUnaffiliated.Unaffiliated, err = s.shDB.CheckUnaffiliated(getUnaffiliated.Unaffiliated, nil)
	if err != nil {
		getUnaffiliated.Unaffiliated = []*models.UnaffiliatedDataOutput{}
		err = errors.Wrap(err, apiName)
		return
	}
	if topN > 0 && int64(len(getUnaffiliated.Unaffiliated)) > topN {
		getUnaffiliated.Unaffiliated = getUnaffiliated.Unaffiliated[0:topN]
	}
	getUnaffiliated.User = username
	getUnaffiliated.Scope = project
	return
}

// GetTopContributors: API params:
// /v1/affiliation/{projectSlug}/top_contributors?from=1552790984700&to=1552790984700][&limit=50][&offset=2]
// {projectSlug} - required path parameter: project to get top contributors stats (project slug URL encoded, can be prefixed with "/projects/")
// from - required query parameter - milliseconds since 1970, for example 1552790984700, filter data from
// to - required query parameter - milliseconds since 1970, for example 1552790984700, filter data to
// limit - optional query parameter: page size, default 10
// offset - optional query parameter: offset in pages, specifying limit=10 and offset=2, you will get 20-30)
func (s *service) GetTopContributors(ctx context.Context, params *affiliation.GetTopContributorsParams) (getTopContributors *models.GetTopContributorsOutput, err error) {
	limit := int64(10)
	if params.Limit != nil {
		limit = *params.Limit
		if limit < 1 {
			limit = 1
		}
	}
	offset := int64(0)
	if params.Offset != nil {
		offset = *params.Offset
		if offset < 0 {
			offset = 1
		}
	}
	getTopContributors = &models.GetTopContributorsOutput{}
	log.Info(fmt.Sprintf("GetTopContributors: from:%d to:%d limit:%d offset:%d", params.From, params.To, limit, offset))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetTopContributors(exit): from:%d to:%d limit:%d offset:%d apiName:%s project:%s username:%s getTopContributors:%d err:%v",
				params.From,
				params.To,
				limit,
				offset,
				apiName,
				project,
				username,
				len(getTopContributors.Contributors),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	getTopContributors, err = s.es.GetTopContributors(project, params.From, params.To, limit, offset)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	err = s.shDB.EnrichContributors(getTopContributors.Contributors, params.To, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	getTopContributors.User = username
	getTopContributors.Scope = project
	return
}
