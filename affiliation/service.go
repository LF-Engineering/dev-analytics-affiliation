package affiliation

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"encoding/json"
	"net/http"

	"github.com/dgrijalva/jwt-go"
	"github.com/go-openapi/strfmt"
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
	GetListOrganizationsDomains(ctx context.Context, in *affiliation.GetListOrganizationsDomainsParams) (*models.GetListOrganizationsDomainsOutput, error)
	GetFindOrganizationByID(ctx context.Context, in *affiliation.GetFindOrganizationByIDParams) (*models.OrganizationDataOutput, error)
	GetFindOrganizationByName(ctx context.Context, in *affiliation.GetFindOrganizationByNameParams) (*models.OrganizationDataOutput, error)
	PostAddOrganization(ctx context.Context, in *affiliation.PostAddOrganizationParams) (*models.OrganizationDataOutput, error)
	PutEditOrganization(ctx context.Context, in *affiliation.PutEditOrganizationParams) (*models.OrganizationDataOutput, error)
	DeleteOrganization(ctx context.Context, in *affiliation.DeleteOrganizationParams) (*models.TextStatusOutput, error)
	PutOrgDomain(ctx context.Context, in *affiliation.PutOrgDomainParams) (*models.PutOrgDomainOutput, error)
	DeleteOrgDomain(ctx context.Context, in *affiliation.DeleteOrgDomainParams) (*models.TextStatusOutput, error)
	GetListProfiles(ctx context.Context, in *affiliation.GetListProfilesParams) (*models.GetListProfilesOutput, error)
	GetProfile(ctx context.Context, in *affiliation.GetProfileParams) (*models.UniqueIdentityNestedDataOutput, error)
	PutEditProfile(ctx context.Context, in *affiliation.PutEditProfileParams) (*models.ProfileDataOutput, error)
	DeleteProfile(ctx context.Context, in *affiliation.DeleteProfileParams) (*models.TextStatusOutput, error)
	PostUnarchiveProfile(ctx context.Context, in *affiliation.PostUnarchiveProfileParams) (*models.UniqueIdentityNestedDataOutput, error)
	PostAddUniqueIdentity(ctx context.Context, in *affiliation.PostAddUniqueIdentityParams) (*models.UniqueIdentityNestedDataOutput, error)
	PostAddIdentity(ctx context.Context, in *affiliation.PostAddIdentityParams) (*models.UniqueIdentityNestedDataOutput, error)
	DeleteIdentity(ctx context.Context, in *affiliation.DeleteIdentityParams) (*models.TextStatusOutput, error)
	GetProfileEnrollments(ctx context.Context, in *affiliation.GetProfileEnrollmentsParams) (*models.GetProfileEnrollmentsDataOutput, error)
	PostAddEnrollment(ctx context.Context, in *affiliation.PostAddEnrollmentParams) (*models.UniqueIdentityNestedDataOutput, error)
	PutEditEnrollment(ctx context.Context, in *affiliation.PutEditEnrollmentParams) (*models.UniqueIdentityNestedDataOutput, error)
	DeleteEnrollments(ctx context.Context, in *affiliation.DeleteEnrollmentsParams) (*models.UniqueIdentityNestedDataOutput, error)
	PutMergeEnrollments(ctx context.Context, in *affiliation.PutMergeEnrollmentsParams) (*models.UniqueIdentityNestedDataOutput, error)
	PutMergeUniqueIdentities(ctx context.Context, in *affiliation.PutMergeUniqueIdentitiesParams) (*models.ProfileDataOutput, error)
	PutMoveIdentity(ctx context.Context, in *affiliation.PutMoveIdentityParams) (*models.ProfileDataOutput, error)
	GetUnaffiliated(ctx context.Context, in *affiliation.GetUnaffiliatedParams) (*models.GetUnaffiliatedOutput, error)
	GetTopContributors(ctx context.Context, in *affiliation.GetTopContributorsParams) (*models.TopContributorsFlatOutput, error)
	GetAllAffiliations(ctx context.Context, in *affiliation.GetAllAffiliationsParams) (*models.AllArrayOutput, error)
	PostBulkUpdate(ctx context.Context, in *affiliation.PostBulkUpdateParams) (*models.TextStatusOutput, error)
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
	case *affiliation.GetListOrganizationsDomainsParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetListOrganizationsDomains"
	case *affiliation.GetFindOrganizationByIDParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetFindOrganizationByID"
	case *affiliation.GetFindOrganizationByNameParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetFindOrganizationByName"
	case *affiliation.DeleteOrganizationParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "DeleteOrganization"
	case *affiliation.PostAddOrganizationParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PostAddOrganization"
	case *affiliation.PutEditOrganizationParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutEditOrganization"
	case *affiliation.GetProfileParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetProfile"
	case *affiliation.GetProfileEnrollmentsParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetProfileEnrollments"
	case *affiliation.DeleteProfileParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "DeleteProfile"
	case *affiliation.PutEditProfileParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutEditProfile"
	case *affiliation.PostUnarchiveProfileParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PostUnarchiveProfile"
	case *affiliation.PostAddUniqueIdentityParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PostAddUniqueIdentity"
	case *affiliation.PostAddIdentityParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PostAddIdentity"
	case *affiliation.DeleteIdentityParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "DeleteIdentity"
	case *affiliation.GetListProfilesParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "GetListProfiles"
	case *affiliation.PostAddEnrollmentParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PostAddEnrollment"
	case *affiliation.PutEditEnrollmentParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutEditEnrollment"
	case *affiliation.DeleteEnrollmentsParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "DeleteEnrollments"
	case *affiliation.PutMergeEnrollmentsParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutMergeEnrollments"
	case *affiliation.PutOrgDomainParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutOrgDomain"
	case *affiliation.DeleteOrgDomainParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "DeleteOrgDomain"
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
// rows - optional query parameter: rows per page, if 0 no paging is used and page parameter is ignored, default 10 (setting to zero still limits results to 65535)
// page - optional query parameter: if set, it will return rows from a given page, default 1
func (s *service) GetListOrganizations(ctx context.Context, params *affiliation.GetListOrganizationsParams) (getListOrganizations *models.GetListOrganizationsOutput, err error) {
	q := ""
	if params.Q != nil {
		q = *params.Q
	}
	rows := int64(10)
	if params.Rows != nil {
		rows = *params.Rows
		if rows <= 0 {
			rows = 0xffff
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
// {orgName} - required path parameter: organization name to be added
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

// PutEditOrganization: API params:
// /v1/affiliation/{projectSlug}/edit_organization/{orgID}/{orgName}
// {projectSlug} - required path parameter: project to add organization to (project slug URL encoded, can be prefixed with "/projects/")
// {orgID} - required path parameter: organization ID to be edited
// {orgName} - required path parameter: organization name - this is the new name that will be saved for this organization
func (s *service) PutEditOrganization(ctx context.Context, params *affiliation.PutEditOrganizationParams) (organization *models.OrganizationDataOutput, err error) {
	organization = &models.OrganizationDataOutput{}
	orgID := params.OrgID
	orgName := params.OrgName
	log.Info(fmt.Sprintf("PutEditOrganization: orgID:%d orgName:%s", orgID, orgName))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PutEditOrganization(exit): orgID:%d orgName:%s apiName:%s project:%s username:%s organization:%+v err:%v",
				orgID,
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
	organization, err = s.shDB.EditOrganization(
		&models.OrganizationDataOutput{
			ID:   orgID,
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

// PostAddUniqueIdentity: API params:
// /v1/affiliation/{projectSlug}/add_unique_identity/{uuid}
// {projectSlug} - required path parameter: project to add unique identity to (project slug URL encoded, can be prefixed with "/projects/")
// {uuid} - required path parameter: UUID to be added
func (s *service) PostAddUniqueIdentity(ctx context.Context, params *affiliation.PostAddUniqueIdentityParams) (uniqueIdentity *models.UniqueIdentityNestedDataOutput, err error) {
	uniqueIdentity = &models.UniqueIdentityNestedDataOutput{}
	uuid := params.UUID
	log.Info(fmt.Sprintf("PostAddUniqueIdentity: uuid:%s", uuid))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PostAddUniqueIdentity(exit): uuid:%s apiName:%s project:%s username:%s uniqueIdentity:%+v err:%v",
				uuid,
				apiName,
				project,
				username,
				s.ToLocalNestedUniqueIdentity(uniqueIdentity),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	uniqueIdentity, err = s.shDB.AddNestedUniqueIdentity(uuid)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// DeleteIdentity: API params:
// /v1/affiliation/{projectSlug}/delete_identity/{id}
// {projectSlug} - required path parameter: project to add unique identity to (project slug URL encoded, can be prefixed with "/projects/")
// {id} - required path parameter: Identity ID to be added
func (s *service) DeleteIdentity(ctx context.Context, params *affiliation.DeleteIdentityParams) (status *models.TextStatusOutput, err error) {
	id := params.ID
	status = &models.TextStatusOutput{}
	log.Info(fmt.Sprintf("DeleteIdentity: id:%s", id))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteIdentity(exit): id:%s apiName:%s project:%s username:%s status:%+v err:%v",
				id,
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
	err = s.shDB.DeleteIdentity(id, false, true, nil, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	status.Text = "Deleted identity id '" + id + "'"
	return
}

// PostAddIdentity: API params:
// /v1/affiliation/{projectSlug}/add_identity/{source}
// {projectSlug} - required path parameter: project to add unique identity to (project slug URL encoded, can be prefixed with "/projects/")
// {source} - required path parameter: Source of identity to be added
// name - optional query parameter: identity name
// email - optional query parameter: identity email
// username - optional query parameter: identity username
// uuid - optional query parameter: UUID, if set - identity will be connected to that UUID if not, it will be auto-generated and new unique identity will be created
func (s *service) PostAddIdentity(ctx context.Context, params *affiliation.PostAddIdentityParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	identity := &models.IdentityDataOutput{
		Source:   params.Source,
		Name:     params.Name,
		Email:    params.Email,
		Username: params.Username,
		UUID:     params.UUID,
	}
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("PostAddIdentity: source:%s identity:%+v uid:%+v", params.Source, s.ToLocalIdentity(identity), s.ToLocalNestedUniqueIdentity(uid)))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PostAddIdentity(exit): source:%s identity:%+v apiName:%s project:%s username:%s uid:%+v err:%v",
				params.Source,
				s.ToLocalIdentity(identity),
				apiName,
				project,
				username,
				s.ToLocalNestedUniqueIdentity(uid),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	uid, err = s.shDB.AddNestedIdentity(identity)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// PostAddEnrollment: API params:
// /v1/affiliation/{projectSlug}/add_enrollment/{uuid}/{orgName}
// {projectSlug} - required path parameter: project to add enrollment to (project slug URL encoded, can be prefixed with "/projects/")
// {uuid} - required path parameter: Profile UUID to add enrollment to
// {orgName} - required path parameter: enrollment organization to add (must exist)
// start - optional query parameter: enrollment start date, 1900-01-01 if not set
// end - optional query parameter: enrollment end date, 2100-01-01 if not set
// merge - optional query parameter: if set it will merge enrollment dates for organization added
func (s *service) PostAddEnrollment(ctx context.Context, params *affiliation.PostAddEnrollmentParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	enrollment := &models.EnrollmentDataOutput{UUID: params.UUID}
	organization := &models.OrganizationDataOutput{Name: params.OrgName}
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("PostAddEnrollment: uuid:%s enrollment:%+v organization:%+v uid:%+v", params.UUID, enrollment, organization, s.ToLocalNestedUniqueIdentity(uid)))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PostAddEnrollment(exit): uuid:%s enrollment:%+v organization:%+v apiName:%s project:%s username:%s uid:%+v err:%v",
				params.UUID,
				enrollment,
				organization,
				apiName,
				project,
				username,
				s.ToLocalNestedUniqueIdentity(uid),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	organization, err = s.shDB.GetOrganizationByName(params.OrgName, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	enrollment.OrganizationID = organization.ID
	uniqueIdentity := &models.UniqueIdentityDataOutput{}
	uniqueIdentity, err = s.shDB.GetUniqueIdentity(params.UUID, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	_, err = s.shDB.GetProfile(params.UUID, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	if params.Start != nil {
		enrollment.Start = *(params.Start)
	} else {
		enrollment.Start = strfmt.DateTime(shdb.MinPeriodDate)
	}
	if params.End != nil {
		enrollment.End = *(params.End)
	} else {
		enrollment.End = strfmt.DateTime(shdb.MaxPeriodDate)
	}
	// Do the actual API call
	_, err = s.shDB.AddEnrollment(enrollment, false, false, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	if params.Merge != nil && *(params.Merge) {
		err = s.shDB.MergeEnrollments(uniqueIdentity, organization, nil)
		if err != nil {
			err = errors.Wrap(err, apiName)
			return
		}
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+params.UUID, 1, 1, false, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errors.Wrap(fmt.Errorf("Profile with UUID '%s' not found", params.UUID), apiName)
		return
	}
	uid = ary[0]
	return
}

// PutEditEnrollment: API params:
// /v1/affiliation/{projectSlug}/edit_enrollment/{uuid}/{orgName}
// {projectSlug} - required path parameter: project to edit enrollment for (project slug URL encoded, can be prefixed with "/projects/")
// {uuid} - required path parameter: Profile UUID to edit enrollment on
// {orgName} - required path parameter: enrollment organization to edt (must exist) (if that organization is affiliated to given profile more than once, start and/or end date must be given too)
// start - optional query parameter: current enrollment start date to edit, not used if not specified, start or end must be specified if organization is enrolled more than once on the profile
// end - optional query parameter: current enrollment end date, not used if not specified, start or end must be specified if organization is enrolled more than once on the profile
// new_start - optional query parameter: new enrollment start date, 1900-01-01 if not set
// new_end - optional query parameter: new enrollment end date, 2100-01-01 if not set
// merge - optional query parameter: if set it will merge enrollment dates for organization edited
func (s *service) PutEditEnrollment(ctx context.Context, params *affiliation.PutEditEnrollmentParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	enrollment := &models.EnrollmentDataOutput{UUID: params.UUID}
	organization := &models.OrganizationDataOutput{Name: params.OrgName}
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("PutEditEnrollment: uuid:%s enrollment:%+v organization:%+v uid:%+v", params.UUID, enrollment, organization, s.ToLocalNestedUniqueIdentity(uid)))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PutEditEnrollment(exit): uuid:%s enrollment:%+v organization:%+v apiName:%s project:%s username:%s uid:%+v err:%v",
				params.UUID,
				enrollment,
				organization,
				apiName,
				project,
				username,
				s.ToLocalNestedUniqueIdentity(uid),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	organization, err = s.shDB.GetOrganizationByName(params.OrgName, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	enrollment.OrganizationID = organization.ID
	uniqueIdentity := &models.UniqueIdentityDataOutput{}
	uniqueIdentity, err = s.shDB.GetUniqueIdentity(params.UUID, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	_, err = s.shDB.GetProfile(params.UUID, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	columns := []string{"uuid", "organization_id"}
	values := []interface{}{params.UUID, enrollment.OrganizationID}
	isDates := []bool{false, false}
	if params.Start != nil {
		columns = append(columns, "start")
		values = append(values, *(params.Start))
		isDates = append(isDates, true)
	}
	if params.End != nil {
		columns = append(columns, "end")
		values = append(values, *(params.End))
		isDates = append(isDates, true)
	}
	rols := []*models.EnrollmentDataOutput{}
	rols, err = s.shDB.FindEnrollments(columns, values, isDates, true, nil)
	if err != nil {
		return
	}
	if len(rols) > 1 {
		err = errors.Wrap(
			fmt.Errorf(
				"multiple enrollments found for columns %+v values %+v: %+v",
				columns,
				values,
				s.ToLocalEnrollments(rols),
			),
			apiName,
		)
		return
	}
	enrollment.ID = rols[0].ID
	if params.NewStart != nil {
		enrollment.Start = *(params.NewStart)
	} else {
		enrollment.Start = strfmt.DateTime(shdb.MinPeriodDate)
	}
	if params.NewEnd != nil {
		enrollment.End = *(params.NewEnd)
	} else {
		enrollment.End = strfmt.DateTime(shdb.MaxPeriodDate)
	}
	_, err = s.shDB.EditEnrollment(enrollment, false, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	if params.Merge != nil && *(params.Merge) {
		err = s.shDB.MergeEnrollments(uniqueIdentity, organization, nil)
		if err != nil {
			err = errors.Wrap(err, apiName)
			return
		}
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+params.UUID, 1, 1, false, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errors.Wrap(fmt.Errorf("Profile with UUID '%s' not found", params.UUID), apiName)
		return
	}
	uid = ary[0]
	return
}

// DeleteEnrollments: API params:
// /v1/affiliation/{projectSlug}/delete_enrollments/{uuid}/{orgName}
// {projectSlug} - required path parameter: project to delete enrollments from (project slug URL encoded, can be prefixed with "/projects/")
// {uuid} - required path parameter: Profile UUID to add enrollments from
// {orgName} - required path parameter: enrollments organization to delete (must exist)
// start - optional query parameter: enrollments start date, 1900-01-01 if not set
// end - optional query parameter: enrollments end date, 2100-01-01 if not set
func (s *service) DeleteEnrollments(ctx context.Context, params *affiliation.DeleteEnrollmentsParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	enrollment := &models.EnrollmentDataOutput{UUID: params.UUID}
	organization := &models.OrganizationDataOutput{Name: params.OrgName}
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("DeleteEnrollments: uuid:%s enrollment:%+v organization:%+v uid:%+v", params.UUID, enrollment, organization, s.ToLocalNestedUniqueIdentity(uid)))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteEnrollments(exit): uuid:%s enrollment:%+v organization:%+v apiName:%s project:%s username:%s uid:%+v err:%v",
				params.UUID,
				enrollment,
				organization,
				apiName,
				project,
				username,
				s.ToLocalNestedUniqueIdentity(uid),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	organization, err = s.shDB.GetOrganizationByName(params.OrgName, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	enrollment.OrganizationID = organization.ID
	_, err = s.shDB.GetUniqueIdentity(params.UUID, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	_, err = s.shDB.GetProfile(params.UUID, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	if params.Start != nil {
		enrollment.Start = *(params.Start)
	} else {
		enrollment.Start = strfmt.DateTime(shdb.MinPeriodDate)
	}
	if params.End != nil {
		enrollment.End = *(params.End)
	} else {
		enrollment.End = strfmt.DateTime(shdb.MaxPeriodDate)
	}
	// Do the actual API call
	err = s.shDB.WithdrawEnrollment(enrollment, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+params.UUID, 1, 1, false, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errors.Wrap(fmt.Errorf("Profile with UUID '%s' not found", params.UUID), apiName)
		return
	}
	uid = ary[0]
	return
}

// PutMergeEnrollments: API params:
//  This function merges those enrollments, related to the given 'uuid' and
//  'organization', that have overlapping dates. Default start and end dates
//  (1900-01-01 and 2100-01-01) are considered range limits and will be
//  removed when a set of ranges overlap. For example:
//   * [(1900-01-01, 2010-01-01), (2008-01-01, 2100-01-01)]
//         --> (2008-01-01, 2010-01-01)
//   * [(1900-01-01, 2010-01-01), (2008-01-01, 2010-01-01), (2010-01-02, 2100-01-01)]
//         --> (2008-01-01, 2010-01-01),(2010-01-02, 2100-01-01)
//   * [(1900-01-01, 2010-01-01), (2010-01-02, 2100-01-01)]
//         --> (1900-01-01, 2010-01-01), (2010-01-02, 2100-01-01)
// /v1/affiliation/{projectSlug}/merge_enrollments/{uuid}/{orgName}
// {projectSlug} - required path parameter: project to merge enrollments (project slug URL encoded, can be prefixed with "/projects/")
// {uuid} - required path parameter: Profile UUID to merge enrollments
// {orgName} - required path parameter: enrollment organization to delete (must exist)
func (s *service) PutMergeEnrollments(ctx context.Context, params *affiliation.PutMergeEnrollmentsParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	enrollment := &models.EnrollmentDataOutput{UUID: params.UUID}
	organization := &models.OrganizationDataOutput{Name: params.OrgName}
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("PutMergeEnrollments: uuid:%s enrollment:%+v organization:%+v uid:%+v", params.UUID, enrollment, organization, s.ToLocalNestedUniqueIdentity(uid)))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PutMergeEnrollments(exit): uuid:%s enrollment:%+v organization:%+v apiName:%s project:%s username:%s uid:%+v err:%v",
				params.UUID,
				enrollment,
				organization,
				apiName,
				project,
				username,
				s.ToLocalNestedUniqueIdentity(uid),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	organization, err = s.shDB.GetOrganizationByName(params.OrgName, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	enrollment.OrganizationID = organization.ID
	uniqueIdentity := &models.UniqueIdentityDataOutput{}
	uniqueIdentity, err = s.shDB.GetUniqueIdentity(params.UUID, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	_, err = s.shDB.GetProfile(params.UUID, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	// Do the actual API call
	err = s.shDB.MergeEnrollments(uniqueIdentity, organization, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+params.UUID, 1, 1, false, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errors.Wrap(fmt.Errorf("Profile with UUID '%s' not found", params.UUID), apiName)
		return
	}
	uid = ary[0]
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
				"GetFindOrganizationByID(exit): orgID:%d apiName:%s project:%s username:%s getFindOrganizationByID:%+v err:%v",
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
				"GetFindOrganizationByName(exit): orgName:%s apiName:%s project:%s username:%s getFindOrganizationByName:%+v err:%v",
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

// PutEditProfile: API params:
// /v1/affiliation/{projectSlug}/edit_profile/{uuid}[?name=somename][&email=xyz@o2.pl][&gender=female][&gender-acc=95][&is_bot=0][&country_code=PL]
// {projectSlug} - required path parameter: project to edit profile in (project slug URL encoded, can be prefixed with "/projects/")
// {uuid} - required path parameter: profile uuid to be edited
// name - optional query parameter: if set, it will update profile name to this value
// email - optional query parameter: if set, it will update profile email to this value
// gender - optional query parameter: if set, it will update profile gender to this value: allowed: male, female
// gender_acc - optional query parameter: if set, it will update profile gender probablity to this value: integer 1-100
// is_bot - optional query parameter: if set, it will update profile bot flag to this value, integer, allowed: 0, 1
// country_code - optional query parameter: if set, it will update profile country code to this value, 2 letter contry code, validated agains countries table (foreign key), for example: PL
func (s *service) PutEditProfile(ctx context.Context, params *affiliation.PutEditProfileParams) (profile *models.ProfileDataOutput, err error) {
	uuid := params.UUID
	profile = &models.ProfileDataOutput{
		UUID:        uuid,
		Name:        params.Name,
		Email:       params.Email,
		Gender:      params.Gender,
		GenderAcc:   params.GenderAcc,
		IsBot:       params.IsBot,
		CountryCode: params.CountryCode,
	}
	log.Info(fmt.Sprintf("PutEditProfile: uuid:%s uid:%+v", uuid, s.ToLocalProfile(profile)))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PutEditProfile(exit): uuid:%s apiName:%s project:%s username:%s profile:%+v err:%v",
				uuid,
				apiName,
				project,
				username,
				s.ToLocalProfile(profile),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	profile, err = s.shDB.EditProfile(profile, true, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// DeleteProfile: API params:
// /v1/affiliation/{projectSlug}/delete_profile/{uuid}[?archive=true]
// {projectSlug} - required path parameter: project to delete profile from (project slug URL encoded, can be prefixed with "/projects/")
// {uuid} - required path parameter: profile uuid to be deleted (it will cascade delete all objects referring to that uuid)
// archive - optional query parameter: if set, it will archive given profile data (and all dependend objects, so full restore will be possible)
func (s *service) DeleteProfile(ctx context.Context, params *affiliation.DeleteProfileParams) (status *models.TextStatusOutput, err error) {
	uuid := params.UUID
	archive := false
	if params.Archive != nil {
		archive = *params.Archive
	}
	log.Info(fmt.Sprintf("DeleteProfile: uuid:%s archive:%v", uuid, archive))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteProfile(exit): uuid:%s archive:%v apiName:%s project:%s username:%s status:%+v err:%v",
				uuid,
				archive,
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
	status, err = s.shDB.DeleteProfileNested(uuid, archive)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// PostUnarchiveProfile: API params:
// /v1/affiliation/{projectSlug}/delete_profile/{uuid}[?archive=true]
// {projectSlug} - required path parameter: project where we need to unarchive profile (project slug URL encoded, can be prefixed with "/projects/")
// {uuid} - required path parameter: profile uuid to be unarchived (it will cascade delete all objects referring to that uuid)
func (s *service) PostUnarchiveProfile(ctx context.Context, params *affiliation.PostUnarchiveProfileParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	uuid := params.UUID
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("PostUnarchiveProfile: uuid:%s", uuid))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PostUnarchiveProfile(exit): uuid:%s apiName:%s project:%s username:%s uid:%+v err:%v",
				uuid,
				apiName,
				project,
				username,
				s.ToLocalNestedUniqueIdentity(uid),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	uid, err = s.shDB.UnarchiveProfileNested(uuid)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// DeleteOrganization: API params:
// /v1/affiliation/{projectSlug}/delete_organization_by_id/{orgID}
// {projectSlug} - required path parameter: project to modify affiliations organizations (project slug URL encoded, can be prefixed with "/projects/")
// {orgID} - required path parameter: organization ID to be deleted
func (s *service) DeleteOrganization(ctx context.Context, params *affiliation.DeleteOrganizationParams) (status *models.TextStatusOutput, err error) {
	orgID := params.OrgID
	log.Info(fmt.Sprintf("DeleteOrganization: orgID:%d", orgID))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteOrganization(exit): orgID:%d apiName:%s project:%s username:%s status:%+v err:%v",
				orgID,
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
	status, err = s.shDB.DeleteOrganization(orgID)
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
// rows - optional query parameter: rows per page, if 0 no paging is used and page parameter is ignored, default 10 (setting to zero still limits results to 65535)
// page - optional query parameter: if set, it will return rows from a given page, default 1
func (s *service) GetMatchingBlacklist(ctx context.Context, params *affiliation.GetMatchingBlacklistParams) (getMatchingBlacklist *models.GetMatchingBlacklistOutput, err error) {
	q := ""
	if params.Q != nil {
		q = *params.Q
	}
	rows := int64(10)
	if params.Rows != nil {
		rows = *params.Rows
		if rows <= 0 {
			rows = 0xfff
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

// GetListProfiles: API params:
// /v1/affiliation/{projectSlug}/list_profiles[?q=xyz][&rows=100][&page=2]
// {projectSlug} - required path parameter: project to get profiles (project slug URL encoded, can be prefixed with "/projects/")
// q - optional query parameter: if you specify that parameter only profiles where name, email, username or source like '%q%' will be returned
// rows - optional query parameter: rows per page, if 0 no paging is used and page parameter is ignored, default 10 (setting to zero still limits results to 65535)
// page - optional query parameter: if set, it will return rows from a given page, default 1
func (s *service) GetListProfiles(ctx context.Context, params *affiliation.GetListProfilesParams) (getListProfiles *models.GetListProfilesOutput, err error) {
	q := ""
	if params.Q != nil {
		q = *params.Q
	}
	rows := int64(10)
	if params.Rows != nil {
		rows = *params.Rows
		if rows <= 0 {
			rows = 0xffff
		}
	}
	page := int64(1)
	if params.Page != nil {
		page = *params.Page
		if page < 1 {
			page = 1
		}
	}
	getListProfiles = &models.GetListProfilesOutput{}
	log.Info(fmt.Sprintf("GetListProfiles: q:%s rows:%d page:%d", q, rows, page))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		list := ""
		nProfs := len(getListProfiles.Uids)
		if nProfs > shared.LogListMax {
			list = fmt.Sprintf("%d", nProfs)
		} else {
			list = fmt.Sprintf("%+v", s.ToLocalNestedUniqueIdentities(getListProfiles.Uids))
		}
		log.Info(
			fmt.Sprintf(
				"GetListProfiles(exit): q:%s rows:%d page:%d apiName:%s project:%s username:%s getListProfiles:%s err:%v",
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
	getListProfiles, err = s.shDB.GetListProfiles(q, rows, page)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	getListProfiles.User = username
	getListProfiles.Scope = project
	return
}

// GetProfile: API params:
// /v1/affiliation/{projectSlug}/get_profile/{uuid}
// {projectSlug} - required path parameter: project to get profile from (project slug URL encoded, can be prefixed with "/projects/")
// {uuid} - required path parameter: UUID of the profile to get
func (s *service) GetProfile(ctx context.Context, params *affiliation.GetProfileParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	uuid := params.UUID
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("GetProfile: uuid:%s", uuid))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetProfile(exit): uuid:%s apiName:%s project:%s username:%s uid:%v err:%v",
				uuid,
				apiName,
				project,
				username,
				s.ToLocalNestedUniqueIdentity(uid),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+uuid, 1, 1, false, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errors.Wrap(fmt.Errorf("Profile with UUID '%s' not found", uuid), apiName)
		return
	}
	uid = ary[0]
	return
}

// GetProfileEnrollments: API params:
// /v1/affiliation/{projectSlug}/enrollments/{uuid}
// {projectSlug} - required path parameter: project to get profile from (project slug URL encoded, can be prefixed with "/projects/")
// {uuid} - required path parameter: UUID of the profile to get enrollments
func (s *service) GetProfileEnrollments(ctx context.Context, params *affiliation.GetProfileEnrollmentsParams) (output *models.GetProfileEnrollmentsDataOutput, err error) {
	uuid := params.UUID
	output = &models.GetProfileEnrollmentsDataOutput{}
	log.Info(fmt.Sprintf("GetProfileEnrollments: uuid:%s", uuid))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetProfileEnrollments(exit): uuid:%s apiName:%s project:%s username:%s output:%v err:%v",
				uuid,
				apiName,
				project,
				username,
				output,
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	var enrollments []*models.EnrollmentNestedDataOutput
	enrollments, err = s.shDB.FindEnrollmentsNested([]string{"e.uuid"}, []interface{}{uuid}, []bool{false}, false, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	if len(enrollments) == 0 {
		_, err = s.shDB.GetUniqueIdentity(uuid, true, nil)
		if err != nil {
			err = errors.Wrap(err, apiName)
			return
		}
	}
	output.UUID = uuid
	output.User = username
	output.Scope = project
	output.Enrollments = enrollments
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
	if params.SkipEnrollments != nil {
		skipEnrollments = *params.SkipEnrollments
	}
	log.Info(fmt.Sprintf("PutOrgDomain: org:%s dom:%s overwrite:%v isTopDomain:%v skipEnrollments:%v", org, dom, overwrite, isTopDomain, skipEnrollments))
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

// DeleteOrgDomain: API params:
// /v1/affiliation/{projectSlug}/remove_domain/{orgName}/{domain}
// {orgName} - required path parameter:      organization to remove domain from, must be URL encoded, for example 'The%20Microsoft%20company'
// {domain} - required path parameter:       domain to be deleted, for example 'microsoft.com'
func (s *service) DeleteOrgDomain(ctx context.Context, params *affiliation.DeleteOrgDomainParams) (status *models.TextStatusOutput, err error) {
	org := params.OrgName
	dom := params.Domain
	log.Info(fmt.Sprintf("DeleteOrgDomain: org:%s dom:%s", org, dom))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteOrgDomain(exit): org:%s dom:%s apiName:%s project:%s username:%s status:%+v err:%v",
				org,
				dom,
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
	status, err = s.shDB.DeleteOrgDomain(org, dom)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// GetListOrganizationsDomains: API params:
// /v1/affiliation/{projectSlug}/list_domains[?orgID=23456][&q=xyz][&rows=100][&page=2]
// {projectSlug} - required path parameter: project to get organizations (project slug URL encoded, can be prefixed with "/projects/")
// orgID - optional query parameter: organization ID to get domains, default is 0 it return data for all organizations then
// q - optional query parameter: if you specify that parameter only domains like '%q%' will be returned
// rows - optional query parameter: rows per page, if 0 no paging is used and page parameter is ignored, default 10  (setting to zero still limits results to 65535)
// page - optional query parameter: if set, it will return rows from a given page, default 1
func (s *service) GetListOrganizationsDomains(ctx context.Context, params *affiliation.GetListOrganizationsDomainsParams) (getListOrganizationsDomains *models.GetListOrganizationsDomainsOutput, err error) {
	orgID := int64(0)
	if params.OrgID != nil {
		orgID = *params.OrgID
	}
	q := ""
	if params.Q != nil {
		q = *params.Q
	}
	rows := int64(10)
	if params.Rows != nil {
		rows = *params.Rows
		if rows <= 0 {
			rows = 0xffff
		}
	}
	page := int64(1)
	if params.Page != nil {
		page = *params.Page
		if page < 1 {
			page = 1
		}
	}
	getListOrganizationsDomains = &models.GetListOrganizationsDomainsOutput{}
	log.Info(fmt.Sprintf("GetListOrganizationsDomains: orgID:%d q:%s rows:%d page:%d", orgID, q, rows, page))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		list := ""
		nDoms := len(getListOrganizationsDomains.Domains)
		if nDoms > shared.LogListMax {
			list = fmt.Sprintf("%d", nDoms)
		} else {
			list = fmt.Sprintf("%+v", s.ToLocalDomains(getListOrganizationsDomains.Domains))
		}
		log.Info(
			fmt.Sprintf(
				"GetListOrganizationsDomains(exit): orgID:%d q:%s rows:%d page:%d apiName:%s project:%s username:%s getListOrganizationsDomains:%s err:%v",
				orgID,
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
	getListOrganizationsDomains, err = s.shDB.GetListOrganizationsDomains(orgID, q, rows, page)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	getListOrganizationsDomains.User = username
	getListOrganizationsDomains.Scope = project
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
		err = errors.Wrap(err, apiName)
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
		err = errors.Wrap(err, apiName)
		return
	}
	return
}

// GetUnaffiliated: API params:
// /v1/affiliation/{projectSlug}/unaffiliated[?page=2][&[rows=50]
// {projectSlug} - required path parameter: project to get top unaffiliated users (project slug URL encoded, can be prefixed with "/projects/")
// rows - optional query parameter: rows per page, if 0 no paging is used and page parameter is ignored, default 10  (setting to zero still limits results to 65535)
// page - optional query parameter: if set, it will return rows from a given page, default 1
func (s *service) GetUnaffiliated(ctx context.Context, params *affiliation.GetUnaffiliatedParams) (getUnaffiliated *models.GetUnaffiliatedOutput, err error) {
	rows := int64(10)
	if params.Rows != nil {
		rows = *params.Rows
		if rows <= 0 {
			rows = 0xffff
		}
	}
	page := int64(1)
	if params.Page != nil {
		page = *params.Page
		if page < 1 {
			page = 1
		}
	}
	getUnaffiliated = &models.GetUnaffiliatedOutput{}
	log.Info(fmt.Sprintf("GetUnaffiliated: rows:%d page:%d", rows, page))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetUnaffiliated(exit): rows:%d page:%d apiName:%s project:%s username:%s getUnaffiliated:%d err:%v",
				rows,
				page,
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
	last := page * rows
	more := (last + 5) * 3
	prevN := int64(-1)
	for {
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
		n := int64(len(getUnaffiliated.Unaffiliated))
		if prevN == n {
			break
		}
		if n < last {
			prevN = n
			more *= 2
			continue
		}
		break
	}
	n := int64(len(getUnaffiliated.Unaffiliated))
	from := (page - 1) * rows
	to := from + rows
	if from > n {
		from = n
	}
	if to > n {
		to = n
	}
	if from == to {
		getUnaffiliated.Unaffiliated = []*models.UnaffiliatedDataOutput{}
		return
	}
	getUnaffiliated.Unaffiliated = getUnaffiliated.Unaffiliated[from:to]
	getUnaffiliated.Page = page
	getUnaffiliated.Rows = rows
	getUnaffiliated.User = username
	getUnaffiliated.Scope = project
	return
}

// GetTopContributors: API params:
// /v1/affiliation/{projectSlug}/top_contributors?from=1552790984700&to=1552790984700][&limit=50][&offset=2]
// {projectSlug} - required path parameter: project to get top contributors stats (project slug URL encoded, can be prefixed with "/projects/")
// from - optional query parameter - milliseconds since 1970, for example 1552790984700, filter data from, default 90 days ago
// to - optional query parameter - milliseconds since 1970, for example 1552790984700, filter data to, default now
// limit - optional query parameter: page size, default 10
// offset - optional query parameter: offset in pages, specifying limit=10 and offset=2, you will get 20-30)
// search - optional query parameter: for example john
// sort_field - optional query parameter: sort field for example gerrit_merged_changesets
// sort_order - optional query parameter: sort order for example desc, asc
func (s *service) GetTopContributors(ctx context.Context, params *affiliation.GetTopContributorsParams) (topContributors *models.TopContributorsFlatOutput, err error) {
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
	from := int64(0)
	if params.From != nil {
		from = *params.From
	} else {
		from = (time.Now().Add(-24 * 90 * time.Hour)).UnixNano() / 1.0e6
	}
	to := int64(0)
	if params.To != nil {
		to = *params.To
	} else {
		to = time.Now().UnixNano() / 1.0e6
	}
	if to < from {
		err = fmt.Errorf("to parameter (%d) must be higher or equal from (%d)", to, from)
		return
	}
	search := ""
	if params.Search != nil {
		search = strings.ToLower(strings.TrimSpace(*params.Search))
	}
	sortField := ""
	if params.SortField != nil {
		sortField = strings.TrimSpace(*params.SortField)
	}
	sortOrder := ""
	if params.SortOrder != nil {
		sortOrder = strings.ToLower(strings.TrimSpace(*params.SortOrder))
	}
	topContributors = &models.TopContributorsFlatOutput{}
	log.Info(fmt.Sprintf("GetTopContributors: from:%d to:%d limit:%d offset:%d search:%s sortField:%s sortOrder:%s", from, to, limit, offset, search, sortField, sortOrder))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetTopContributors(exit): from:%d to:%d limit:%d offset:%d search:%s sortField:%s sortOrder:%s apiName:%s project:%s username:%s topContributors:%d err:%v",
				from,
				to,
				limit,
				offset,
				search,
				sortField,
				sortOrder,
				apiName,
				project,
				username,
				len(topContributors.Contributors),
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	// Do the actual API call
	topContributors, err = s.es.GetTopContributors(project, from, to, limit, offset, search, sortField, sortOrder)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	err = s.shDB.EnrichContributors(topContributors.Contributors, to, nil)
	if err != nil {
		err = errors.Wrap(err, apiName)
		return
	}
	topContributors.From = from
	topContributors.To = to
	topContributors.Limit = limit
	topContributors.Offset = offset
	topContributors.Search = search
	topContributors.SortField = sortField
	topContributors.SortOrder = sortOrder
	topContributors.User = username
	topContributors.Scope = project
	return
}

// GetAllAffiliations: API params:
// /v1/affiliation/all
func (s *service) GetAllAffiliations(ctx context.Context, params *affiliation.GetAllAffiliationsParams) (all *models.AllArrayOutput, err error) {
	all = &models.AllArrayOutput{}
	log.Info("GetAllAffiliations")
	defer func() {
		log.Info(fmt.Sprintf("GetAllAffiliations(exit): all:%d err:%v", len(all.Profiles), err))
	}()
	all, err = s.shDB.GetAllAffiliations()
	if err != nil {
		return
	}
	return
}

// PostBulkUpdate: API params:
// /v1/affiliation/bulk_update
// update - required body YAML parameter - list of profiles to add and/or remove
func (s *service) PostBulkUpdate(ctx context.Context, params *affiliation.PostBulkUpdateParams) (status *models.TextStatusOutput, err error) {
	status = &models.TextStatusOutput{}
	nAdded := 0
	nDeleted := 0
	nUpdated := 0
	log.Info(fmt.Sprintf("PostBulkUpdate: add:%d del:%d", len(params.Body.Add), len(params.Body.Del)))
	defer func() {
		log.Info(fmt.Sprintf("PostBulkUpdate(exit): add:%d del:%d added:%d deleted:%d updated:%d status:%s err:%v", len(params.Body.Add), len(params.Body.Del), nAdded, nDeleted, nUpdated, status.Text, err))
	}()
	nAdded, nDeleted, nUpdated, err = s.shDB.BulkUpdate(params.Body.Add, params.Body.Del)
	if err != nil {
		return
	}
	status.Text = fmt.Sprintf("Requested: Add: %d, Delete:%d, Done: Added: %d, Deleted: %d, Updated: %d", len(params.Body.Add), len(params.Body.Del), nAdded, nDeleted, nUpdated)
	return
}
