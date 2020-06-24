package affiliation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/dgrijalva/jwt-go"
	"github.com/go-openapi/strfmt"

	"github.com/LF-Engineering/dev-analytics-affiliation/apidb"
	"github.com/LF-Engineering/dev-analytics-affiliation/elastic"
	"github.com/LF-Engineering/dev-analytics-affiliation/errs"
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
	PutEditProfile(ctx context.Context, in *affiliation.PutEditProfileParams) (*models.UniqueIdentityNestedDataOutput, error)
	DeleteProfile(ctx context.Context, in *affiliation.DeleteProfileParams) (*models.TextStatusOutput, error)
	PostUnarchiveProfile(ctx context.Context, in *affiliation.PostUnarchiveProfileParams) (*models.UniqueIdentityNestedDataOutput, error)
	PostAddUniqueIdentity(ctx context.Context, in *affiliation.PostAddUniqueIdentityParams) (*models.UniqueIdentityNestedDataOutput, error)
	PostAddIdentity(ctx context.Context, in *affiliation.PostAddIdentityParams) (*models.UniqueIdentityNestedDataOutput, error)
	DeleteIdentity(ctx context.Context, in *affiliation.DeleteIdentityParams) (*models.TextStatusOutput, error)
	GetProfileEnrollments(ctx context.Context, in *affiliation.GetProfileEnrollmentsParams) (*models.GetProfileEnrollmentsDataOutput, error)
	PostAddEnrollment(ctx context.Context, in *affiliation.PostAddEnrollmentParams) (*models.UniqueIdentityNestedDataOutputNoDates, error)
	PutEditEnrollment(ctx context.Context, in *affiliation.PutEditEnrollmentParams) (*models.UniqueIdentityNestedDataOutput, error)
	PutEditEnrollmentByID(ctx context.Context, in *affiliation.PutEditEnrollmentByIDParams) (*models.UniqueIdentityNestedDataOutput, error)
	DeleteEnrollments(ctx context.Context, in *affiliation.DeleteEnrollmentsParams) (*models.UniqueIdentityNestedDataOutput, error)
	DeleteEnrollment(ctx context.Context, in *affiliation.DeleteEnrollmentParams) (*models.UniqueIdentityNestedDataOutput, error)
	PutMergeEnrollments(ctx context.Context, in *affiliation.PutMergeEnrollmentsParams) (*models.UniqueIdentityNestedDataOutput, error)
	PutMergeUniqueIdentities(ctx context.Context, in *affiliation.PutMergeUniqueIdentitiesParams) (*models.UniqueIdentityNestedDataOutput, error)
	PutMoveIdentity(ctx context.Context, in *affiliation.PutMoveIdentityParams) (*models.UniqueIdentityNestedDataOutput, error)
	GetUnaffiliated(ctx context.Context, in *affiliation.GetUnaffiliatedParams) (*models.GetUnaffiliatedOutput, error)
	TopContributorsParams(*affiliation.GetTopContributorsParams, *affiliation.GetTopContributorsCSVParams) (int64, int64, int64, int64, string, string, string)
	GetTopContributors(ctx context.Context, in *affiliation.GetTopContributorsParams) (*models.TopContributorsFlatOutput, error)
	GetTopContributorsCSV(ctx context.Context, in *affiliation.GetTopContributorsCSVParams) (io.ReadCloser, error)
	GetAllAffiliations(ctx context.Context, in *affiliation.GetAllAffiliationsParams) (*models.AllArrayOutput, error)
	PostBulkUpdate(ctx context.Context, in *affiliation.PostBulkUpdateParams) (*models.TextStatusOutput, error)
	PutMergeAll(ctx context.Context, in *affiliation.PutMergeAllParams) (*models.TextStatusOutput, error)
	PutHideEmails(ctx context.Context, in *affiliation.PutHideEmailsParams) (*models.TextStatusOutput, error)
	PutMapOrgNames(ctx context.Context, in *affiliation.PutMapOrgNamesParams) (*models.TextStatusOutput, error)
	PutDetAffRange(ctx context.Context, in *affiliation.PutDetAffRangeParams) (*models.TextStatusOutput, error)
	GetListProjects(ctx context.Context, in *affiliation.GetListProjectsParams) (*models.ListProjectsOutput, error)
	SetServiceRequestID(requestID string)
	GetServiceRequestID() string

	// Internal methods
	getPemCert(*jwt.Token, string) (string, error)
	checkToken(string) (string, bool, error)
	checkTokenAndPermission(interface{}) (string, string, string, error)
	toNoDates(*models.UniqueIdentityNestedDataOutput) *models.UniqueIdentityNestedDataOutputNoDates
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
	shDBGitdm shdb.Service
	es        elastic.Service
}

// New is a simple helper function to create a service instance
func New(apiDB apidb.Service, shDBAPI, shDBGitdm shdb.Service, es elastic.Service) Service {
	return &service{
		apiDB:     apiDB,
		shDB:      shDBAPI,
		shDBGitdm: shDBGitdm,
		es:        es,
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
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "getPemCert")
		return cert, err
	}
	defer resp.Body.Close()
	var jwks = Jwks{}
	err = json.NewDecoder(resp.Body).Decode(&jwks)
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "getPemCert")
		return cert, err
	}
	for k := range jwks.Keys {
		if token.Header["kid"] == jwks.Keys[k].Kid {
			cert = "-----BEGIN CERTIFICATE-----\n" + jwks.Keys[k].X5c[0] + "\n-----END CERTIFICATE-----"
		}
	}
	if cert == "" {
		err := errs.Wrap(errs.New(fmt.Errorf("Unable to find appropriate key."), errs.ErrServerError), "getPemCert")
		return cert, err
	}
	return cert, nil
}

func (s *service) checkToken(tokenStr string) (username string, agw bool, err error) {
	if !strings.HasPrefix(tokenStr, "Bearer ") {
		err = fmt.Errorf("Authorization header should start with 'Bearer '")
		err = errs.Wrap(errs.New(err, errs.ErrUnauthorized), "checkToken")
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
			err = errs.Wrap(errs.New(err, errs.ErrUnauthorized), "jwt.ParseWithClaims")
			return nil, err
		}
		cert, err := jwt.ParseRSAPublicKeyFromPEM([]byte(certStr))
		if err != nil {
			err = errs.Wrap(errs.New(err, errs.ErrUnauthorized), "jwt.ParseRSAPublicKeyFromPEM")
			return nil, err
		}
		return cert, nil
	})
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrUnauthorized), "checkToken")
		return
	}
	if !token.Valid {
		err = fmt.Errorf("invalid token")
		err = errs.Wrap(errs.New(err, errs.ErrUnauthorized), "checkToken")
		return
	}
	checkIss := token.Claims.(jwt.MapClaims).VerifyIssuer(auth0Domain, true)
	if !checkIss {
		checkIss := token.Claims.(jwt.MapClaims).VerifyIssuer("https://linuxfoundation.auth0.com/", true)
		if !checkIss {
			err = fmt.Errorf("invalid issuer: '%s' != '%s'", token.Claims.(jwt.MapClaims)["iss"], auth0Domain)
			err = errs.Wrap(errs.New(err, errs.ErrUnauthorized), "checkToken")
			return
		}
		agw = true
	}
	aud := os.Getenv("AUTH0_CLIENT_ID")
	checkAud := token.Claims.(jwt.MapClaims).VerifyAudience(aud, true)
	if !checkAud {
		checkAud = token.Claims.(jwt.MapClaims).VerifyAudience("https://api-gw.platform.linuxfoundation.org/", true)
		if !checkAud {
			checkAud = token.Claims.(jwt.MapClaims).VerifyAudience("https://api-gw.test.platform.linuxfoundation.org/", true)
		}
		if !checkAud {
			err = fmt.Errorf("invalid audience: '%s' != '%s'", token.Claims.(jwt.MapClaims)["aud"], aud)
			err = errs.Wrap(errs.New(err, errs.ErrUnauthorized), "checkToken")
			return
		}
		agw = true
	}
	if agw {
		username = "internal-api-user"
	} else {
		ucl := os.Getenv("AUTH0_USERNAME_CLAIM")
		iusername, ok := token.Claims.(jwt.MapClaims)[ucl]
		if !ok {
			err = fmt.Errorf("invalid user name claim: '%s', not present in %+v", ucl, token.Claims.(jwt.MapClaims))
			err = errs.Wrap(errs.New(err, errs.ErrUnauthorized), "checkToken")
			return
		}
		username, ok = iusername.(string)
		if !ok {
			err = fmt.Errorf("invalid user name: '%+v': is not string", iusername)
			err = errs.Wrap(errs.New(err, errs.ErrUnauthorized), "checkToken")
			return
		}
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
	case *affiliation.PutEditEnrollmentByIDParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "PutEditEnrollmentByID"
	case *affiliation.DeleteEnrollmentsParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "DeleteEnrollments"
	case *affiliation.DeleteEnrollmentParams:
		auth = params.Authorization
		project = params.ProjectSlug
		apiName = "DeleteEnrollment"
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
		if params.Authorization != nil {
			auth = *params.Authorization
		}
		project = params.ProjectSlug
		apiName = "GetTopContributors"
	case *affiliation.GetTopContributorsCSVParams:
		if params.Authorization != nil {
			auth = *params.Authorization
		}
		project = params.ProjectSlug
		apiName = "GetTopContributorsCSV"
	case *affiliation.PutMergeAllParams:
		auth = params.Authorization
		apiName = "PutMergeAll"
	case *affiliation.PutHideEmailsParams:
		auth = params.Authorization
		apiName = "PutHideEmails"
	case *affiliation.PutMapOrgNamesParams:
		auth = params.Authorization
		apiName = "PutMapOrgNames"
	case *affiliation.PutDetAffRangeParams:
		auth = params.Authorization
		apiName = "PutDetAffRange"
	case *affiliation.GetListProjectsParams:
		auth = params.Authorization
		apiName = "GetListProjects"
	case *affiliation.GetAllAffiliationsParams:
		auth = params.Authorization
		apiName = "GetAllAffiliations"
	case *affiliation.PostBulkUpdateParams:
		auth = params.Authorization
		apiName = "PostBulkUpdate"
	default:
		err = errs.Wrap(errs.New(fmt.Errorf("unknown params type"), errs.ErrServerError), "checkTokenAndPermission")
		return
	}
	project = strings.Replace(project, "/projects/", "", -1)
	// Validate JWT token, final outcome is the LFID of current authorized user
	agw := false
	username, agw, err = s.checkToken(auth)
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrUnauthorized), apiName+": checkTokenAndPermission")
		return
	}
	if !agw {
		// Check if that user can manage identities for given project/scope
		var allowed bool
		allowed, err = s.apiDB.CheckIdentityManagePermission(username, project, nil)
		if err != nil {
			err = errs.Wrap(errs.New(err, errs.ErrUnauthorized), apiName+": checkTokenAndPermission")
			return
		}
		if !allowed {
			err = errs.Wrap(errs.New(fmt.Errorf("user '%s' is not allowed to manage identities in '%s'", username, project), errs.ErrUnauthorized), apiName+": checkTokenAndPermission")
			return
		}
	}
	return
}

func (s *service) toNoDates(in *models.UniqueIdentityNestedDataOutput) (out *models.UniqueIdentityNestedDataOutputNoDates) {
	out = &models.UniqueIdentityNestedDataOutputNoDates{
		UUID:         in.UUID,
		Profile:      in.Profile,
		Identities:   in.Identities,
		LastModified: in.LastModified,
	}
	/*
		emptyStart := false
		emptyEnd := false
		if start == nil || (start != nil && time.Time(*start) == shared.MinPeriodDate) {
			emptyStart = true
		}
		if end == nil || (end != nil && time.Time(*end) == shared.MaxPeriodDate) {
			emptyEnd = true
		}
	*/
	for _, enrollment := range in.Enrollments {
		enrollmentStart := enrollment.Start.String()
		enrollmentEnd := enrollment.End.String()
		//if emptyStart && time.Time(enrollment.Start) == shared.MinPeriodDate {
		if time.Time(enrollment.Start) == shared.MinPeriodDate {
			enrollmentStart = ""
		}
		//if emptyEnd && time.Time(enrollment.End) == shared.MaxPeriodDate {
		if time.Time(enrollment.End) == shared.MaxPeriodDate {
			enrollmentEnd = ""
		}
		out.Enrollments = append(
			out.Enrollments,
			&models.EnrollmentNestedDataOutputNoDates{
				ID:             enrollment.ID,
				UUID:           enrollment.UUID,
				Start:          enrollmentStart,
				End:            enrollmentEnd,
				OrganizationID: enrollment.OrganizationID,
				Organization:   enrollment.Organization,
				ProjectSlug:    enrollment.ProjectSlug,
			},
		)
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
		err = errs.Wrap(err, apiName)
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	organization, err = s.shDB.AddOrganization(
		&models.OrganizationDataOutput{
			Name: orgName,
		},
		true,
		nil,
	)
	if err != nil {
		err = errs.Wrap(err, apiName)
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
	defer func() { s.shDB.NotifySSAW() }()
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
		err = errs.Wrap(err, apiName)
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	uniqueIdentity, err = s.shDB.AddNestedUniqueIdentity(uuid)
	if err != nil {
		err = errs.Wrap(err, apiName)
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	err = s.shDB.DeleteIdentity(id, false, true, nil, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
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
	defer func() { s.shDB.NotifySSAW() }()
	if err != nil {
		return
	}
	// Do the actual API call
	uid, err = s.shDB.AddNestedIdentity(identity)
	if err != nil {
		err = errs.Wrap(err, apiName)
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
// is_project_specific - optional query parameter, if set - enrollment will be marked as {projectSlug} specific (its "project_slug" column will be {projectSlug}
//   else enrollment will be global (its "project_slug" column will be set to null)
func (s *service) PostAddEnrollment(ctx context.Context, params *affiliation.PostAddEnrollmentParams) (uidnd *models.UniqueIdentityNestedDataOutputNoDates, err error) {
	enrollment := &models.EnrollmentDataOutput{UUID: params.UUID}
	organization := &models.OrganizationDataOutput{Name: params.OrgName}
	uid := &models.UniqueIdentityNestedDataOutput{}
	uidnd = &models.UniqueIdentityNestedDataOutputNoDates{}
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
		err = errs.Wrap(err, apiName)
		return
	}
	enrollment.OrganizationID = organization.ID
	uniqueIdentity := &models.UniqueIdentityDataOutput{}
	uniqueIdentity, err = s.shDB.GetUniqueIdentity(params.UUID, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	_, err = s.shDB.GetProfile(params.UUID, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if params.Start != nil {
		enrollment.Start = *(params.Start)
	} else {
		enrollment.Start = strfmt.DateTime(shared.MinPeriodDate)
	}
	if params.End != nil {
		enrollment.End = *(params.End)
	} else {
		enrollment.End = strfmt.DateTime(shared.MaxPeriodDate)
	}
	if params.IsProjectSpecific != nil && *(params.IsProjectSpecific) {
		enrollment.ProjectSlug = &project
	}
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	_, err = s.shDB.AddEnrollment(enrollment, false, false, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if params.Merge != nil && *(params.Merge) {
		err = s.shDB.MergeEnrollments(uniqueIdentity, organization, enrollment.ProjectSlug, false, nil)
		if err != nil {
			err = errs.Wrap(err, apiName)
			return
		}
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+params.UUID, 1, 1, false, project, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errs.Wrap(fmt.Errorf("Profile with UUID '%s' not found", params.UUID), apiName)
		return
	}
	uid = ary[0]
	uidnd = s.toNoDates(uid)
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
// is_project_specific - optional query parameter, if set - enrollment specific to this project will be edited
//   else global enrollment will be edited
// new_is_project_specific - ooptional query parameter, if set - will update is_project_specific value
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
		err = errs.Wrap(err, apiName)
		return
	}
	enrollment.OrganizationID = organization.ID
	uniqueIdentity := &models.UniqueIdentityDataOutput{}
	uniqueIdentity, err = s.shDB.GetUniqueIdentity(params.UUID, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	_, err = s.shDB.GetProfile(params.UUID, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if params.IsProjectSpecific != nil && *(params.IsProjectSpecific) {
		enrollment.ProjectSlug = &project
	}
	columns := []string{"uuid", "organization_id", "project_slug"}
	values := []interface{}{params.UUID, enrollment.OrganizationID, enrollment.ProjectSlug}
	isDates := []bool{false, false, false}
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
		err = errs.Wrap(
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
		enrollment.Start = strfmt.DateTime(shared.MinPeriodDate)
	}
	if params.NewEnd != nil {
		enrollment.End = *(params.NewEnd)
	} else {
		enrollment.End = strfmt.DateTime(shared.MaxPeriodDate)
	}
	if params.NewIsProjectSpecific != nil {
		if *(params.NewIsProjectSpecific) == true {
			enrollment.ProjectSlug = &project
		} else {
			enrollment.ProjectSlug = nil
		}
	}
	defer func() { s.shDB.NotifySSAW() }()
	_, err = s.shDB.EditEnrollment(enrollment, false, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if params.Merge != nil && *(params.Merge) {
		err = s.shDB.MergeEnrollments(uniqueIdentity, organization, enrollment.ProjectSlug, false, nil)
		if err != nil {
			err = errs.Wrap(err, apiName)
			return
		}
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+params.UUID, 1, 1, false, project, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errs.Wrap(fmt.Errorf("Profile with UUID '%s' not found", params.UUID), apiName)
		return
	}
	uid = ary[0]
	return
}

// PutEditEnrollmentByID: API params:
// /v1/affiliation/{projectSlug}/edit_enrollment_by_id/{enrollment_id}
// {projectSlug} - required path parameter: project to edit enrollment for (project slug URL encoded, can be prefixed with "/projects/")
// {enrollment_id} - required path parameter: Enrollment ID to edit
// new_start - optional query parameter: new enrollment start date, 1900-01-01 if not set
// new_end - optional query parameter: new enrollment end date, 2100-01-01 if not set
// merge - optional query parameter: if set it will merge enrollment dates for organization edited
// new_is_project_specific - ooptional query parameter, if set - will update is_project_specific value
func (s *service) PutEditEnrollmentByID(ctx context.Context, params *affiliation.PutEditEnrollmentByIDParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	enrollment := &models.EnrollmentDataOutput{ID: params.EnrollmentID}
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("PutEditEnrollmentByID: enrollment_id:%d enrollment:%+v uid:%+v", params.EnrollmentID, enrollment, s.ToLocalNestedUniqueIdentity(uid)))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PutEditEnrollmentByID(exit): enrollment_id:%d enrollment:%+v apiName:%s project:%s username:%s uid:%+v err:%v",
				params.EnrollmentID,
				enrollment,
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
	enrollment, err = s.shDB.GetEnrollment(enrollment.ID, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if enrollment.ProjectSlug != nil && *enrollment.ProjectSlug != project {
		err = errs.Wrap(
			fmt.Errorf(
				"cannot edit '%s' project enrollment: current project is '%s'",
				*enrollment.ProjectSlug,
				project,
			),
			apiName,
		)
		return
	}
	if params.NewStart != nil {
		enrollment.Start = *(params.NewStart)
	} else {
		enrollment.Start = strfmt.DateTime(shared.MinPeriodDate)
	}
	if params.NewEnd != nil {
		enrollment.End = *(params.NewEnd)
	} else {
		enrollment.End = strfmt.DateTime(shared.MaxPeriodDate)
	}
	if params.NewIsProjectSpecific != nil {
		if *(params.NewIsProjectSpecific) == true {
			enrollment.ProjectSlug = &project
		} else {
			enrollment.ProjectSlug = nil
		}
	}
	uuid := enrollment.UUID
	defer func() { s.shDB.NotifySSAW() }()
	_, err = s.shDB.EditEnrollment(enrollment, false, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if params.Merge != nil && *(params.Merge) {
		uniqueIdentity := &models.UniqueIdentityDataOutput{}
		uniqueIdentity, err = s.shDB.GetUniqueIdentity(uuid, true, nil)
		if err != nil {
			err = errs.Wrap(err, apiName)
			return
		}
		organization := &models.OrganizationDataOutput{}
		organization, err = s.shDB.GetOrganization(enrollment.OrganizationID, true, nil)
		if err != nil {
			err = errs.Wrap(err, apiName)
			return
		}
		err = s.shDB.MergeEnrollments(uniqueIdentity, organization, enrollment.ProjectSlug, false, nil)
		if err != nil {
			err = errs.Wrap(err, apiName)
			return
		}
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+uuid, 1, 1, false, project, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errs.Wrap(fmt.Errorf("Profile with UUID '%s' not found", uuid), apiName)
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
// is_project_specific - optional query parameter, if set - enrollemnt specific to this project will be deleted
//   else global enrollment will be deleted
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
		err = errs.Wrap(err, apiName)
		return
	}
	enrollment.OrganizationID = organization.ID
	_, err = s.shDB.GetUniqueIdentity(params.UUID, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	_, err = s.shDB.GetProfile(params.UUID, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if params.Start != nil {
		enrollment.Start = *(params.Start)
	} else {
		enrollment.Start = strfmt.DateTime(shared.MinPeriodDate)
	}
	if params.End != nil {
		enrollment.End = *(params.End)
	} else {
		enrollment.End = strfmt.DateTime(shared.MaxPeriodDate)
	}
	if params.IsProjectSpecific != nil && *(params.IsProjectSpecific) {
		enrollment.ProjectSlug = &project
	}
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	err = s.shDB.WithdrawEnrollment(enrollment, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+params.UUID, 1, 1, false, project, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errs.Wrap(fmt.Errorf("Profile with UUID '%s' not found", params.UUID), apiName)
		return
	}
	uid = ary[0]
	return
}

// DeleteEnrollment: API params:
// /v1/affiliation/{projectSlug}/delete_enrollment/{id}
// {projectSlug} - required path parameter: project to delete enrollments from (project slug URL encoded, can be prefixed with "/projects/")
// {id} - required path parameter: Enrollment ID to delete
func (s *service) DeleteEnrollment(ctx context.Context, params *affiliation.DeleteEnrollmentParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	enrollment := &models.EnrollmentDataOutput{ID: params.ID}
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("DeleteEnrollment: id:%d", params.ID))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteEnrollment(exit): id:%d enrollment:%+v apiName:%s project:%s username:%s uid:%+v err:%v",
				params.ID,
				enrollment,
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
	defer func() { s.shDB.NotifySSAW() }()
	var rols []*models.EnrollmentDataOutput
	rols, err = s.shDB.FindEnrollments([]string{"id"}, []interface{}{enrollment.ID}, []bool{false}, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	// Do the actual API call
	err = s.shDB.DeleteEnrollment(enrollment.ID, true, true, nil, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	enrollment = rols[0]
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+enrollment.UUID, 1, 1, false, project, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errs.Wrap(fmt.Errorf("Profile with UUID '%s' not found", enrollment.UUID), apiName)
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
// {orgName} - required path parameter: enrollment organization to merge
// is_project_specific - optional query parameter, if set - enrollment specific to project will be merged
//   else global enrollment will be merged
// all_projects - optional query parameter, if set all enrollments will be merged (global one and 0 or more project specific ones)
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
		err = errs.Wrap(err, apiName)
		return
	}
	enrollment.OrganizationID = organization.ID
	uniqueIdentity := &models.UniqueIdentityDataOutput{}
	uniqueIdentity, err = s.shDB.GetUniqueIdentity(params.UUID, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	_, err = s.shDB.GetProfile(params.UUID, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if params.IsProjectSpecific != nil && *(params.IsProjectSpecific) {
		enrollment.ProjectSlug = &project
	}
	allProjectSlugs := false
	if params.AllProjects != nil && *(params.AllProjects) {
		allProjectSlugs = true
	}
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	err = s.shDB.MergeEnrollments(uniqueIdentity, organization, enrollment.ProjectSlug, allProjectSlugs, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	if allProjectSlugs {
		ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+params.UUID, 1, 1, false, "", nil)
	} else {
		ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+params.UUID, 1, 1, false, project, nil)
	}
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errs.Wrap(fmt.Errorf("Profile with UUID '%s' not found", params.UUID), apiName)
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
		err = errs.Wrap(err, apiName)
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
		err = errs.Wrap(err, apiName)
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
func (s *service) PutEditProfile(ctx context.Context, params *affiliation.PutEditProfileParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	uuid := params.UUID
	profile := &models.ProfileDataOutput{
		UUID:        uuid,
		Name:        params.Name,
		Email:       params.Email,
		Gender:      params.Gender,
		GenderAcc:   params.GenderAcc,
		IsBot:       params.IsBot,
		CountryCode: params.CountryCode,
	}
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("PutEditProfile: uuid:%s profile:%+v", uuid, s.ToLocalProfile(profile)))
	// Check token and permission
	apiName, project, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PutEditProfile(exit): uuid:%s apiName:%s project:%s username:%s uid:%+v err:%v",
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	_, err = s.shDB.EditProfile(profile, true, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+uuid, 1, 1, false, project, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errs.Wrap(fmt.Errorf("Profile with UUID '%s' not found", uuid), apiName)
		return
	}
	uid = ary[0]
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	status, err = s.shDB.DeleteProfileNested(uuid, archive)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	return
}

// PostUnarchiveProfile: API params:
// /v1/affiliation/{projectSlug}/unarchive_profile/{uuid}[?archive=true]
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	uid, err = s.shDB.UnarchiveProfileNested(uuid)
	if err != nil {
		err = errs.Wrap(err, apiName)
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	status, err = s.shDB.DeleteOrganization(orgID)
	if err != nil {
		err = errs.Wrap(err, apiName)
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
		err = errs.Wrap(err, apiName)
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	postMatchingBlacklist, err = s.shDB.PostMatchingBlacklist(email)
	if err != nil {
		err = errs.Wrap(err, apiName)
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	status, err = s.shDB.DeleteMatchingBlacklist(email)
	if err != nil {
		err = errs.Wrap(err, apiName)
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	getListProfiles, err = s.shDB.GetListProfiles(q, rows, page, project)
	if err != nil {
		err = errs.Wrap(err, apiName)
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
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+uuid, 1, 1, false, project, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errs.Wrap(fmt.Errorf("Profile with UUID '%s' not found", uuid), apiName)
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
	enrollments, err = s.shDB.FindEnrollmentsNested([]string{"e.uuid"}, []interface{}{uuid}, []bool{false}, false, project, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(enrollments) == 0 {
		_, err = s.shDB.GetUniqueIdentity(uuid, true, nil)
		if err != nil {
			err = errs.Wrap(err, apiName)
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	putOrgDomain, err = s.shDB.PutOrgDomain(org, dom, overwrite, isTopDomain, skipEnrollments, project)
	if err != nil {
		err = errs.Wrap(err, apiName)
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	status, err = s.shDB.DeleteOrgDomain(org, dom)
	if err != nil {
		err = errs.Wrap(err, apiName)
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
		err = errs.Wrap(err, apiName)
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
func (s *service) PutMergeUniqueIdentities(ctx context.Context, params *affiliation.PutMergeUniqueIdentitiesParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
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
				"PutMergeUniqueIdentities(exit): fromUUID:%s toUUID:%s apiName:%s project:%s username:%s uid:%+v err:%v",
				fromUUID,
				toUUID,
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	esUUID := ""
	esIsBot := false
	esUUID, esIsBot, err = s.shDB.MergeUniqueIdentities(fromUUID, toUUID, archive)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+toUUID, 1, 1, false, project, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errs.Wrap(fmt.Errorf("Profile with UUID '%s' not found", toUUID), apiName)
		return
	}
	uid = ary[0]
	if esUUID != "" {
		go func() {
			s.es.UpdateByQuery("sds-*,-*-raw", "author_bot", esIsBot, "author_uuid", esUUID, true)
		}()
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
func (s *service) PutMoveIdentity(ctx context.Context, params *affiliation.PutMoveIdentityParams) (uid *models.UniqueIdentityNestedDataOutput, err error) {
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
				"PutMoveIdentity(exit): fromID:%s toUUID:%s apiName:%s project:%s username:%s uid:%+v err:%v",
				fromID,
				toUUID,
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
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	err = s.shDB.MoveIdentity(fromID, toUUID, archive)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.shDB.QueryUniqueIdentitiesNested("uuid="+toUUID, 1, 1, false, project, nil)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(ary) == 0 {
		err = errs.Wrap(fmt.Errorf("Profile with UUID '%s' not found", toUUID), apiName)
		return
	}
	uid = ary[0]
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
			err = errs.Wrap(err, apiName)
			return
		}
		getUnaffiliated.Unaffiliated, err = s.shDB.CheckUnaffiliated(getUnaffiliated.Unaffiliated, project, nil)
		if err != nil {
			getUnaffiliated.Unaffiliated = []*models.UnaffiliatedDataOutput{}
			err = errs.Wrap(err, apiName)
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

func (s *service) TopContributorsParams(params *affiliation.GetTopContributorsParams, paramsCSV *affiliation.GetTopContributorsCSVParams) (limit, offset, from, to int64, search, sortField, sortOrder string) {
	if params == nil {
		params = &affiliation.GetTopContributorsParams{
			From:      paramsCSV.From,
			To:        paramsCSV.To,
			Limit:     paramsCSV.Limit,
			Offset:    paramsCSV.Offset,
			Search:    paramsCSV.Search,
			SortField: paramsCSV.SortField,
			SortOrder: paramsCSV.SortOrder,
		}
	}
	if params.From != nil {
		from = *params.From
	} else {
		from = (time.Now().Add(-24 * 90 * time.Hour)).UnixNano() / 1.0e6
	}
	if params.To != nil {
		to = *params.To
	} else {
		to = time.Now().UnixNano() / 1.0e6
	}
	limit = 10
	if params.Limit != nil {
		limit = *params.Limit
		if limit < 1 {
			limit = 1
		}
		if limit > 9999 {
			limit = 9999
		}
	}
	if params.Offset != nil {
		offset = *params.Offset
		if offset < 0 {
			offset = 1
		}
	}
	if params.Search != nil {
		search = strings.ToLower(strings.TrimSpace(*params.Search))
	}
	if params.SortField != nil {
		sortField = strings.TrimSpace(*params.SortField)
	}
	if params.SortOrder != nil {
		sortOrder = strings.ToLower(strings.TrimSpace(*params.SortOrder))
	}
	return
}

// GetTopContributors: API params:
// /v1/affiliation/{projectSlug}/top_contributors?from=1552790984700&to=1552790984700][&limit=50][&offset=2][&search=john][&sort_field=gerrit_merged_changesets][&sort_order=desc]
// {projectSlug} - required path parameter: project to get top contributors stats (project slug URL encoded, can be prefixed with "/projects/")
// from - optional query parameter - milliseconds since 1970, for example 1552790984700, filter data from, default 90 days ago
// to - optional query parameter - milliseconds since 1970, for example 1552790984700, filter data to, default now
// limit - optional query parameter: page size, default 10
// offset - optional query parameter: offset in pages, specifying limit=10 and offset=2, you will get 20-30)
// search - optional query parameter: for example john, it can be specified in multiple forms (must be urlencoded)
//     empty - so search filter will be applied
//     john - will search using like '.*john.*' no case sensitive regexp pattern on author_org_name, author_name and author_uuids columns
//     all=john - will fetch list of all string type columns (per index pattern) and then search for '.*john.*' (case insensitive) on all of them
//     all=john,pamela,..,josh - will search for multiple values on all columns
//     col1,col2,...,colN=val1,val2,...,valM - will search for any of val1 - valM on all col1 - colN columns using N x M or conditions
// sort_field - optional query parameter: sort field for example gerrit_merged_changesets, can be fetched from "data_source_types" object returned per given project slug
//     if not specified API sorts contributors by count of documents related to their activity descending - so its most probable to get actual top contributors across all data sources defined for the project
//     if defined it must be one of fields returned in data source types fields object (from "data_source_types" object + special author_uuid) in that case API fetches contributors by this field first
//     and then once it has list of their UUIDs (after searching, sorting and paging) - it uses that list of UUIDs to get the remaining fields and finally merge results into one object.
// sort_order - optional query parameter: sort order allowed desc or asc, default is desc
//     when sorting asc (which is almost senseless) API only returns objects that have at least 1 document matching this sort criteria
//     so for example sort by git commits asc, will start from contributors having at least one commit, not 0).
func (s *service) GetTopContributors(ctx context.Context, params *affiliation.GetTopContributorsParams) (topContributors *models.TopContributorsFlatOutput, err error) {
	limit, offset, from, to, search, sortField, sortOrder := s.TopContributorsParams(params, nil)
	if to < from {
		err = fmt.Errorf("to parameter (%d) must be higher or equal from (%d)", to, from)
		return
	}
	topContributors = &models.TopContributorsFlatOutput{}
	log.Info(fmt.Sprintf("GetTopContributors: from:%d to:%d limit:%d offset:%d search:%s sortField:%s sortOrder:%s", from, to, limit, offset, search, sortField, sortOrder))
	// Check token and permission
	public := false
	apiName, project, username, e := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetTopContributors(exit): from:%d to:%d limit:%d offset:%d search:%s sortField:%s sortOrder:%s apiName:%s project:%s username:%s topContributors:%d public:%v err:%v",
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
				public,
				err,
			),
		)
	}()
	if e != nil {
		public = true
	}
	var dataSourceTypes []string
	dataSourceTypes, err = s.apiDB.GetDataSourceTypes(project)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}

	topContributors, err = s.es.GetTopContributors(project, dataSourceTypes, from, to, limit, offset, search, sortField, sortOrder)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(topContributors.Contributors) > 0 {
		err = s.shDB.EnrichContributors(topContributors.Contributors, project, to, nil)
		if err != nil {
			err = errs.Wrap(err, apiName)
			return
		}
	}
	if public {
		for i := range topContributors.Contributors {
			topContributors.Contributors[i].Email = ""
		}
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
	topContributors.Public = public
	return
}

// GetTopContributorsCSV: API params:
// /v1/affiliation/{projectSlug}/top_contributors_csv?from=1552790984700&to=1552790984700][&limit=50][&offset=2][&search=john][&sort_field=gerrit_merged_changesets][&sort_order=desc]
// {projectSlug} - required path parameter: project to get top contributors stats (project slug URL encoded, can be prefixed with "/projects/")
// from - optional query parameter - milliseconds since 1970, for example 1552790984700, filter data from, default 90 days ago
// to - optional query parameter - milliseconds since 1970, for example 1552790984700, filter data to, default now
// limit - optional query parameter: page size, default 10
// offset - optional query parameter: offset in pages, specifying limit=10 and offset=2, you will get 20-30)
// search - optional query parameter: for example john, it can be specified in multiple forms (must be urlencoded)
//     empty - so search filter will be applied
//     john - will search using like '.*john.*' no case sensitive regexp pattern on author_org_name, author_name and author_uuids columns
//     all=john - will fetch list of all string type columns (per index pattern) and then search for '.*john.*' (case insensitive) on all of them
//     all=john,pamela,..,josh - will search for multiple values on all columns
//     col1,col2,...,colN=val1,val2,...,valM - will search for any of val1 - valM on all col1 - colN columns using N x M or conditions
// sort_field - optional query parameter: sort field for example gerrit_merged_changesets, can be fetched from "data_source_types" object returned per given project slug
//     if not specified API sorts contributors by count of documents related to their activity descending - so its most probable to get actual top contributors across all data sources defined for the project
//     if defined it must be one of fields returned in data source types fields object (from "data_source_types" object + special author_uuid) in that case API fetches contributors by this field first
//     and then once it has list of their UUIDs (after searching, sorting and paging) - it uses that list of UUIDs to get the remaining fields and finally merge results into one object.
// sort_order - optional query parameter: sort order allowed desc or asc, default is desc
//     when sorting asc (which is almost senseless) API only returns objects that have at least 1 document matching this sort criteria
//     so for example sort by git commits asc, will start from contributors having at least one commit, not 0).
func (s *service) GetTopContributorsCSV(ctx context.Context, params *affiliation.GetTopContributorsCSVParams) (f io.ReadCloser, err error) {
	limit, offset, from, to, search, sortField, sortOrder := s.TopContributorsParams(nil, params)
	if to < from {
		err = fmt.Errorf("to parameter (%d) must be higher or equal from (%d)", to, from)
		return
	}
	topContributors := &models.TopContributorsFlatOutput{}
	log.Info(fmt.Sprintf("GetTopContributors: from:%d to:%d limit:%d offset:%d search:%s sortField:%s sortOrder:%s", from, to, limit, offset, search, sortField, sortOrder))
	// Check token and permission
	public := false
	apiName, project, username, e := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetTopContributors(exit): from:%d to:%d limit:%d offset:%d search:%s sortField:%s sortOrder:%s apiName:%s project:%s username:%s topContributors:%d public:%v err:%v",
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
				public,
				err,
			),
		)
	}()
	if e != nil {
		public = true
	}
	var dataSourceTypes []string
	dataSourceTypes, err = s.apiDB.GetDataSourceTypes(project)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	topContributors, err = s.es.GetTopContributors(project, dataSourceTypes, from, to, limit, offset, search, sortField, sortOrder)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	if len(topContributors.Contributors) > 0 {
		err = s.shDB.EnrichContributors(topContributors.Contributors, project, to, nil)
		if err != nil {
			err = errs.Wrap(err, apiName)
			return
		}
	}
	if public {
		for i := range topContributors.Contributors {
			topContributors.Contributors[i].Email = ""
		}
	}
	hdr := []string{
		"uuid",
		"name",
		"organization",
		"git_commits",
		"git_lines_of_code_added",
		"git_lines_of_code_changed",
		"git_lines_of_code_removed",
		"gerrit_changesets",
		"gerrit_merged_changesets",
		"gerrit_reviews_approved",
		"jira_comments",
		"jira_issues_created",
		"jira_issues_assigned",
		"jira_issues_closed",
		"jira_average_issues_open_days",
		"confluence_pages_created",
		"confluence_pages_edited",
		"confluence_blog_posts",
		"confluence_comments",
		"confluence_last_documentation",
		"confluence_date_since_last_documentation",
		"github_issue_issues_created",
		"github_issue_issues_assigned",
		"github_issue_average_time_open_days",
		"github_pull_request_prs_created",
		"github_pull_request_prs_merged",
		"github_pull_request_prs_open",
		"github_pull_request_prs_closed",
		"bugzilla_issues_created",
		"bugzilla_issues_closed",
		"bugzilla_issues_assigned",
	}
	if !public {
		hdr = append(hdr, "email")
	}
	buffer := &bytes.Buffer{}
	writer := csv.NewWriter(buffer)
	err = writer.Write(hdr)
	if err != nil {
		err = errs.Wrap(fmt.Errorf("error writing CSV header row: %+v: %+v", hdr, err), apiName)
		return
	}
	for index, contributor := range topContributors.Contributors {
		row := []string{
			contributor.UUID,
			contributor.Name,
			contributor.Organization,
			strconv.FormatInt(contributor.GitCommits, 10),
			strconv.FormatInt(contributor.GitLinesAdded, 10),
			strconv.FormatInt(contributor.GitLinesChanged, 10),
			strconv.FormatInt(contributor.GitLinesRemoved, 10),
			strconv.FormatInt(contributor.GerritChangesets, 10),
			strconv.FormatInt(contributor.GerritMergedChangesets, 10),
			strconv.FormatInt(contributor.GerritApprovals, 10),
			strconv.FormatInt(contributor.JiraComments, 10),
			strconv.FormatInt(contributor.JiraIssuesCreated, 10),
			strconv.FormatInt(contributor.JiraIssuesAssigned, 10),
			strconv.FormatInt(contributor.JiraIssuesClosed, 10),
			strconv.FormatFloat(contributor.JiraAverageIssueOpenDays, 'f', -1, 64),
			strconv.FormatInt(contributor.ConfluencePagesCreated, 10),
			strconv.FormatInt(contributor.ConfluencePagesEdited, 10),
			strconv.FormatInt(contributor.ConfluenceBlogPosts, 10),
			strconv.FormatInt(contributor.ConfluenceComments, 10),
			contributor.ConfluenceLastActionDate,
			strconv.FormatFloat(contributor.ConfluenceDaysSinceLastDocumentation, 'f', -1, 64),
			strconv.FormatInt(contributor.GithubIssueIssuesCreated, 10),
			strconv.FormatInt(contributor.GithubIssueIssuesAssigned, 10),
			strconv.FormatFloat(contributor.GithubIssueAverageTimeOpenDays, 'f', -1, 64),
			strconv.FormatInt(contributor.GithubPullRequestPrsCreated, 10),
			strconv.FormatInt(contributor.GithubPullRequestPrsMerged, 10),
			strconv.FormatInt(contributor.GithubPullRequestPrsOpen, 10),
			strconv.FormatInt(contributor.GithubPullRequestPrsClosed, 10),
			strconv.FormatInt(contributor.BugzillaIssuesCreated, 10),
			strconv.FormatInt(contributor.BugzillaIssuesClosed, 10),
			strconv.FormatInt(contributor.BugzillaIssuesAssigned, 10),
		}
		if !public {
			row = append(row, contributor.Email)
		}
		err = writer.Write(row)
		if err != nil {
			err = errs.Wrap(fmt.Errorf("error writing #%d/%+v row: %+v", index+1, row, err), apiName)
			return
		}
	}
	writer.Flush()
	f = ioutil.NopCloser(bytes.NewReader(buffer.Bytes()))
	return
}

// GetAllAffiliations: API params:
// /v1/affiliation/all
func (s *service) GetAllAffiliations(ctx context.Context, params *affiliation.GetAllAffiliationsParams) (all *models.AllArrayOutput, err error) {
	all = &models.AllArrayOutput{}
	log.Info("GetAllAffiliations")
	// Check token and permission
	apiName, _, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(fmt.Sprintf("GetAllAffiliations(exit): apiName:%s username:%s all:%d err:%v", apiName, username, len(all.Profiles), err))
	}()
	if err != nil {
		return
	}
	all, err = s.shDBGitdm.GetAllAffiliations()
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
	// Check token and permission
	apiName, _, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PostBulkUpdate(exit): add:%d del:%d apiName:%s username:%s added:%d deleted:%d updated:%d status:%s err:%v",
				len(params.Body.Add),
				len(params.Body.Del),
				apiName,
				username,
				nAdded,
				nDeleted,
				nUpdated,
				status.Text,
				err,
			),
		)
	}()
	if err != nil {
		return
	}
	nAdded, nDeleted, nUpdated, err = s.shDBGitdm.BulkUpdate(params.Body.Add, params.Body.Del)
	if err != nil {
		return
	}
	status.Text = fmt.Sprintf("Requested: Add: %d, Delete:%d, Done: Added: %d, Deleted: %d, Updated: %d", len(params.Body.Add), len(params.Body.Del), nAdded, nDeleted, nUpdated)
	return
}

// PutMergeAll: API
// ===========================================================================
// Find any identities with the same email belonging to different profiles
// Then merge such profiles
// ===========================================================================
// /v1/affiliation/merge_all:
func (s *service) PutMergeAll(ctx context.Context, params *affiliation.PutMergeAllParams) (status *models.TextStatusOutput, err error) {
	status = &models.TextStatusOutput{}
	log.Info("PutMergeAll")
	// Check token and permission
	apiName, _, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(fmt.Sprintf("PutMergeAll(exit): apiName:%s username:%s status:%s err:%v", apiName, username, status.Text, err))
	}()
	if err != nil {
		return
	}
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	stat := ""
	stat, err = s.shDB.MergeAll()
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	status.Text = stat
	return
}

// PutHideEmails: API
// ===========================================================================
// For all non-email columns on profiles and identities, if emails value is found
// name@doman - remove '@domain' part and leave only 'name'
// ===========================================================================
// /v1/affiliation/hide_emails:
func (s *service) PutHideEmails(ctx context.Context, params *affiliation.PutHideEmailsParams) (status *models.TextStatusOutput, err error) {
	status = &models.TextStatusOutput{}
	log.Info("PutHideEmails")
	// Check token and permission
	apiName, _, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(fmt.Sprintf("PutHideEmails(exit): apiName:%s username:%s status:%s err:%v", apiName, username, status.Text, err))
	}()
	if err != nil {
		return
	}
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	stat := ""
	stat, err = s.shDB.HideEmails()
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	status.Text = stat
	return
}

// PutMapOrgNames: API
// ===========================================================================
// common incorrect company names to correct ones using definitions from map_org_names.yaml
// example: from: Orange Business Services --> to: Orange S.A.
// 'from' is a regexp while 'to' is a final (correct) organization name.
// ===========================================================================
// /v1/affiliation/map_org_names:
func (s *service) PutMapOrgNames(ctx context.Context, params *affiliation.PutMapOrgNamesParams) (status *models.TextStatusOutput, err error) {
	status = &models.TextStatusOutput{}
	log.Info("PutMapOrgNames")
	// Check token and permission
	apiName, _, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(fmt.Sprintf("PutMapOrgNames(exit): apiName:%s username:%s status:%s err:%v", apiName, username, status.Text, err))
	}()
	if err != nil {
		return
	}
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	stat := ""
	stat, err = s.shDB.MapOrgNames()
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	status.Text = stat
	return
}

// PutDetAffRange: API
// ===========================================================================
// For all profiles that have a single company affiliation (in a given project or global)
// detect time range when ES has contributions and use that range to set start/end dates (if not already set)
// ===========================================================================
// /v1/affiliation/det_aff_range:
func (s *service) PutDetAffRange(ctx context.Context, params *affiliation.PutDetAffRangeParams) (status *models.TextStatusOutput, err error) {
	status = &models.TextStatusOutput{}
	log.Info("PutDetAffRange")
	// Check token and permission
	apiName, _, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(fmt.Sprintf("PutDetAffRange(exit): apiName:%s username:%s status:%s err:%v", apiName, username, status.Text, err))
	}()
	if err != nil {
		return
	}
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	subjects := []*models.EnrollmentProjectRange{}
	subjects, err = s.shDB.GetDetAffRangeSubjects()
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	updates := []*models.EnrollmentProjectRange{}
	updates, err = s.es.DetAffRange(subjects)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	stat := ""
	stat, err = s.shDB.UpdateAffRange(updates)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	status.Text = stat
	return
}

// GetListProjects: API
// ===========================================================================
// list projects given user has affiliations management access to
// user is determind from auth token.
// ===========================================================================
// /v1/affiliation/list_profiles:
func (s *service) GetListProjects(ctx context.Context, params *affiliation.GetListProjectsParams) (projects *models.ListProjectsOutput, err error) {
	projects = &models.ListProjectsOutput{}
	log.Info("GetListProjects")
	// Check token and permission
	apiName, _, username, err := s.checkTokenAndPermission(params)
	defer func() {
		log.Info(fmt.Sprintf("GetListProjects(exit): apiName:%s username:%s projetcs:%+v err:%v", apiName, username, projects, err))
	}()
	if err != nil {
		return
	}
	defer func() { s.shDB.NotifySSAW() }()
	// Do the actual API call
	projects, err = s.apiDB.GetListProjects(username)
	if err != nil {
		err = errs.Wrap(err, apiName)
		return
	}
	projects.User = username
	return
}
