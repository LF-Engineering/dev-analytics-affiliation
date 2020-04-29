package shared

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"database/sql"
	"encoding/json"

	"github.com/go-openapi/strfmt"
	"github.com/jmoiron/sqlx"

	"github.com/LF-Engineering/dev-analytics-affiliation/errs"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
)

const (
	// LogListMax - do not log lists longer than 30 elements, display list counts instead
	LogListMax = 30
)

var (
	// GSQLOut - if set displays all SQLs that are executed (if not set, only failed ones)
	GSQLOut bool
)

// ServiceInterface - Shared API interface
type ServiceInterface interface {
	// Formatting data for logs
	ToLocalOrganizations([]*models.OrganizationDataOutput) []interface{}
	ToLocalDomains([]*models.DomainDataOutput) []interface{}
	ToLocalNestedOrganizations([]*models.OrganizationNestedDataOutput) []interface{}
	ToLocalNestedUniqueIdentities([]*models.UniqueIdentityNestedDataOutput) []interface{}
	ToLocalNestedUniqueIdentity(*models.UniqueIdentityNestedDataOutput) interface{}
	ToLocalNestedEnrollments([]*models.EnrollmentNestedDataOutput) []interface{}
	ToLocalMatchingBlacklist([]*models.MatchingBlacklistOutput) []interface{}
	ToLocalUnaffiliatedObj(*models.GetUnaffiliatedOutput) []interface{}
	ToLocalUnaffiliated([]*models.UnaffiliatedDataOutput) []interface{}
	ToLocalProfile(*models.ProfileDataOutput) *LocalProfile
	ToLocalProfiles([]*models.ProfileDataOutput) []*LocalProfile
	ToLocalIdentity(*models.IdentityDataOutput) *LocalIdentity
	ToLocalIdentities([]*models.IdentityDataOutput) []*LocalIdentity
	ToLocalUniqueIdentity(*models.UniqueIdentityDataOutput) *LocalUniqueIdentity
	ToLocalOrganization(*models.OrganizationDataOutput) *LocalOrganization
	ToLocalEnrollments([]*models.EnrollmentDataOutput) []interface{}
	ToLocalTopContributorsFlatObj(*models.TopContributorsFlatOutput) []interface{}
	ToLocalTopContributorsFlat([]*models.ContributorFlatStats) []interface{}
	// shared DB functions
	QueryOut(string, ...interface{})
	QueryDB(*sqlx.DB, string, ...interface{}) (*sql.Rows, error)
	QueryTX(*sql.Tx, string, ...interface{}) (*sql.Rows, error)
	Query(*sqlx.DB, *sql.Tx, string, ...interface{}) (*sql.Rows, error)
	ExecDB(*sqlx.DB, string, ...interface{}) (sql.Result, error)
	ExecTX(*sql.Tx, string, ...interface{}) (sql.Result, error)
	Exec(*sqlx.DB, *sql.Tx, string, ...interface{}) (sql.Result, error)
	// Other utils
	Now() *strfmt.DateTime
	TimeParseAny(string) (time.Time, error)
	JSONEscape(string) string
	SanitizeShortProfile(*models.AllOutput, bool)
	SanitizeShortIdentity(*models.IdentityShortOutput, bool)
	SanitizeShortEnrollment(*models.EnrollmentShortOutput)
	SanitizeIdentity(*models.IdentityDataOutput)
	SanitizeProfile(*models.ProfileDataOutput)
}

// ServiceStruct - Shared API Struct
type ServiceStruct struct {
}

// LocalProfile - to display data inside pointers
type LocalProfile struct {
	*models.ProfileDataOutput
}

// LocalIdentity - to display data inside pointers
type LocalIdentity struct {
	*models.IdentityDataOutput
}

// LocalUniqueIdentity - to display data inside pointers
type LocalUniqueIdentity struct {
	*models.UniqueIdentityDataOutput
}

// LocalOrganization - to display data inside pointers
type LocalOrganization struct {
	*models.OrganizationDataOutput
}

// LocalEnrollmentShortOutput - embed to add Sortkey() method
type LocalEnrollmentShortOutput struct {
	*models.EnrollmentShortOutput
}

// LocalIdentityShortOutput - embed to add Sortkey() method
type LocalIdentityShortOutput struct {
	*models.IdentityShortOutput
}

// LocalAllOutput - embed to add Sortkey() method
type LocalAllOutput struct {
	*models.AllOutput
}

// SortKey - defines sort order for enrollments
func (e *LocalEnrollmentShortOutput) SortKey() string {
	return e.Start + ":" + e.End + ":" + e.Organization
}

// SortKey - defines sort order for enrollments
func (i *LocalIdentityShortOutput) SortKey() (key string) {
	key = i.Source
	if i.Name != nil {
		key += ":" + *(i.Name)
	} else {
		key += ":"
	}
	if i.Email != nil {
		key += ":" + *(i.Email)
	} else {
		key += ":"
	}
	if i.Username != nil {
		key += ":" + *(i.Username)
	} else {
		key += ":"
	}
	return
}

// SortKey - defines sort order for enrollments
func (a *LocalAllOutput) SortKey(recursive bool) (key string) {
	if a.Name != nil {
		key += *(a.Name)
	}
	if a.Email != nil {
		key += ":" + *(a.Email)
	} else {
		key += ":"
	}
	if a.CountryCode != nil {
		key += ":" + strings.ToLower(*(a.CountryCode))
	} else {
		key += ":"
	}
	if a.Gender != nil {
		key += ":" + *(a.Gender)
	} else {
		key += ":"
	}
	if a.IsBot != nil {
		if *(a.IsBot) == 0 {
			key += ":0"
		} else {
			key += ":1"
		}
	} else {
		key += ":"
	}
	if !recursive {
		key = strings.ToLower(key)
		return
	}
	for _, identity := range a.Identities {
		a := &LocalIdentityShortOutput{IdentityShortOutput: identity}
		key += ":" + a.SortKey()
	}
	for _, enrollment := range a.Enrollments {
		a := &LocalEnrollmentShortOutput{EnrollmentShortOutput: enrollment}
		key += ":" + a.SortKey()
	}
	return
}

// SanitizeIdentity - trim white spaces
func (s *ServiceStruct) SanitizeIdentity(identity *models.IdentityDataOutput) {
	identity.ID = strings.TrimSpace(identity.ID)
	identity.Source = strings.TrimSpace(identity.Source)
	if identity.UUID != nil {
		uuid := strings.TrimSpace(*(identity.UUID))
		identity.UUID = &uuid
	}
	if identity.Email != nil {
		email := strings.TrimSpace(*(identity.Email))
		identity.Email = &email
	}
	if identity.Name != nil {
		name := strings.TrimSpace(*(identity.Name))
		identity.Name = &name
	}
	if identity.Username != nil {
		username := strings.TrimSpace(*(identity.Username))
		identity.Username = &username
	}
}

// SanitizeShortIdentity - trim white spaces and email @/!
func (s *ServiceStruct) SanitizeShortIdentity(identity *models.IdentityShortOutput, atToExcl bool) {
	from := "@"
	to := "!"
	if !atToExcl {
		from, to = to, from
	}
	identity.Source = strings.TrimSpace(identity.Source)
	if identity.Email != nil {
		email := strings.TrimSpace(strings.Replace(*(identity.Email), from, to, -1))
		identity.Email = &email
	}
	if identity.Name != nil {
		name := strings.TrimSpace(*(identity.Name))
		identity.Name = &name
	}
	if identity.Username != nil {
		username := strings.TrimSpace(*(identity.Username))
		identity.Username = &username
	}
}

// SanitizeShortEnrollment - trim white spaces
func (s *ServiceStruct) SanitizeShortEnrollment(enrollment *models.EnrollmentShortOutput) {
	enrollment.Organization = strings.TrimSpace(enrollment.Organization)
	enrollment.Start = strings.TrimSpace(enrollment.Start)
	enrollment.End = strings.TrimSpace(enrollment.End)
}

// SanitizeProfile - trim white spaces
func (s *ServiceStruct) SanitizeProfile(prof *models.ProfileDataOutput) {
	prof.UUID = strings.TrimSpace(prof.UUID)
	if prof.Email != nil {
		email := strings.TrimSpace(*(prof.Email))
		prof.Email = &email
	}
	if prof.Name != nil {
		name := strings.TrimSpace(*(prof.Name))
		prof.Name = &name
	}
	if prof.Gender != nil {
		gender := strings.TrimSpace(*(prof.Gender))
		prof.Gender = &gender
	}
	if prof.CountryCode != nil {
		countryCode := strings.TrimSpace(*(prof.CountryCode))
		prof.CountryCode = &countryCode
	}
}

// SanitizeShortProfile - trim white spaces, email @/! and dependent objects
func (s *ServiceStruct) SanitizeShortProfile(prof *models.AllOutput, atToExcl bool) {
	from := "@"
	to := "!"
	if !atToExcl {
		from, to = to, from
	}
	if prof.Email != nil {
		email := strings.TrimSpace(strings.Replace(*(prof.Email), from, to, -1))
		prof.Email = &email
	}
	if prof.Name != nil {
		name := strings.TrimSpace(*(prof.Name))
		prof.Name = &name
	}
	if prof.Gender != nil {
		gender := strings.TrimSpace(*(prof.Gender))
		prof.Gender = &gender
	}
	if prof.CountryCode != nil {
		countryCode := strings.TrimSpace(*(prof.CountryCode))
		prof.CountryCode = &countryCode
	}
	for _, identity := range prof.Identities {
		s.SanitizeShortIdentity(identity, atToExcl)
	}
	for _, enrollment := range prof.Enrollments {
		s.SanitizeShortEnrollment(enrollment)
	}
}

func (p *LocalProfile) String() (s string) {
	s = "{UUID:" + p.UUID + ","
	if p.Name == nil {
		s += "Name:nil,"
	} else {
		s += "Name:" + *p.Name + ","
	}
	if p.Email == nil {
		s += "Email:nil,"
	} else {
		s += "Email:" + *p.Email + ","
	}
	if p.Gender == nil {
		s += "Gender:nil,"
	} else {
		s += "Gender:" + *p.Gender + ","
	}
	if p.GenderAcc == nil {
		s += "GenderAcc:nil,"
	} else {
		s += "GenderAcc:" + strconv.FormatInt(*p.GenderAcc, 10) + ","
	}
	if p.IsBot == nil {
		s += "IsBot:nil,"
	} else {
		s += "IsBot:" + strconv.FormatInt(*p.IsBot, 10) + ","
	}
	if p.CountryCode == nil {
		s += "CountryCode:nil}"
	} else {
		s += "CountryCode:" + *p.CountryCode + "}"
	}
	return
}

func (p *LocalIdentity) String() (s string) {
	s = "{ID:" + p.ID + ",Source:" + p.Source + ","
	if p.UUID == nil {
		s += "UUID:nil,"
	} else {
		s += "UUID:" + *p.UUID + ","
	}
	if p.Name == nil {
		s += "Name:nil,"
	} else {
		s += "Name:" + *p.Name + ","
	}
	if p.Username == nil {
		s += "Username:nil,"
	} else {
		s += "Username:" + *p.Username + ","
	}
	if p.Email == nil {
		s += "Email:nil,"
	} else {
		s += "Email:" + *p.Email + ","
	}
	if p.LastModified == nil {
		s += "LastModified:nil}"
	} else {
		s += fmt.Sprintf("LastModified:%+v}", *p.LastModified)
	}
	return
}

func (p *LocalUniqueIdentity) String() (s string) {
	s = "{UUID:" + p.UUID + ","
	if p.LastModified == nil {
		s += "LastModified:nil}"
	} else {
		s += fmt.Sprintf("LastModified:%+v}", *p.LastModified)
	}
	return
}

func (p *LocalOrganization) String() (s string) {
	s = fmt.Sprintf("{ID:%d,Name:%s}", p.ID, p.Name)
	return
}

// ToLocalDomains - to display values inside pointers
func (s *ServiceStruct) ToLocalDomains(ia []*models.DomainDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

// ToLocalOrganizations - to display values inside pointers
func (s *ServiceStruct) ToLocalOrganizations(ia []*models.OrganizationDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

// ToLocalNestedOrganizations - to display values inside pointers
func (s *ServiceStruct) ToLocalNestedOrganizations(ia []*models.OrganizationNestedDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		ary := []interface{}{i.ID, i.Name}
		ary2 := []interface{}{}
		for _, d := range i.Domains {
			if d != nil {
				ary2 = append(ary2, *d)
			}
		}
		if len(ary2) > 0 {
			ary = append(ary, ary2)
		}
		oa = append(oa, ary)
	}
	return
}

// ToLocalNestedUniqueIdentity - to display values inside pointers
func (s *ServiceStruct) ToLocalNestedUniqueIdentity(i *models.UniqueIdentityNestedDataOutput) (o interface{}) {
	if i == nil {
		return i
	}
	m := map[string]interface{}{
		"UUID": i.UUID,
	}
	if i.LastModified == nil {
		m["LastModified"] = nil
	} else {
		m["LastModified"] = *(i.LastModified)
	}
	m["Profile"] = s.ToLocalProfile(i.Profile)
	m["Identities"] = s.ToLocalIdentities(i.Identities)
	m["Enrollments"] = s.ToLocalNestedEnrollments(i.Enrollments)
	return m
}

// ToLocalNestedUniqueIdentities - to display values inside pointers
func (s *ServiceStruct) ToLocalNestedUniqueIdentities(ia []*models.UniqueIdentityNestedDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		m := map[string]interface{}{
			"UUID": i.UUID,
		}
		if i.LastModified == nil {
			m["LastModified"] = nil
		} else {
			m["LastModified"] = *(i.LastModified)
		}
		m["Profile"] = s.ToLocalProfile(i.Profile)
		m["Identities"] = s.ToLocalIdentities(i.Identities)
		m["Enrollments"] = s.ToLocalNestedEnrollments(i.Enrollments)
		oa = append(oa, m)
	}
	return
}

// ToLocalMatchingBlacklist - to display values inside pointers
func (s *ServiceStruct) ToLocalMatchingBlacklist(ia []*models.MatchingBlacklistOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

// ToLocalTopContributorsFlatObj - to display values inside pointers
func (s *ServiceStruct) ToLocalTopContributorsFlatObj(ia *models.TopContributorsFlatOutput) (oa []interface{}) {
	for _, i := range ia.Contributors {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

// ToLocalTopContributorsFlat - to display values inside pointers
func (s *ServiceStruct) ToLocalTopContributorsFlat(ia []*models.ContributorFlatStats) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

// ToLocalUnaffiliatedObj - to display values inside pointers
func (s *ServiceStruct) ToLocalUnaffiliatedObj(ia *models.GetUnaffiliatedOutput) (oa []interface{}) {
	for _, i := range ia.Unaffiliated {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

// ToLocalUnaffiliated - to display values inside pointers
func (s *ServiceStruct) ToLocalUnaffiliated(ia []*models.UnaffiliatedDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

// ToLocalNestedEnrollments - to display values inside pointers
func (s *ServiceStruct) ToLocalNestedEnrollments(ia []*models.EnrollmentNestedDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		m := map[string]interface{}{
			"ID":             i.ID,
			"UUID":           i.UUID,
			"Start":          i.Start,
			"End":            i.End,
			"OrganizationID": i.OrganizationID,
		}
		if i.Organization != nil {
			m["Organization"] = *(i.Organization)
		}
		oa = append(oa, m)
	}
	return
}

// ToLocalEnrollments - to display values inside pointers
func (s *ServiceStruct) ToLocalEnrollments(ia []*models.EnrollmentDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

// ToLocalProfile - to display values inside pointers
func (s *ServiceStruct) ToLocalProfile(i *models.ProfileDataOutput) (o *LocalProfile) {
	if i == nil {
		return
	}
	o = &LocalProfile{i}
	return
}

// ToLocalProfiles - to display values inside pointers
func (s *ServiceStruct) ToLocalProfiles(ia []*models.ProfileDataOutput) (oa []*LocalProfile) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, &LocalProfile{i})
	}
	return
}

// ToLocalIdentity - to display values inside pointers
func (s *ServiceStruct) ToLocalIdentity(i *models.IdentityDataOutput) (o *LocalIdentity) {
	if i == nil {
		return
	}
	o = &LocalIdentity{i}
	return
}

// ToLocalIdentities - to display values inside pointers
func (s *ServiceStruct) ToLocalIdentities(ia []*models.IdentityDataOutput) (oa []*LocalIdentity) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, &LocalIdentity{i})
	}
	return
}

// ToLocalUniqueIdentity - to display values inside pointers
func (s *ServiceStruct) ToLocalUniqueIdentity(i *models.UniqueIdentityDataOutput) (o *LocalUniqueIdentity) {
	if i == nil {
		return
	}
	o = &LocalUniqueIdentity{i}
	return
}

// ToLocalOrganization - to display values inside pointers
func (s *ServiceStruct) ToLocalOrganization(i *models.OrganizationDataOutput) (o *LocalOrganization) {
	if i == nil {
		return
	}
	o = &LocalOrganization{i}
	return
}

// Now - return date  now
func (s *ServiceStruct) Now() *strfmt.DateTime {
	n := strfmt.DateTime(time.Now())
	return &n
}

// TimeParseAny - parse time from string
func (s *ServiceStruct) TimeParseAny(dtStr string) (time.Time, error) {
	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02 15",
		"2006-01-02",
		"2006-01",
		"2006",
	}
	for _, format := range formats {
		t, e := time.Parse(format, dtStr)
		if e == nil {
			return t, nil
		}
	}
	err := fmt.Errorf("cannot parse datetime: '%s'\n", dtStr)
	err = errs.Wrap(errs.New(err, errs.ErrServerError), "TimeParseAny")
	return time.Now(), err
}

// JSONEscape - escape string for JSOn to avoid injections
func (s *ServiceStruct) JSONEscape(str string) string {
	b, _ := json.Marshal(str)
	return string(b[1 : len(b)-1])
}

// QueryOut - display DB query
func (s *ServiceStruct) QueryOut(query string, args ...interface{}) {
	log.Info(query)
	if len(args) > 0 {
		s := ""
		for vi, vv := range args {
			switch v := vv.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128, string, bool, time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float32, *float64, *complex64, *complex128, *string, *bool, *time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case nil:
				s += fmt.Sprintf("%d:(null) ", vi+1)
			default:
				s += fmt.Sprintf("%d:%+v ", vi+1, reflect.ValueOf(vv))
			}
		}
		log.Info("[" + s + "]")
	}
}

// QueryDB - query database without transaction
func (s *ServiceStruct) QueryDB(db *sqlx.DB, query string, args ...interface{}) (rows *sql.Rows, err error) {
	rows, err = db.Query(query, args...)
	if err != nil || GSQLOut {
		if err != nil {
			err = errs.Wrap(errs.New(err, errs.ErrServerError), "QueryDB")
			log.Info("QueryDB failed")
		}
		s.QueryOut(query, args...)
	}
	return
}

// QueryTX - query database with transaction
func (s *ServiceStruct) QueryTX(db *sql.Tx, query string, args ...interface{}) (rows *sql.Rows, err error) {
	rows, err = db.Query(query, args...)
	if err != nil || GSQLOut {
		if err != nil {
			err = errs.Wrap(errs.New(err, errs.ErrServerError), "QueryTX")
			log.Info("QueryTX failed")
		}
		s.QueryOut(query, args...)
	}
	return
}

// Query - query DB using transaction if provided
func (s *ServiceStruct) Query(db *sqlx.DB, tx *sql.Tx, query string, args ...interface{}) (*sql.Rows, error) {
	if tx == nil {
		return s.QueryDB(db, query, args...)
	}
	return s.QueryTX(tx, query, args...)
}

// ExecDB - execute DB query without transaction
func (s *ServiceStruct) ExecDB(db *sqlx.DB, query string, args ...interface{}) (res sql.Result, err error) {
	res, err = db.Exec(query, args...)
	if err != nil || GSQLOut {
		if err != nil {
			err = errs.Wrap(errs.New(err, errs.ErrServerError), "ExecDB")
			log.Info("ExecDB failed")
		}
		s.QueryOut(query, args...)
	}
	return
}

// ExecTX - execute DB query with transaction
func (s *ServiceStruct) ExecTX(db *sql.Tx, query string, args ...interface{}) (res sql.Result, err error) {
	res, err = db.Exec(query, args...)
	if err != nil || GSQLOut {
		if err != nil {
			err = errs.Wrap(errs.New(err, errs.ErrServerError), "ExecTX")
			log.Info("ExecTX failed")
		}
		s.QueryOut(query, args...)
	}
	return
}

// Exec - execute db query with transaction if provided
func (s *ServiceStruct) Exec(db *sqlx.DB, tx *sql.Tx, query string, args ...interface{}) (sql.Result, error) {
	if tx == nil {
		return s.ExecDB(db, query, args...)
	}
	return s.ExecTX(tx, query, args...)
}
