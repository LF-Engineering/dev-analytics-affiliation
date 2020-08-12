package shared

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"database/sql"
	"encoding/json"

	"github.com/go-openapi/strfmt"
	"github.com/jmoiron/sqlx"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"

	"github.com/LF-Engineering/dev-analytics-affiliation/errs"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
)

const (
	// LogListMax - do not log lists longer than 30 elements, display list counts instead
	LogListMax = 30
	// DateFormat - format date as YYYY-MM-DD
	DateFormat = "2006-01-02"
	// DefaultRole - default enrollment role
	DefaultRole = "Contributor"
	// ContributorRole - Contributor
	ContributorRole = "Contributor"
	// MaintainerRole - Maintainer
	MaintainerRole = "Maintainer"
	// FetchSize
	FetchSize = 20_000
	// MaxAggsSize
	MaxAggsSize = 10_000
	// CacheTimeResolution - when caching top contributors from and to parameters are rounded using this parameter (ms)
	CacheTimeResolution = 43200000 // 12 hours
)

var (
	// GSQLOut - if set displays all SQLs that are executed (if not set, only failed ones)
	GSQLOut bool
	// GSyncURL - used to trigger ssaw sync
	GSyncURL string
	// GDA2SF - map DA name to SF name
	GDA2SF map[string]string
	// GSF2DA - map SF name to DA name
	GSF2DA map[string]string
	// MinPeriodDate - default start data for enrollments
	MinPeriodDate = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	// MaxPeriodDate - default end date for enrollments
	MaxPeriodDate = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	// Roles - all currently defined roles
	Roles = []string{"Contributor", "Maintainer"}
	// TopContributorsCacheTTL - top contributors cache TTL (12 hours)
	TopContributorsCacheTTL = time.Duration(12) * time.Hour
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
	GetThreadsNum() int
	Now() *strfmt.DateTime
	TimeParseAny(string) (time.Time, error)
	DayStart(time.Time) time.Time
	RoundMSTime(int64) int64
	JSONEscape(string) string
	StripUnicode(string) string
	ToCaseInsensitiveRegexp(string) string
	SanitizeShortProfile(*models.AllOutput, bool)
	SanitizeShortIdentity(*models.IdentityShortOutput, bool)
	SanitizeShortEnrollment(*models.EnrollmentShortOutput, bool)
	SanitizeIdentity(*models.IdentityDataOutput)
	SanitizeProfile(*models.ProfileDataOutput)
	// Mapping
	DA2SF(string) string
	SF2DA(string) string
	AryDA2SF([]string) string
	UUDA2SF(*models.UniqueIdentityNestedDataOutput)
	ListProfilesDA2SF(*models.GetListProfilesOutput)
	ProfileEnrollmentsDA2SF(*models.GetProfileEnrollmentsDataOutput)
	ListProjectsDA2SF(*models.ListProjectsOutput)
	AllDA2SF(*models.AllArrayOutput)
	AllSF2DA([]*models.AllOutput)
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
func (e *LocalEnrollmentShortOutput) SortKey() (key string) {
	key = e.Start + ":" + e.End + ":" + e.Organization + ":" + e.Role + ":"
	if e.ProjectSlug != nil {
		key += *(e.ProjectSlug)
	}
	return
}

// SortKey - defines sort order for identities
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

// SortKey - defines sort order for profiles
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
func (s *ServiceStruct) SanitizeShortIdentity(identity *models.IdentityShortOutput, isGet bool) {
	from := "@"
	to := "!"
	if !isGet {
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
func (s *ServiceStruct) SanitizeShortEnrollment(enrollment *models.EnrollmentShortOutput, isGet bool) {
	role := strings.TrimSpace(enrollment.Role)
	if isGet {
		if enrollment.Role == ContributorRole {
			role = "C"
		} else if enrollment.Role == MaintainerRole {
			role = "M"
		} else if role != "C" && role != "M" {
			log.Info("WARNING: unknown role: " + enrollment.Role)
			role = "C"
		}
	} else {
		if enrollment.Role == "C" {
			role = ContributorRole
		} else if enrollment.Role == "M" {
			role = MaintainerRole
		} else if role != ContributorRole && role != MaintainerRole {
			log.Info("WARNING: unknown role: " + enrollment.Role)
			role = ContributorRole
		}
	}
	enrollment.Role = role
	enrollment.Organization = strings.TrimSpace(enrollment.Organization)
	enrollment.Start = strings.TrimSpace(enrollment.Start)
	enrollment.End = strings.TrimSpace(enrollment.End)
	if enrollment.ProjectSlug != nil {
		projectSlug := strings.TrimSpace(*(enrollment.ProjectSlug))
		enrollment.ProjectSlug = &projectSlug
	}
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
func (s *ServiceStruct) SanitizeShortProfile(prof *models.AllOutput, isGet bool) {
	from := "@"
	to := "!"
	if !isGet {
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
		s.SanitizeShortIdentity(identity, isGet)
	}
	for _, enrollment := range prof.Enrollments {
		s.SanitizeShortEnrollment(enrollment, isGet)
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
			"Role":           i.Role,
		}
		if i.Organization != nil {
			m["Organization"] = *(i.Organization)
		}
		if i.ProjectSlug != nil {
			m["ProjectSlug"] = *(i.ProjectSlug)
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

// GetThreadsNum - returns number of available threads
func (s *ServiceStruct) GetThreadsNum() int {
	nCPUsStr := os.Getenv("N_CPUS")
	nCPUs := 0
	if nCPUsStr != "" {
		var err error
		nCPUs, err = strconv.Atoi(nCPUsStr)
		if err != nil || nCPUs < 0 {
			nCPUs = 0
		}
	}
	if nCPUs > 0 {
		n := runtime.NumCPU()
		if nCPUs > n {
			nCPUs = n
		}
		runtime.GOMAXPROCS(nCPUs)
		return nCPUs
	}
	thrN := runtime.NumCPU()
	runtime.GOMAXPROCS(thrN)
	return thrN
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

// DayStart - round date to day start
func (s *ServiceStruct) DayStart(dt time.Time) time.Time {
	return time.Date(
		dt.Year(),
		dt.Month(),
		dt.Day(),
		0,
		0,
		0,
		0,
		time.UTC,
	)
}

// RoundMSTime - round using CacheTimeResolution (12 hours)
func (s *ServiceStruct) RoundMSTime(t int64) int64 {
	return (t / CacheTimeResolution) * CacheTimeResolution
}

// ToCaseInsensitiveRegexp - transform string say "abc" to ".*[aA][bB][cC].*"
func (s *ServiceStruct) ToCaseInsensitiveRegexp(str string) string {
	ret := "'.*"
	for _, b := range str {
		if b >= 0x41 && b <= 0x5a {
			ret += "[" + string(b+0x20) + string(b) + "]"
		} else if b >= 0x61 && b <= 0x7a {
			ret += "[" + string(b) + string(b-0x20) + "]"
		} else if b == 0x20 {
			ret += `\s+`
		} else {
			ret += string(b)
		}
	}
	return ret + ".*'"
}

// StripUnicode - strip special characters and remove non-ascii chars ł->l, ą->a etc.
func (s *ServiceStruct) StripUnicode(str string) string {
	isNonASCII := func(r rune) bool {
		return r < 32 || r >= 127
	}
	manualReplaces := [][2]string{
		{"ł", "l"},
		{"ø", "o"},
		{"ß", "ss"},
		{"æ", "ae"},
	}
	for _, replace := range manualReplaces {
		str = strings.Replace(str, replace[0], replace[1], -1)
	}
	t := transform.Chain(norm.NFKD, transform.RemoveFunc(isNonASCII))
	str, _, _ = transform.String(t, str)
	return str
}

// JSONEscape - escape string for JSON to avoid injections
func (s *ServiceStruct) JSONEscape(str string) string {
	b, _ := json.Marshal(str)
	return string(b[1 : len(b)-1])
}

// QueryOut - display DB query
func (s *ServiceStruct) QueryOut(query string, args ...interface{}) {
	log.Warn(query)
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
		log.Warn("[" + s + "]")
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

// DA2SF - map DA name to SF name (fallback to no change)
func (s *ServiceStruct) DA2SF(da string) (sf string) {
	var ok bool
	sf, ok = GDA2SF[da]
	if !ok {
		sf = da
	}
	return
}

// SF2DA - map SF name to DA name (fallback to no change)
func (s *ServiceStruct) SF2DA(sf string) (da string) {
	var ok bool
	da, ok = GSF2DA[sf]
	if !ok {
		da = sf
	}
	return
}

// AryDA2SF - map DA names array to SF names string "," separated (fallback to no change)
func (s *ServiceStruct) AryDA2SF(da []string) (sf string) {
	ary := []string{}
	for _, d := range da {
		ary = append(ary, s.DA2SF(d))
	}
	return strings.Join(ary, ",")
}

// UUDA2SF - map DA name to SF name
func (s *ServiceStruct) UUDA2SF(uid *models.UniqueIdentityNestedDataOutput) {
	for i, rol := range uid.Enrollments {
		if rol.ProjectSlug != nil {
			project := s.DA2SF(*rol.ProjectSlug)
			uid.Enrollments[i].ProjectSlug = &project
		}
	}
}

// ListProfilesDA2SF - map DA name to SF name
func (s *ServiceStruct) ListProfilesDA2SF(data *models.GetListProfilesOutput) {
	for i := range data.Uids {
		s.UUDA2SF(data.Uids[i])
	}
}

// ProfileEnrollmentsDA2SF - map DA name to SF name
func (s *ServiceStruct) ProfileEnrollmentsDA2SF(data *models.GetProfileEnrollmentsDataOutput) {
	for i, rol := range data.Enrollments {
		if rol.ProjectSlug != nil {
			project := s.DA2SF(*rol.ProjectSlug)
			data.Enrollments[i].ProjectSlug = &project
		}
	}
}

// ListProjectsDA2SF - map DA name to SF name
func (s *ServiceStruct) ListProjectsDA2SF(data *models.ListProjectsOutput) {
	for i := range data.Projects {
		data.Projects[i].ProjectSlug = s.DA2SF(data.Projects[i].ProjectSlug)
	}
}

// AllDA2SF - map DA name to SF name
func (s *ServiceStruct) AllDA2SF(data *models.AllArrayOutput) {
	for i, prof := range data.Profiles {
		for j, rol := range prof.Enrollments {
			if rol.ProjectSlug != nil {
				project := s.DA2SF(*rol.ProjectSlug)
				data.Profiles[i].Enrollments[j].ProjectSlug = &project
			}
		}
	}
}

// AllSF2DA - map SF name to DA name
func (s *ServiceStruct) AllSF2DA(data []*models.AllOutput) {
	for i, prof := range data {
		for j, rol := range prof.Enrollments {
			if rol.ProjectSlug != nil {
				project := s.SF2DA(*rol.ProjectSlug)
				data[i].Enrollments[j].ProjectSlug = &project
			}
		}
	}
}
