package shdb

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"io/ioutil"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"

	"github.com/go-openapi/strfmt"
	"github.com/jmoiron/sqlx"

	"github.com/LF-Engineering/dev-analytics-affiliation/errs"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/LF-Engineering/dev-analytics-affiliation/shared"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
	yaml "gopkg.in/yaml.v2"

	// SortingHat database is MariaDB/MySQL format
	_ "github.com/go-sql-driver/mysql"
)

// "github.com/LF-Engineering/ssaw/ssawsync"

// Service - access affiliations MariaDB interface
type Service interface {
	shared.ServiceInterface
	// External CRUD methods
	// Country
	GetCountry(string, *sql.Tx) (*models.CountryDataOutput, error)
	// Profile
	GetProfile(string, bool, *sql.Tx) (*models.ProfileDataOutput, error)
	EditProfile(*models.ProfileDataOutput, bool, *sql.Tx) (*models.ProfileDataOutput, error)
	AddProfile(*models.ProfileDataOutput, bool, *sql.Tx) (*models.ProfileDataOutput, error)
	ValidateProfile(*models.ProfileDataOutput, *sql.Tx) error
	DeleteProfile(string, bool, bool, *time.Time, *sql.Tx) error
	ArchiveProfile(string, *time.Time, *sql.Tx) error
	UnarchiveProfile(string, bool, *time.Time, *sql.Tx) error
	DeleteProfileArchive(string, bool, bool, *time.Time, *sql.Tx) error
	FindProfiles([]string, []interface{}, bool, *sql.Tx) ([]*models.ProfileDataOutput, error)
	ProfileUUIDHash(*models.ProfileDataOutput) (string, error)
	// Identity
	TouchIdentity(string, *sql.Tx) (int64, error)
	GetIdentity(string, bool, *sql.Tx) (*models.IdentityDataOutput, error)
	EditIdentity(*models.IdentityDataOutput, bool, *sql.Tx) (*models.IdentityDataOutput, error)
	DeleteIdentity(string, bool, bool, *time.Time, *sql.Tx) error
	ArchiveIdentity(string, *time.Time, *sql.Tx) error
	UnarchiveIdentity(string, bool, *time.Time, *sql.Tx) error
	DeleteIdentityArchive(string, bool, bool, *time.Time, *sql.Tx) error
	ValidateIdentity(*models.IdentityDataOutput, bool) error
	FindIdentities([]string, []interface{}, []bool, bool, *sql.Tx) ([]*models.IdentityDataOutput, error)
	AddIdentity(*models.IdentityDataOutput, bool, bool, *sql.Tx) (*models.IdentityDataOutput, error)
	IdentityIDHash(*models.IdentityDataOutput) (string, error)
	// UniqueIdentity
	TouchUniqueIdentity(string, *sql.Tx) (int64, error)
	AddUniqueIdentity(*models.UniqueIdentityDataOutput, bool, *sql.Tx) (*models.UniqueIdentityDataOutput, error)
	GetUniqueIdentity(string, bool, *sql.Tx) (*models.UniqueIdentityDataOutput, error)
	DeleteUniqueIdentity(string, bool, bool, *time.Time, *sql.Tx) error
	ArchiveUniqueIdentity(string, *time.Time, *sql.Tx) error
	UnarchiveUniqueIdentity(string, bool, *time.Time, *sql.Tx) error
	DeleteUniqueIdentityArchive(string, bool, bool, *time.Time, *sql.Tx) error
	QueryUniqueIdentitiesNested(string, int64, int64, bool, []string, *sql.Tx) ([]*models.UniqueIdentityNestedDataOutput, int64, error)
	// Enrollment
	GetEnrollment(int64, bool, *sql.Tx) (*models.EnrollmentDataOutput, error)
	FindEnrollments([]string, []interface{}, []bool, bool, *sql.Tx) ([]*models.EnrollmentDataOutput, error)
	EditEnrollment(*models.EnrollmentDataOutput, bool, *sql.Tx) (*models.EnrollmentDataOutput, error)
	AddEnrollment(*models.EnrollmentDataOutput, bool, bool, *sql.Tx) (*models.EnrollmentDataOutput, error)
	DeleteEnrollment(int64, bool, bool, *time.Time, *sql.Tx) error
	ArchiveEnrollment(int64, *time.Time, *sql.Tx) error
	UnarchiveEnrollment(int64, bool, *time.Time, *sql.Tx) error
	DeleteEnrollmentArchive(int64, bool, bool, *time.Time, *sql.Tx) error
	ValidateEnrollment(*models.EnrollmentDataOutput, bool) error
	// Organization
	FindOrganizations([]string, []interface{}, bool, *sql.Tx) ([]*models.OrganizationDataOutput, error)
	QueryOrganizationsNested(string, int64, int64, *sql.Tx) ([]*models.OrganizationNestedDataOutput, int64, error)
	AddOrganization(*models.OrganizationDataOutput, bool, *sql.Tx) (*models.OrganizationDataOutput, error)
	EditOrganization(*models.OrganizationDataOutput, bool, *sql.Tx) (*models.OrganizationDataOutput, error)
	GetOrganization(int64, bool, *sql.Tx) (*models.OrganizationDataOutput, error)
	GetOrganizationByName(string, bool, *sql.Tx) (*models.OrganizationDataOutput, error)
	DropOrganization(int64, bool, *sql.Tx) error
	ValidateOrganization(*models.OrganizationDataOutput, bool) error
	// Organization Domain
	DropOrgDomain(string, string, bool, *sql.Tx) error
	QueryOrganizationsDomains(int64, string, int64, int64, *sql.Tx) ([]*models.DomainDataOutput, int64, error)
	// MatchingBlacklist
	QueryMatchingBlacklist(*sql.Tx, string, int64, int64) ([]*models.MatchingBlacklistOutput, int64, error)
	AddMatchingBlacklist(*models.MatchingBlacklistOutput, bool, *sql.Tx) (*models.MatchingBlacklistOutput, error)
	FetchMatchingBlacklist(string, bool, *sql.Tx) (*models.MatchingBlacklistOutput, error)
	DropMatchingBlacklist(string, bool, *sql.Tx) error
	// Slug Mappings
	GetSlugMappings() error
	GetListSlugMappings() (*models.ListSlugMappings, error)
	FindSlugMappings([]string, []interface{}, bool, *sql.Tx) ([]*models.SlugMapping, error)
	AddSlugMapping(*models.SlugMapping, *sql.Tx) (*models.SlugMapping, error)
	DeleteSlugMapping(string) (*models.TextStatusOutput, error)
	DropSlugMapping(string, bool, *sql.Tx) error
	EditSlugMapping(*models.SlugMapping, *models.SlugMapping, *sql.Tx) (*models.SlugMapping, error)
	// Other
	MoveIdentityToUniqueIdentity(*models.IdentityDataOutput, *models.UniqueIdentityDataOutput, bool, *sql.Tx) error
	GetArchiveUniqueIdentityEnrollments(string, time.Time, bool, *sql.Tx) ([]*models.EnrollmentDataOutput, error)
	GetArchiveUniqueIdentityIdentities(string, time.Time, bool, *sql.Tx) ([]*models.IdentityDataOutput, error)
	GetUniqueIdentityEnrollments(string, bool, *sql.Tx) ([]*models.EnrollmentDataOutput, error)
	GetUniqueIdentityIdentities(string, bool, *sql.Tx) ([]*models.IdentityDataOutput, error)
	MoveEnrollmentToUniqueIdentity(*models.EnrollmentDataOutput, *models.UniqueIdentityDataOutput, *sql.Tx) error
	MergeEnrollments(*models.UniqueIdentityDataOutput, *models.OrganizationDataOutput, *string, bool, bool, *sql.Tx) error
	MergeDateRanges([][]strfmt.DateTime) ([][]strfmt.DateTime, error)
	FindUniqueIdentityOrganizations(string, bool, *sql.Tx) ([]*models.OrganizationDataOutput, error)
	ArchiveUUID(string, *time.Time, *sql.Tx) (*time.Time, error)
	UnarchiveUUID(string, time.Time, *sql.Tx) error
	SetProfileEmptyDataFromIdentities(string, []*models.IdentityDataOutput, *sql.Tx) error
	Unarchive(string, string) (bool, error)
	CheckUnaffiliated([]*models.UnaffiliatedDataOutput, []string, *sql.Tx) ([]*models.UnaffiliatedDataOutput, error)
	EnrichContributors([]*models.ContributorFlatStats, []string, int64, *sql.Tx) error
	GetDetAffRangeSubjects() ([]*models.EnrollmentProjectRange, error)
	UpdateAffRange([]*models.EnrollmentProjectRange) (string, error)
	UpdateProjectSlugs(map[string][]string) (string, error)
	DedupEnrollments() error
	// SSAW related
	// NotifySSAW()
	// SetOrigin()

	// API endpoints
	GetMatchingBlacklist(string, int64, int64) (*models.GetMatchingBlacklistOutput, error)
	PostMatchingBlacklist(string) (*models.MatchingBlacklistOutput, error)
	DeleteMatchingBlacklist(string) (*models.TextStatusOutput, error)
	DeleteOrganization(int64) (*models.TextStatusOutput, error)
	DeleteOrgDomain(string, string) (*models.TextStatusOutput, error)
	DeleteProfileNested(string, bool) (*models.TextStatusOutput, error)
	UnarchiveProfileNested(string, []string) (*models.UniqueIdentityNestedDataOutput, error)
	GetListOrganizations(string, int64, int64) (*models.GetListOrganizationsOutput, error)
	GetListOrganizationsDomains(int64, string, int64, int64) (*models.GetListOrganizationsDomainsOutput, error)
	GetListProfiles(string, int64, int64, []string) (*models.GetListProfilesOutput, error)
	AddNestedUniqueIdentity(string) (*models.UniqueIdentityNestedDataOutput, error)
	AddNestedIdentity(*models.IdentityDataOutput) (*models.UniqueIdentityNestedDataOutput, error)
	FindEnrollmentsNested([]string, []interface{}, []bool, bool, []string, *sql.Tx) ([]*models.EnrollmentNestedDataOutput, error)
	WithdrawEnrollment(*models.EnrollmentDataOutput, bool, *sql.Tx) error
	PutOrgDomain(string, string, bool, bool, bool) (*models.PutOrgDomainOutput, error)
	MergeUniqueIdentities(string, string, bool) (string, bool, error)
	MoveIdentity(string, string, bool) error
	GetAllAffiliations() (*models.AllArrayOutput, error)
	BulkUpdate([]*models.AllOutput, []*models.AllOutput) (int, int, int, error)
	MergeAll(int, bool) (string, error)
	HideEmails() (string, error)
	MapOrgNames() (string, error)
}

type allMappings struct {
	Mappings [][2]string `yaml:"mappings"`
}

type service struct {
	shared.ServiceStruct
	db               *sqlx.DB
	rodb             *sqlx.DB
	origin           string
	mtx              *sync.RWMutex
	orgNamesMappings allMappings
	mappingsLoaded   bool
}

// New creates new db service instance with given db
func New(db, rodb *sqlx.DB, origin string) Service {
	return &service{
		db:     db,
		rodb:   rodb,
		origin: origin,
		mtx:    &sync.RWMutex{},
	}
}

// DateTimeFormat - this is how we format datetime for MariaDB
const (
	DateTimeFormat  = "%Y-%m-%dT%H:%i:%s.%fZ"
	MapOrgNamesFile = "map_org_names.yaml"
)

func (s *service) GetCountry(countryCode string, tx *sql.Tx) (countryData *models.CountryDataOutput, err error) {
	log.Info(fmt.Sprintf("GetCountry: countryCode:%s tx:%v", countryCode, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("GetCountry(exit): countryCode:%s tx:%v countryData:%+v err:%v", countryCode, tx != nil, countryData, err))
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	countryData = &models.CountryDataOutput{}
	rows, err := s.Query(
		sdb,
		tx,
		"select code, name, alpha3 from countries where code = ? limit 1",
		countryCode,
	)
	if err != nil {
		return
	}
	fetched := false
	for rows.Next() {
		err = rows.Scan(
			&countryData.Code,
			&countryData.Name,
			&countryData.Alpha3,
		)
		if err != nil {
			return
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if !fetched {
		err = fmt.Errorf("cannot find country by code '%s'", countryCode)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetCountry")
		return
	}
	return
}

func (s *service) MergeDateRanges(dates [][]strfmt.DateTime) (mergedDates [][]strfmt.DateTime, err error) {
	log.Info(fmt.Sprintf("MergeDateRanges: dates:%+v", dates))
	defer func() {
		log.Info(fmt.Sprintf("MergeDateRanges(exit): dates:%+v mergedDates:%+v err:%v", dates, mergedDates, err))
	}()
	if len(dates) == 0 {
		return
	}
	sortedDates := [][]strfmt.DateTime{}
	for index, pair := range dates {
		if len(pair) != 2 {
			err = fmt.Errorf("datetime start-end pair number #%d doesn't have exactly 2 elements: %+v", index, pair)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "MergeDateRanges")
			return
		}
		newPair := []strfmt.DateTime{}
		if time.Time(pair[0]).Before(time.Time(pair[1])) {
			newPair = append(newPair, pair[0])
			newPair = append(newPair, pair[1])
		} else {
			newPair = append(newPair, pair[1])
			newPair = append(newPair, pair[0])
		}
		sortedDates = append(sortedDates, newPair)
	}
	sort.SliceStable(sortedDates, func(i, j int) bool {
		idx := 0
		if sortedDates[i][0] == sortedDates[j][0] {
			idx = 1
		}
		return time.Time(sortedDates[i][idx]).Before(time.Time(sortedDates[j][idx]))
	})
	saved := []strfmt.DateTime{sortedDates[0][0], sortedDates[0][1]}
	minRange, maxRange := false, false
	for _, data := range sortedDates {
		st := data[0]
		en := data[1]
		if time.Time(st).Before(shared.MinPeriodDate) || time.Time(st).After(shared.MaxPeriodDate) {
			err = fmt.Errorf("start date %v must be between %v and %v", st, shared.MinPeriodDate, shared.MaxPeriodDate)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "MergeDateRanges")
			return
		}
		if time.Time(en).Before(shared.MinPeriodDate) || time.Time(en).After(shared.MaxPeriodDate) {
			err = fmt.Errorf("end date %v must be between %v and %v", en, shared.MinPeriodDate, shared.MaxPeriodDate)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "MergeDateRanges")
			return
		}
		// st <= saved[1]
		if !time.Time(st).After(time.Time(saved[1])) {
			// saved[0] == MIN_PERIOD_DATE
			if !time.Time(saved[0]).After(shared.MinPeriodDate) {
				saved[0] = st
				minRange = true
			}
			// if MAX_PERIOD_DATE in (en, saved[1]):
			if !time.Time(saved[1]).Before(shared.MaxPeriodDate) || !time.Time(en).Before(shared.MaxPeriodDate) {
				// if saved1 > en
				if time.Time(saved[1]).After(time.Time(en)) {
					saved[1] = en
				}
				maxRange = true
			} else {
				// if saved1 < en
				if time.Time(saved[1]).Before(time.Time(en)) {
					saved[1] = en
				}
			}
		} else {
			// st > saved[1]
			mergedDates = append(mergedDates, []strfmt.DateTime{saved[0], saved[1]})
			saved[0] = st
			saved[1] = en
		}
	}
	mergedDates = append(mergedDates, saved)
	if minRange {
		mergedDates[0][0] = strfmt.DateTime(shared.MinPeriodDate)
	}
	if maxRange {
		mergedDates[len(mergedDates)-1][1] = strfmt.DateTime(shared.MaxPeriodDate)
	}
	return
}

func (s *service) MergeEnrollments(uniqueIdentity *models.UniqueIdentityDataOutput, organization *models.OrganizationDataOutput, projectSlug *string, allProjectSlugs, noMergeFatal bool, tx *sql.Tx) (err error) {
	pSlug := ""
	if projectSlug != nil {
		pSlug = *projectSlug
	}
	log.Info(fmt.Sprintf("MergeEnrollments: uniqueIdentity:%+v organization:%+v projectSlug:%v allProjectSlugs:%v noMergeFatal:%v tx:%v", s.ToLocalUniqueIdentity(uniqueIdentity), organization, pSlug, allProjectSlugs, noMergeFatal, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("MergeEnrollments(exit): uniqueIdentity:%+v organization:%+v projectSlug:%v allProjectSlugs:%v noMergeFatal:%v tx:%v err:%v", s.ToLocalUniqueIdentity(uniqueIdentity), organization, pSlug, allProjectSlugs, noMergeFatal, tx != nil, err))
	}()
	projectSlugs := make(map[*string]struct{})
	projectSlugsInf := make(map[string]struct{})
	if allProjectSlugs {
		var rols []*models.EnrollmentDataOutput
		rols, err = s.FindEnrollments([]string{"uuid", "organization_id"}, []interface{}{uniqueIdentity.UUID, organization.ID}, []bool{false, false}, false, tx)
		if err != nil {
			return
		}
		for _, rol := range rols {
			projectSlugs[rol.ProjectSlug] = struct{}{}
			ps := rol.ProjectSlug
			if ps == nil {
				projectSlugsInf["(null)"] = struct{}{}
			} else {
				projectSlugsInf[*ps] = struct{}{}
			}
		}
	} else {
		projectSlugs[projectSlug] = struct{}{}
		if projectSlug == nil {
			projectSlugsInf["(null)"] = struct{}{}
		} else {
			projectSlugsInf[*projectSlug] = struct{}{}
		}
	}
	log.Info(fmt.Sprintf("Merging projects %+v organization %+v uniqueIdentity %+v", projectSlugsInf, organization, s.ToLocalUniqueIdentity(uniqueIdentity)))
	for slug := range projectSlugs {
		pS := ""
		if slug != nil {
			pS = *slug
		}
		merged := 0
		for _, role := range shared.Roles {
			var disjoint []*models.EnrollmentDataOutput
			disjoint, err = s.FindEnrollments([]string{"uuid", "organization_id", "project_slug", "role"}, []interface{}{uniqueIdentity.UUID, organization.ID, slug, role}, []bool{false, false, false, false}, false, tx)
			if err != nil {
				return
			}
			if len(disjoint) == 0 {
				continue
			}
			dates := [][]strfmt.DateTime{}
			for _, rol := range disjoint {
				dates = append(dates, []strfmt.DateTime{rol.Start, rol.End})
			}
			var mergedDates [][]strfmt.DateTime
			mergedDates, err = s.MergeDateRanges(dates)
			if err != nil {
				return
			}
			for _, data := range mergedDates {
				st := data[0]
				en := data[1]
				isDup := func(x *models.EnrollmentDataOutput, st, en strfmt.DateTime) bool {
					return x.Start == st && x.End == en
				}
				filtered := []*models.EnrollmentDataOutput{}
				for _, rol := range disjoint {
					if !isDup(rol, st, en) {
						filtered = append(filtered, rol)
					}
				}
				if len(filtered) != len(disjoint) {
					disjoint = filtered
					continue
				}
				newEnrollment := &models.EnrollmentDataOutput{UUID: uniqueIdentity.UUID, OrganizationID: organization.ID, Start: st, End: en, ProjectSlug: slug, Role: role}
				_, err = s.AddEnrollment(newEnrollment, false, false, tx)
				if err != nil {
					return
				}
			}
			for _, rol := range disjoint {
				err = s.DeleteEnrollment(rol.ID, false, true, nil, tx)
				if err != nil {
					return
				}
			}
			merged++
		}
		if merged == 0 && noMergeFatal {
			err = fmt.Errorf("merge enrollments unique identity '%+v' organization '%+v' projectSlug %v allProjectSlugs %v found no enrollments", s.ToLocalUniqueIdentity(uniqueIdentity), organization, pS, allProjectSlugs)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "MergeEnrollments")
			return
		}
	}
	return
}

func (s *service) MoveEnrollmentToUniqueIdentity(enrollment *models.EnrollmentDataOutput, uniqueIdentity *models.UniqueIdentityDataOutput, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("MoveEnrollmentToUniqueIdentity: enrollment:%+v uniqueIdentity:%+v tx:%v", enrollment, s.ToLocalUniqueIdentity(uniqueIdentity), tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("MoveEnrollmentToUniqueIdentity(exit): enrollment:%+v uniqueIdentity:%+v tx:%v err:%v", enrollment, s.ToLocalUniqueIdentity(uniqueIdentity), tx != nil, err))
	}()
	if enrollment.UUID == uniqueIdentity.UUID {
		return
	}
	oldUniqueIdentity, err := s.GetUniqueIdentity(enrollment.UUID, true, tx)
	if err != nil {
		return
	}
	enrollment.UUID = uniqueIdentity.UUID
	enrollment, err = s.EditEnrollment(enrollment, true, tx)
	if err != nil {
		return
	}
	affected, err := s.TouchUniqueIdentity(oldUniqueIdentity.UUID, tx)
	if err != nil {
		return
	}
	if affected != 1 {
		err = fmt.Errorf("'%+v' unique identity update affected %d rows", s.ToLocalUniqueIdentity(oldUniqueIdentity), affected)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "MoveEnrollmentToUniqueIdentity")
		return
	}
	affected, err = s.TouchUniqueIdentity(uniqueIdentity.UUID, tx)
	if err != nil {
		return
	}
	if affected != 1 {
		err = fmt.Errorf("'%+v' unique identity update affected %d rows", s.ToLocalUniqueIdentity(uniqueIdentity), affected)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "MoveEnrollmentToUniqueIdentity")
		return
	}
	return
}

func (s *service) MoveIdentityToUniqueIdentity(identity *models.IdentityDataOutput, uniqueIdentity *models.UniqueIdentityDataOutput, unarchive bool, tx *sql.Tx) (err error) {
	log.Info(
		fmt.Sprintf(
			"MoveIdentityToUniqueIdentity: identity:%+v uniqueIdentity:%+v unarchive:%v tx:%v",
			s.ToLocalIdentity(identity),
			s.ToLocalUniqueIdentity(uniqueIdentity),
			unarchive,
			tx != nil,
		),
	)
	defer func() {
		log.Info(
			fmt.Sprintf(
				"MoveIdentityToUniqueIdentity(exit): identity:%+v uniqueIdentity:%+v unarchive:%v tx:%v err:%v",
				s.ToLocalIdentity(identity),
				s.ToLocalUniqueIdentity(uniqueIdentity),
				unarchive,
				tx != nil,
				err,
			),
		)
	}()
	if identity.UUID != nil && (*(identity.UUID) == uniqueIdentity.UUID) {
		return
	}
	var oldUniqueIdentity *models.UniqueIdentityDataOutput
	if identity.UUID != nil {
		oldUniqueIdentity, err = s.GetUniqueIdentity(*(identity.UUID), true, tx)
		if err != nil {
			return
		}
	}
	identity.UUID = &uniqueIdentity.UUID
	identity.LastModified = s.Now()
	identity, err = s.EditIdentity(identity, true, tx)
	if err != nil {
		return
	}
	if oldUniqueIdentity != nil {
		affected := int64(0)
		affected, err = s.TouchUniqueIdentity(oldUniqueIdentity.UUID, tx)
		if err != nil {
			return
		}
		if affected != 1 {
			err = fmt.Errorf("'%+v' unique identity update affected %d rows", s.ToLocalUniqueIdentity(oldUniqueIdentity), affected)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "MoveIdentityToUniqueIdentity")
			return
		}
		affected, err = s.TouchUniqueIdentity(uniqueIdentity.UUID, tx)
		if err != nil {
			return
		}
		if affected != 1 {
			err = fmt.Errorf("'%+v' unique identity update affected %d rows", s.ToLocalUniqueIdentity(uniqueIdentity), affected)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "MoveIdentityToUniqueIdentity")
			return
		}
	}
	return
}

func (s *service) AddMatchingBlacklist(inMatchingBlacklist *models.MatchingBlacklistOutput, refresh bool, tx *sql.Tx) (matchingBlacklist *models.MatchingBlacklistOutput, err error) {
	log.Info(fmt.Sprintf("AddMatchingBlacklist: inMatchingBlacklist:%+v refresh:%v tx:%v", inMatchingBlacklist, refresh, tx != nil))
	matchingBlacklist = inMatchingBlacklist
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddMatchingBlacklist(exit): inMatchingBlacklist:%+v refresh:%v tx:%v matchingBlacklist:%+v err:%v",
				inMatchingBlacklist,
				refresh,
				tx != nil,
				matchingBlacklist,
				err,
			),
		)
	}()
	matchingBlacklist.Excluded = strings.TrimSpace(matchingBlacklist.Excluded)
	_, err = s.Exec(
		s.db,
		tx,
		"insert into matching_blacklist(excluded) select ?",
		matchingBlacklist.Excluded,
	)
	if err != nil {
		matchingBlacklist = nil
		return
	}
	if refresh {
		matchingBlacklist, err = s.FetchMatchingBlacklist(matchingBlacklist.Excluded, true, tx)
		if err != nil {
			matchingBlacklist = nil
			return
		}
	}
	return
}

func (s *service) EnrichContributors(contributors []*models.ContributorFlatStats, projectSlugs []string, millisSinceEpoch int64, tx *sql.Tx) (err error) {
	inf := ""
	n := len(contributors)
	if n > shared.LogListMax {
		inf = fmt.Sprintf("%d", n)
	} else {
		inf = fmt.Sprintf("%+v", s.ToLocalTopContributorsFlat(contributors))
	}
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	found := 0
	orgFound := 0
	log.Debug(fmt.Sprintf("EnrichContributors: contributors:%s projectSlugs:%+v millisSinceEpoch:%d tx:%v", inf, projectSlugs, millisSinceEpoch, tx != nil))
	defer func() {
		log.Debug(
			fmt.Sprintf(
				"EnrichContributors(exit): contributors:%s projectSlugs:%+v millisSinceEpoch:%d tx:%v found:%d/%d/%d err:%v",
				inf,
				projectSlugs,
				millisSinceEpoch,
				tx != nil,
				orgFound,
				found,
				n,
				err,
			),
		)
	}()
	if len(contributors) == 0 {
		return
	}
	pSlugs := []string{}
	fLikes := []string{}
	for _, projectSlug := range projectSlugs {
		if strings.HasSuffix(projectSlug, "-f") {
			fLikes = append(fLikes, projectSlug[:len(projectSlug)-2]+"/%")
		} else {
			pSlugs = append(pSlugs, projectSlug)
		}
	}
	//fmt.Printf("pSlugs: %v\nfLikes: %v\n", pSlugs, fLikes)
	secsSinceEpoch := float64(millisSinceEpoch) / 1000.0
	sel := "select distinct p.uuid, coalesce(p.name, ''), coalesce(p.email, ''), coalesce(o.name, '') from profiles p left join enrollments e"
	sel += fmt.Sprintf(
		" on p.uuid = e.uuid and e.start <= coalesce(from_unixtime(%f), now()) and e.end >= coalesce(from_unixtime(%f), now())",
		secsSinceEpoch,
		secsSinceEpoch,
	)
	sel += " left join organizations o on e.organization_id = o.id where p.uuid in ("
	uuids := []interface{}{}
	data := make(map[string][3]string)
	for _, contributor := range contributors {
		uuid := contributor.UUID
		uuids = append(uuids, uuid)
		sel += "?,"
	}
	sel = sel[0:len(sel)-1] + ") and (e.project_slug is null"
	if len(pSlugs) > 0 {
		sel += " or e.project_slug in ("
		for _, pSlug := range pSlugs {
			sel += "?,"
			uuids = append(uuids, pSlug)
		}
		sel = sel[0:len(sel)-1] + ")"
	}
	if len(fLikes) > 0 {
		for _, fLike := range fLikes {
			sel += " or e.project_slug like ?"
			uuids = append(uuids, fLike)
		}
	}
	sel += ") order by e.project_slug is null"
	var rows *sql.Rows
	// fmt.Printf("\n%+v\n%s\n\n", uuids, sel)
	rows, err = s.Query(sdb, tx, sel, uuids...)
	if err != nil {
		return
	}
	uuid := ""
	name := ""
	email := ""
	org := ""
	for rows.Next() {
		err = rows.Scan(&uuid, &name, &email, &org)
		if err != nil {
			return
		}
		_, ok := data[uuid]
		if !ok {
			data[uuid] = [3]string{name, email, org}
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	missUUIDs := []string{}
	for idx, contributor := range contributors {
		uuid := contributor.UUID
		ary, ok := data[uuid]
		if ok {
			contributors[idx].Name = ary[0]
			contributors[idx].Email = ary[1]
			contributors[idx].Organization = ary[2]
			found++
			if ary[2] != "" {
				orgFound++
			}
		} else {
			missUUIDs = append(missUUIDs, uuid)
		}
	}
	if len(missUUIDs) > 0 {
		sel := "select distinct p.uuid, coalesce(p.name, ''), coalesce(p.email, ''), coalesce(o.name, '') from profiles_archive p left join enrollments_archive e"
		sel += fmt.Sprintf(
			" on p.archived_at = e.archived_at and p.uuid = e.uuid and e.start <= coalesce(from_unixtime(%f), now()) and e.end >= coalesce(from_unixtime(%f), now())",
			secsSinceEpoch,
			secsSinceEpoch,
		)
		sel += " left join organizations o on e.organization_id = o.id where p.uuid in ("
		uuids := []interface{}{}
		data := make(map[string][3]string)
		for _, uuid := range missUUIDs {
			uuids = append(uuids, uuid)
			sel += "?,"
		}
		sel = sel[0:len(sel)-1] + ") and (e.project_slug is null"
		if len(pSlugs) > 0 {
			sel += " or e.project_slug in ("
			for _, pSlug := range pSlugs {
				sel += "?,"
				uuids = append(uuids, pSlug)
			}
			sel = sel[0:len(sel)-1] + ")"
		}
		if len(fLikes) > 0 {
			for _, fLike := range fLikes {
				sel += " or e.project_slug like ?"
				uuids = append(uuids, fLike)
			}
		}
		sel += ") order by e.project_slug is null, p.uuid asc, p.archived_at desc"
		var rows *sql.Rows
		rows, err = s.Query(sdb, tx, sel, uuids...)
		if err != nil {
			return
		}
		uuid := ""
		name := ""
		email := ""
		org := ""
		for rows.Next() {
			err = rows.Scan(&uuid, &name, &email, &org)
			if err != nil {
				return
			}
			_, ok := data[uuid]
			if !ok {
				data[uuid] = [3]string{name, email, org}
			}
		}
		err = rows.Err()
		if err != nil {
			return
		}
		err = rows.Close()
		if err != nil {
			return
		}
		for idx, contributor := range contributors {
			uuid := contributor.UUID
			ary, ok := data[uuid]
			if ok {
				contributors[idx].Name = ary[0]
				contributors[idx].Email = ary[1]
				contributors[idx].Organization = ary[2]
				found++
				if ary[2] != "" {
					orgFound++
				}
			}
		}
	}
	return
}

func (s *service) CheckUnaffiliated(inUnaffiliated []*models.UnaffiliatedDataOutput, projectSlugs []string, tx *sql.Tx) (unaffiliated []*models.UnaffiliatedDataOutput, err error) {
	inunaff := ""
	nUnaffiliated := len(inUnaffiliated)
	if nUnaffiliated > shared.LogListMax {
		inunaff = fmt.Sprintf("%d", nUnaffiliated)
	} else {
		inunaff = fmt.Sprintf("%+v", s.ToLocalUnaffiliated(inUnaffiliated))
	}
	log.Info(fmt.Sprintf("CheckUnaffiliated: inUnaffiliated:%s projectSlugs:%+v tx:%v", inunaff, projectSlugs, tx != nil))
	defer func() {
		unaff := ""
		nUnaffiliated := len(unaffiliated)
		if nUnaffiliated > shared.LogListMax {
			unaff = fmt.Sprintf("%d", nUnaffiliated)
		} else {
			unaff = fmt.Sprintf("%+v", s.ToLocalUnaffiliated(unaffiliated))
		}
		log.Info(
			fmt.Sprintf(
				"CheckUnaffiliated(exit): inUnaffiliated:%+v projectSlugs:%+v tx:%v unaffiliated:%+v err:%v",
				inunaff,
				projectSlugs,
				tx != nil,
				unaff,
				err,
			),
		)
	}()
	if len(inUnaffiliated) == 0 {
		log.Info("No unaffiliated data to check")
		return
	}
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	sel := "select p.uuid, p.name, e.uuid from profiles p left join enrollments e on p.uuid = e.uuid"
	sel += " where (p.is_bot is null or p.is_bot = 0) and p.uuid in ("
	uuids := []interface{}{}
	contribs := make(map[string]int64)
	for _, unaff := range inUnaffiliated {
		uuid := unaff.UUID
		uuids = append(uuids, uuid)
		sel += "?,"
		contribs[uuid] = unaff.Contributions
	}
	sel = sel[0:len(sel)-1] + ") and (e.project_slug is null or e.project_slug in ("
	for _, projectSlug := range projectSlugs {
		sel += "?,"
		uuids = append(uuids, projectSlug)
	}
	sel = sel[0:len(sel)-1] + ")) order by e.project_slug is null"
	var rows *sql.Rows
	rows, err = s.Query(sdb, tx, sel, uuids...)
	if err != nil {
		return
	}
	uuid := ""
	var (
		puuid *string
		pname *string
	)
	for rows.Next() {
		err = rows.Scan(&uuid, &pname, &puuid)
		if err != nil {
			return
		}
		if puuid == nil {
			name := ""
			if pname != nil {
				name = *pname
			}
			unaffiliated = append(unaffiliated, &models.UnaffiliatedDataOutput{UUID: uuid, Name: name, Contributions: contribs[uuid]})
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	sort.SliceStable(unaffiliated, func(i, j int) bool {
		return unaffiliated[i].Contributions > unaffiliated[j].Contributions
	})
	return
}

func (s *service) AddOrganization(inOrganization *models.OrganizationDataOutput, refresh bool, tx *sql.Tx) (organization *models.OrganizationDataOutput, err error) {
	log.Info(fmt.Sprintf("AddOrganization: inOrganization:%+v refresh:%v tx:%v", s.ToLocalOrganization(inOrganization), refresh, tx != nil))
	organization = inOrganization
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddOrganization(exit): inOrganization:%+v refresh:%v tx:%v organization:%+v err:%v",
				s.ToLocalOrganization(inOrganization),
				refresh,
				tx != nil,
				s.ToLocalOrganization(organization),
				err,
			),
		)
	}()
	organization.Name = strings.TrimSpace(organization.Name)
	// s.SetOrigin()
	_, err = s.Exec(
		s.db,
		tx,
		"insert into organizations(name) select ?",
		organization.Name,
	)
	if err != nil {
		organization = nil
		return
	}
	if refresh {
		organization, err = s.GetOrganizationByName(organization.Name, true, tx)
		if err != nil {
			organization = nil
			return
		}
	}
	return
}

func (s *service) AddUniqueIdentity(inUniqueIdentity *models.UniqueIdentityDataOutput, refresh bool, tx *sql.Tx) (uniqueIdentity *models.UniqueIdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("AddUniqueIdentity: inUniqueIdentity:%+v refresh:%v tx:%v", s.ToLocalUniqueIdentity(inUniqueIdentity), refresh, tx != nil))
	uniqueIdentity = inUniqueIdentity
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddUniqueIdentity(exit): inUniqueIdentity:%+v refresh:%v tx:%v uniqueIdentity:%+v err:%v",
				s.ToLocalUniqueIdentity(inUniqueIdentity),
				refresh,
				tx != nil,
				s.ToLocalUniqueIdentity(uniqueIdentity),
				err,
			),
		)
	}()
	uniqueIdentity.UUID = strings.TrimSpace(uniqueIdentity.UUID)
	if uniqueIdentity.LastModified == nil {
		uniqueIdentity.LastModified = s.Now()
	}
	// s.SetOrigin()
	_, err = s.Exec(
		s.db,
		tx,
		"insert into uidentities(uuid, last_modified) select ?, str_to_date(?, ?)",
		uniqueIdentity.UUID,
		uniqueIdentity.LastModified,
		DateTimeFormat,
	)
	if err != nil {
		uniqueIdentity = nil
		return
	}
	if refresh {
		uniqueIdentity, err = s.GetUniqueIdentity(uniqueIdentity.UUID, true, tx)
		if err != nil {
			uniqueIdentity = nil
			return
		}
	}
	return
}

func (s *service) FindUniqueIdentityOrganizations(uuid string, missingFatal bool, tx *sql.Tx) (organizations []*models.OrganizationDataOutput, err error) {
	log.Info(fmt.Sprintf("FindUniqueIdentityOrganizations: uuid:%s missingFatal:%v tx:%v", uuid, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"FindUniqueIdentityOrganizations(exit): uuid:%s missingFatal:%v tx:%v organizations:%+v err:%v",
				uuid,
				missingFatal,
				tx != nil,
				s.ToLocalOrganizations(organizations),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	sel := "select distinct o.id, o.name from organizations o, enrollments e where e.organization_id = o.id and e.uuid = ? order by o.name asc"
	rows, err := s.Query(sdb, tx, sel, uuid)
	if err != nil {
		return
	}
	for rows.Next() {
		organizationData := &models.OrganizationDataOutput{}
		err = rows.Scan(
			&organizationData.ID,
			&organizationData.Name,
		)
		if err != nil {
			return
		}
		organizations = append(organizations, organizationData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && len(organizations) == 0 {
		err = fmt.Errorf("cannot find organizations for uuid %s", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "FindUniqueIdenityOrganizations")
		return
	}
	return
}

func (s *service) FindProfiles(columns []string, values []interface{}, missingFatal bool, tx *sql.Tx) (profiles []*models.ProfileDataOutput, err error) {
	log.Info(fmt.Sprintf("FindProfiles: columns:%+v values:%+v missingFatal:%v tx:%v", columns, values, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"FindProfiles(exit): columns:%+v values:%+v missingFatal:%v tx:%v profiles:%+v err:%v",
				columns,
				values,
				missingFatal,
				tx != nil,
				s.ToLocalProfiles(profiles),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	sel := "select uuid, name, email, gender, gender_acc, is_bot, country_code from profiles"
	nColumns := len(columns)
	lastIndex := nColumns - 1
	if nColumns > 0 {
		sel += " where"
	}
	for index := range columns {
		column := columns[index]
		sel += " " + column + " = ?"
		if index < lastIndex {
			sel += " and"
		}
	}
	sel += " order by uuid asc"
	rows, err := s.Query(sdb, tx, sel, values...)
	if err != nil {
		return
	}
	for rows.Next() {
		profileData := &models.ProfileDataOutput{}
		err = rows.Scan(
			&profileData.UUID,
			&profileData.Name,
			&profileData.Email,
			&profileData.Gender,
			&profileData.GenderAcc,
			&profileData.IsBot,
			&profileData.CountryCode,
		)
		if err != nil {
			return
		}
		profiles = append(profiles, profileData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && len(profiles) == 0 {
		err = fmt.Errorf("cannot find profiles for %+v/%+v", columns, values)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "FindProfiles")
		return
	}
	return
}

func (s *service) FindOrganizations(columns []string, values []interface{}, missingFatal bool, tx *sql.Tx) (organizations []*models.OrganizationDataOutput, err error) {
	log.Info(fmt.Sprintf("FindOrganizations: columns:%+v values:%+v missingFatal:%v tx:%v", columns, values, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"FindOrganizations(exit): columns:%+v values:%+v missingFatal:%v tx:%v organizations:%+v err:%v",
				columns,
				values,
				missingFatal,
				tx != nil,
				s.ToLocalOrganizations(organizations),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	sel := "select id, name from organizations"
	nColumns := len(columns)
	lastIndex := nColumns - 1
	if nColumns > 0 {
		sel += " where"
	}
	for index := range columns {
		column := columns[index]
		sel += " " + column + " = ?"
		if index < lastIndex {
			sel += " and"
		}
	}
	sel += " order by name asc"
	rows, err := s.Query(sdb, tx, sel, values...)
	if err != nil {
		return
	}
	for rows.Next() {
		organizationData := &models.OrganizationDataOutput{}
		err = rows.Scan(
			&organizationData.ID,
			&organizationData.Name,
		)
		if err != nil {
			return
		}
		organizations = append(organizations, organizationData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && len(organizations) == 0 {
		err = fmt.Errorf("cannot find organizations for %+v/%+v", columns, values)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "FindOrganizations")
		return
	}
	return
}

func (s *service) FindEnrollmentsNested(columns []string, values []interface{}, isDate []bool, missingFatal bool, projectSlugs []string, tx *sql.Tx) (enrollments []*models.EnrollmentNestedDataOutput, err error) {
	log.Info(fmt.Sprintf("FindEnrollmentsNested: columns:%+v values:%+v isDate:%+v missingFatal:%v projectSlugs:%v tx:%v", columns, values, isDate, missingFatal, projectSlugs, tx != nil))
	// s.SetOrigin()
	defer func() {
		log.Info(
			fmt.Sprintf(
				"FindEnrollmentsNested(exit): columns:%+v values:%+v isDate:%+v missingFatal:%v projectSlugs:%+v tx:%v enrollments:%+v err:%v",
				columns,
				values,
				isDate,
				missingFatal,
				projectSlugs,
				tx != nil,
				s.ToLocalNestedEnrollments(enrollments),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	sel := "select e.id, e.uuid, e.start, e.end, e.project_slug, e.role, o.id, o.name from enrollments e, organizations o where e.organization_id = o.id"
	vals := []interface{}{}
	nColumns := len(columns)
	lastIndex := nColumns - 1
	if nColumns > 0 {
		sel += " and"
	}
	for index := range columns {
		column := columns[index]
		value := values[index]
		date := isDate[index]
		isNil := false
		if column == "e.project_slug" {
			v, ok := value.(*string)
			if ok {
				isNil = v == nil
			}
		}
		if isNil {
			sel += " " + column + " is null"
		} else {
			if date {
				sel += " " + column + " = str_to_date(?, ?)"
				vals = append(vals, value)
				vals = append(vals, DateTimeFormat)
			} else {
				sel += " " + column + " = ?"
				vals = append(vals, value)
			}
		}
		if index < lastIndex {
			sel += " and"
		}
	}
	if len(projectSlugs) > 0 {
		sel += " and (e.project_slug is null or e.project_slug in ("
		for _, projectSlug := range projectSlugs {
			sel += "?,"
			vals = append(vals, projectSlug)
		}
		sel = sel[0:len(sel)-1] + "))"
	}
	sel += " order by e.uuid asc, e.start asc, e.end asc"
	rows, err := s.Query(sdb, tx, sel, vals...)
	if err != nil {
		return
	}
	oName := ""
	for rows.Next() {
		enrollmentData := &models.EnrollmentNestedDataOutput{}
		err = rows.Scan(
			&enrollmentData.ID,
			&enrollmentData.UUID,
			&enrollmentData.Start,
			&enrollmentData.End,
			&enrollmentData.ProjectSlug,
			&enrollmentData.Role,
			&enrollmentData.OrganizationID,
			&oName,
		)
		if err != nil {
			return
		}
		enrollmentData.Organization = &models.OrganizationDataOutput{ID: enrollmentData.OrganizationID, Name: oName}
		enrollments = append(enrollments, enrollmentData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && len(enrollments) == 0 {
		err = fmt.Errorf("cannot find enrollments for %+v/%+v (nested)", columns, values)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "FindOrganizationsNested")
		return
	}
	return
}

func (s *service) FindEnrollments(columns []string, values []interface{}, isDate []bool, missingFatal bool, tx *sql.Tx) (enrollments []*models.EnrollmentDataOutput, err error) {
	log.Info(fmt.Sprintf("FindEnrollments: columns:%+v values:%+v isDate:%+v missingFatal:%v tx:%v", columns, values, isDate, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"FindEnrollments(exit): columns:%+v values:%+v isDate:%+v missingFatal:%v tx:%v enrollments:%+v err:%v",
				columns,
				values,
				isDate,
				missingFatal,
				tx != nil,
				s.ToLocalEnrollments(enrollments),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	sel := "select id, uuid, organization_id, start, end, project_slug, role from enrollments"
	vals := []interface{}{}
	nColumns := len(columns)
	lastIndex := nColumns - 1
	if nColumns > 0 {
		sel += " where"
	}
	for index := range columns {
		column := columns[index]
		value := values[index]
		date := isDate[index]
		isNil := false
		if column == "project_slug" {
			v, ok := value.(*string)
			if ok {
				isNil = v == nil
			}
		}
		if isNil {
			sel += " " + column + " is null"
		} else {
			if date {
				sel += " " + column + " = str_to_date(?, ?)"
				vals = append(vals, value)
				vals = append(vals, DateTimeFormat)
			} else {
				sel += " " + column + " = ?"
				vals = append(vals, value)
			}
		}
		if index < lastIndex {
			sel += " and"
		}
	}
	sel += " order by uuid asc, start asc, end asc"
	rows, err := s.Query(sdb, tx, sel, vals...)
	if err != nil {
		return
	}
	for rows.Next() {
		enrollmentData := &models.EnrollmentDataOutput{}
		err = rows.Scan(
			&enrollmentData.ID,
			&enrollmentData.UUID,
			&enrollmentData.OrganizationID,
			&enrollmentData.Start,
			&enrollmentData.End,
			&enrollmentData.ProjectSlug,
			&enrollmentData.Role,
		)
		if err != nil {
			return
		}
		enrollments = append(enrollments, enrollmentData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && len(enrollments) == 0 {
		err = fmt.Errorf("cannot find enrollments for %+v/%+v", columns, values)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "FindEnrollments")
		return
	}
	return
}

func (s *service) GetArchiveUniqueIdentityIdentities(uuid string, tm time.Time, missingFatal bool, tx *sql.Tx) (identities []*models.IdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("GetArchiveUniqueIdentityIdentities: uuid:%s tm:%v missingFatal:%v tx:%v", uuid, tm, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetArchiveUniqueIdentityIdentities(exit): uuid:%s tm:%v missingFatal:%v tx:%v identities:%+v err:%v",
				uuid,
				tm,
				missingFatal,
				tx != nil,
				s.ToLocalIdentities(identities),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	rows, err := s.Query(
		sdb,
		tx,
		"select id, uuid, source, name, email, username from identities_archive where uuid = ? and archived_at = ? order by id asc",
		uuid,
		tm,
	)
	if err != nil {
		return
	}
	now := s.Now()
	for rows.Next() {
		identityData := &models.IdentityDataOutput{}
		err = rows.Scan(
			&identityData.ID,
			&identityData.UUID,
			&identityData.Source,
			&identityData.Name,
			&identityData.Email,
			&identityData.Username,
		)
		if err != nil {
			return
		}
		identityData.LastModified = now
		identities = append(identities, identityData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && len(identities) == 0 {
		err = fmt.Errorf("cannot find archived identities for uuid '%s'/%v", uuid, tm)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetArchiveUniqueIdentityIdentities")
		return
	}
	return
}

func (s *service) GetArchiveUniqueIdentityEnrollments(uuid string, tm time.Time, missingFatal bool, tx *sql.Tx) (enrollments []*models.EnrollmentDataOutput, err error) {
	log.Info(fmt.Sprintf("GetArchiveUniqueIdentityEnrollments: uuid:%s tm:%v missingFatal:%v tx:%v", uuid, tm, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetArchiveUniqueIdentityEnrollments(exit): uuid:%s tm:%v missingFatal:%v tx:%v enrollments:%+v err:%v",
				uuid,
				tm,
				missingFatal,
				tx != nil,
				s.ToLocalEnrollments(enrollments),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	rows, err := s.Query(
		sdb,
		tx,
		"select id, uuid, organization_id, start, end, project_slug, role from enrollments_archive where uuid = ? and archived_at = ? order by start asc, end asc",
		uuid,
		tm,
	)
	if err != nil {
		return
	}
	for rows.Next() {
		enrollmentData := &models.EnrollmentDataOutput{}
		err = rows.Scan(
			&enrollmentData.ID,
			&enrollmentData.UUID,
			&enrollmentData.OrganizationID,
			&enrollmentData.Start,
			&enrollmentData.End,
			&enrollmentData.ProjectSlug,
			&enrollmentData.Role,
		)
		if err != nil {
			return
		}
		enrollments = append(enrollments, enrollmentData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && len(enrollments) == 0 {
		err = fmt.Errorf("cannot find archive enrollments for uuid '%s'/%v", uuid, tm)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetArchiveUniqueIdentityEnrollments")
		return
	}
	return
}

func (s *service) GetUniqueIdentityIdentities(uuid string, missingFatal bool, tx *sql.Tx) (identities []*models.IdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("GetUniqueIdentityIdentities: uuid:%s missingFatal:%v tx:%v", uuid, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetUniqueIdentityIdentities(exit): uuid:%s missingFatal:%v tx:%v identities:%+v err:%v",
				uuid,
				missingFatal,
				tx != nil,
				s.ToLocalIdentities(identities),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	rows, err := s.Query(
		sdb,
		tx,
		"select id, uuid, source, name, email, username, last_modified from identities where uuid = ? order by id asc",
		uuid,
	)
	if err != nil {
		return
	}
	for rows.Next() {
		identityData := &models.IdentityDataOutput{}
		err = rows.Scan(
			&identityData.ID,
			&identityData.UUID,
			&identityData.Source,
			&identityData.Name,
			&identityData.Email,
			&identityData.Username,
			&identityData.LastModified,
		)
		if err != nil {
			return
		}
		identities = append(identities, identityData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && len(identities) == 0 {
		err = fmt.Errorf("cannot find identities for uuid '%s'", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetUniqueIdentityIdentities")
		return
	}
	return
}

func (s *service) GetUniqueIdentityEnrollments(uuid string, missingFatal bool, tx *sql.Tx) (enrollments []*models.EnrollmentDataOutput, err error) {
	log.Info(fmt.Sprintf("GetUniqueIdentityEnrollments: uuid:%s missingFatal:%v tx:%v", uuid, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetUniqueIdentityEnrollments(exit): uuid:%s missingFatal:%v tx:%v enrollments:%+v err:%v",
				uuid,
				missingFatal,
				tx != nil,
				s.ToLocalEnrollments(enrollments),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	rows, err := s.Query(
		sdb,
		tx,
		"select id, uuid, organization_id, start, end, project_slug, role from enrollments where uuid = ? order by start asc, end asc",
		uuid,
	)
	if err != nil {
		return
	}
	for rows.Next() {
		enrollmentData := &models.EnrollmentDataOutput{}
		err = rows.Scan(
			&enrollmentData.ID,
			&enrollmentData.UUID,
			&enrollmentData.OrganizationID,
			&enrollmentData.Start,
			&enrollmentData.End,
			&enrollmentData.ProjectSlug,
			&enrollmentData.Role,
		)
		if err != nil {
			return
		}
		enrollments = append(enrollments, enrollmentData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && len(enrollments) == 0 {
		err = fmt.Errorf("cannot find enrollments for uuid '%s'", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetUniqueIdentityEnrollments")
		return
	}
	return
}

func (s *service) GetEnrollment(id int64, missingFatal bool, tx *sql.Tx) (enrollmentData *models.EnrollmentDataOutput, err error) {
	log.Info(fmt.Sprintf("GetEnrollment: id:%d missingFatal:%v tx:%v", id, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetEnrollment(exit): id:%d missingFatal:%v tx:%v enrollmentData:%+v err:%v",
				id,
				missingFatal,
				tx != nil,
				enrollmentData,
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	enrollmentData = &models.EnrollmentDataOutput{}
	rows, err := s.Query(
		sdb,
		tx,
		"select id, uuid, organization_id, start, end, project_slug, role from enrollments where id = ? limit 1",
		id,
	)
	if err != nil {
		return
	}
	fetched := false
	for rows.Next() {
		err = rows.Scan(
			&enrollmentData.ID,
			&enrollmentData.UUID,
			&enrollmentData.OrganizationID,
			&enrollmentData.Start,
			&enrollmentData.End,
			&enrollmentData.ProjectSlug,
			&enrollmentData.Role,
		)
		if err != nil {
			return
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && !fetched {
		err = fmt.Errorf("cannot find enrollment id '%d'", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetEnrollment")
		return
	}
	if !fetched {
		enrollmentData = nil
	}
	return
}

func (s *service) FetchMatchingBlacklist(email string, missingFatal bool, tx *sql.Tx) (matchingBlacklistData *models.MatchingBlacklistOutput, err error) {
	log.Info(fmt.Sprintf("FetchMatchingBlacklist: email:%s missingFatal:%v tx:%v", email, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"FetchMatchingBlacklist(exit): email:%s missingFatal:%v tx:%v matchingBlacklistData:%+v err:%v",
				email,
				missingFatal,
				tx != nil,
				matchingBlacklistData,
				err,
			),
		)
	}()
	matchingBlacklistData = &models.MatchingBlacklistOutput{}
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	rows, err := s.Query(
		sdb,
		tx,
		"select excluded from matching_blacklist where excluded = ? limit 1",
		email,
	)
	if err != nil {
		return
	}
	fetched := false
	for rows.Next() {
		err = rows.Scan(
			&matchingBlacklistData.Excluded,
		)
		if err != nil {
			return
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && !fetched {
		err = fmt.Errorf("cannot find matching blacklist email '%s'", email)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "FetchMatchingBlacklist")
		return
	}
	if !fetched {
		matchingBlacklistData = nil
	}
	return
}

func (s *service) GetOrganization(id int64, missingFatal bool, tx *sql.Tx) (organizationData *models.OrganizationDataOutput, err error) {
	log.Info(fmt.Sprintf("GetOrganization: id:%d missingFatal:%v tx:%v", id, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetOrganization(exit): id:%d missingFatal:%v tx:%v organizationData:%+v err:%v",
				id,
				missingFatal,
				tx != nil,
				s.ToLocalOrganization(organizationData),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	organizationData = &models.OrganizationDataOutput{}
	rows, err := s.Query(
		sdb,
		tx,
		"select id, name from organizations where id = ? limit 1",
		id,
	)
	if err != nil {
		return
	}
	fetched := false
	for rows.Next() {
		err = rows.Scan(
			&organizationData.ID,
			&organizationData.Name,
		)
		if err != nil {
			return
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && !fetched {
		err = fmt.Errorf("cannot find organization id %d", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetOrganization")
		return
	}
	if !fetched {
		organizationData = nil
	}
	return
}

func (s *service) GetOrganizationByName(orgName string, missingFatal bool, tx *sql.Tx) (organizationData *models.OrganizationDataOutput, err error) {
	log.Info(fmt.Sprintf("GetOrganizationByName: orgName:%s missingFatal:%v tx:%v", orgName, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetOrganizationByName(exit): orgName:%s missingFatal:%v tx:%v organizationData:%+v err:%v",
				orgName,
				missingFatal,
				tx != nil,
				s.ToLocalOrganization(organizationData),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	organizationData = &models.OrganizationDataOutput{}
	rows, err := s.Query(
		sdb,
		tx,
		"select id, name from organizations where name = ? limit 1",
		orgName,
	)
	if err != nil {
		return
	}
	fetched := false
	for rows.Next() {
		err = rows.Scan(
			&organizationData.ID,
			&organizationData.Name,
		)
		if err != nil {
			return
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && !fetched {
		err = fmt.Errorf("cannot find organization name '%s'", orgName)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetOrganizationByName")
		return
	}
	if !fetched {
		organizationData = nil
	}
	return
}

func (s *service) GetUniqueIdentity(uuid string, missingFatal bool, tx *sql.Tx) (uniqueIdentityData *models.UniqueIdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("GetUniqueIdentity: uuid:%s missingFatal:%v tx:%v", uuid, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetUniqueIdentity(exit): uuid:%s missingFatal:%v tx:%v uniqueIdentityData:%+v err:%v",
				uuid,
				missingFatal,
				tx != nil,
				s.ToLocalUniqueIdentity(uniqueIdentityData),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	uniqueIdentityData = &models.UniqueIdentityDataOutput{}
	rows, err := s.Query(
		sdb,
		tx,
		"select uuid, last_modified from uidentities where uuid = ? limit 1",
		uuid,
	)
	if err != nil {
		return
	}
	fetched := false
	for rows.Next() {
		err = rows.Scan(
			&uniqueIdentityData.UUID,
			&uniqueIdentityData.LastModified,
		)
		if err != nil {
			return
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && !fetched {
		err = fmt.Errorf("cannot find unique identity uuid '%s'", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetUniqueIdentity")
		return
	}
	if !fetched {
		uniqueIdentityData = nil
	}
	return
}

func (s *service) GetIdentity(id string, missingFatal bool, tx *sql.Tx) (identityData *models.IdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("GetIdentity: id:%s missingFatal:%v tx:%v", id, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetIdentity(exit): id:%s missingFatal:%v tx:%v identityData:%+v err:%v",
				id,
				missingFatal,
				tx != nil,
				s.ToLocalIdentity(identityData),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	identityData = &models.IdentityDataOutput{}
	rows, err := s.Query(
		sdb,
		tx,
		"select id, uuid, source, name, username, email, last_modified from identities where id = ? limit 1",
		id,
	)
	if err != nil {
		return
	}
	fetched := false
	for rows.Next() {
		err = rows.Scan(
			&identityData.ID,
			&identityData.UUID,
			&identityData.Source,
			&identityData.Name,
			&identityData.Username,
			&identityData.Email,
			&identityData.LastModified,
		)
		if err != nil {
			return
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && !fetched {
		err = fmt.Errorf("cannot find identity id '%s'", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetIdentity")
		return
	}
	if !fetched {
		identityData = nil
	}
	return
}

func (s *service) GetProfile(uuid string, missingFatal bool, tx *sql.Tx) (profileData *models.ProfileDataOutput, err error) {
	log.Info(fmt.Sprintf("GetProfile: uuid:%s missignFatal:%v tx:%v", uuid, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetProfile(exit): uuid:%s missignFatal:%v tx:%v profileData:%+v err:%v",
				uuid,
				missingFatal,
				tx != nil,
				s.ToLocalProfile(profileData),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	profileData = &models.ProfileDataOutput{}
	rows, err := s.Query(
		sdb,
		tx,
		"select uuid, name, email, gender, gender_acc, is_bot, country_code from profiles where uuid = ? limit 1",
		uuid,
	)
	if err != nil {
		return
	}
	fetched := false
	for rows.Next() {
		err = rows.Scan(
			&profileData.UUID,
			&profileData.Name,
			&profileData.Email,
			&profileData.Gender,
			&profileData.GenderAcc,
			&profileData.IsBot,
			&profileData.CountryCode,
		)
		if err != nil {
			return
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && !fetched {
		err = fmt.Errorf("cannot find profile uuid '%s'", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetProfile")
		return
	}
	if !fetched {
		profileData = nil
	}
	return
}

func (s *service) TouchIdentity(id string, tx *sql.Tx) (affected int64, err error) {
	log.Info(fmt.Sprintf("TouchIdentity: id:%s tx:%v", id, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("TouchIdentity(exit): id:%s tx:%v affected:%d err:%v", id, tx != nil, affected, err))
	}()
	// s.SetOrigin()
	res, err := s.Exec(s.db, tx, "update identities set last_modified = ? where id = ?", time.Now(), id)
	if err != nil {
		return
	}
	affected, err = res.RowsAffected()
	return
}

func (s *service) TouchUniqueIdentity(uuid string, tx *sql.Tx) (affected int64, err error) {
	log.Info(fmt.Sprintf("TouchUniqueIdentity: uuid:%s tx:%v", uuid, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("TouchUniqueIdentity(exit): uuid:%s tx:%v affected:%d err:%v", uuid, tx != nil, affected, err))
	}()
	// s.SetOrigin()
	res, err := s.Exec(s.db, tx, "update uidentities set last_modified = ? where uuid = ?", time.Now(), uuid)
	if err != nil {
		return
	}
	affected, err = res.RowsAffected()
	return
}

func (s *service) DeleteUniqueIdentityArchive(uuid string, missingFatal, onlyLast bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DeleteUniqueIdentityArchive: uuid:%s missingFatal:%v onlyLast:%v tm:%v tx:%v", uuid, missingFatal, onlyLast, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DeleteUniqueIdentityArchive(exit): uuid:%s missingFatal:%v onlyLast:%v tm:%v tx:%v err:%v", uuid, missingFatal, onlyLast, tm, tx != nil, err))
	}()
	var res sql.Result
	if tm != nil {
		del := "delete from uidentities_archive where uuid = ? and archived_at = ?"
		res, err = s.Exec(s.db, tx, del, uuid, tm)
	} else {
		if onlyLast {
			del := "delete from uidentities_archive where uuid = ? and archived_at = (" +
				"select max(archived_at) from uidentities_archive where uuid = ?)"
			res, err = s.Exec(s.db, tx, del, uuid, uuid)
		} else {
			del := "delete from uidentities_archive where uuid = ?"
			res, err = s.Exec(s.db, tx, del, uuid)
		}
	}
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting archived unique identity uuid '%s' had no effect", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DeleteUniqueIdentityArchive")
		return
	}
	return
}

func (s *service) UnarchiveUniqueIdentity(uuid string, replace bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("UnarchiveUniqueIdentity: uuid:%s replace:%v tm:%v tx:%v", uuid, replace, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("UnarchiveUniqueIdentity(exit): uuid:%s replace:%v tm:%v tx:%v err:%v", uuid, replace, tm, tx != nil, err))
	}()
	uuid = strings.TrimSpace(uuid)
	if replace {
		err = s.DeleteUniqueIdentity(uuid, false, false, nil, tx)
		if err != nil {
			return
		}
	}
	var res sql.Result
	// s.SetOrigin()
	if tm != nil {
		insert := "insert into uidentities(uuid, last_modified) " +
			"select uuid, now() from uidentities_archive " +
			"where uuid = ? and archived_at = ?"
		res, err = s.Exec(s.db, tx, insert, uuid, tm)
	} else {
		insert := "insert into uidentities(uuid, last_modified) " +
			"select uuid, now() from uidentities_archive " +
			"where uuid = ? order by archived_at desc limit 1"
		res, err = s.Exec(s.db, tx, insert, uuid)
	}
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if affected == 0 {
		err = fmt.Errorf("unachiving unique identity uuid '%s' created no data", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "UnarchiveUniqueIdentity")
		return
	}
	err = s.DeleteUniqueIdentityArchive(uuid, true, tm == nil, tm, tx)
	if err != nil {
		return
	}
	return
}

func (s *service) ArchiveUniqueIdentity(uuid string, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("ArchiveUniqueIdentity: uuid:%s tm:%v tx:%v", uuid, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("ArchiveUniqueIdentity(exit): uuid:%s tm:%v tx:%v err:%v", uuid, tm, tx != nil, err))
	}()
	uuid = strings.TrimSpace(uuid)
	if tm == nil {
		t := time.Now()
		tm = &t
	}
	insert := "insert into uidentities_archive(uuid, last_modified, archived_at) " +
		"select uuid, last_modified, ? from uidentities where uuid = ? limit 1"
	res, err := s.Exec(s.db, tx, insert, tm, uuid)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if affected == 0 {
		err = fmt.Errorf("archiving unique identity uuid '%s' created no data", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ArchiveUniqueIdentity")
		return
	}
	return
}

func (s *service) DropOrganization(id int64, missingFatal bool, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DropOrganization: id:%d missingFatal:%v tx:%v", id, missingFatal, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DropOrganization(exit): id:%d missingFatal:%v tx:%v err:%v", id, missingFatal, tx != nil, err))
	}()
	del := "delete from organizations where id = ?"
	// s.SetOrigin()
	res, err := s.Exec(s.db, tx, del, id)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting organization id %d had no effect", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DropOrganization")
		return
	}
	return
}

func (s *service) DropOrgDomain(organization, domain string, missingFatal bool, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DropOrgDomain: organization:%s domain:%s missingFatal:%v tx:%v", organization, domain, missingFatal, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DropOrgDomain(exit): organization:%s domain:%s missingFatal:%v tx:%v err:%v", organization, domain, missingFatal, tx != nil, err))
	}()
	del := "delete from domains_organizations where organization_id in ("
	del += "select id from organizations where name = ?) and domain = ?"
	// s.SetOrigin()
	res, err := s.Exec(s.db, tx, del, organization, domain)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting organization '%s' domain '%s' had no effect", organization, domain)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DropOrgDomain")
		return
	}
	return
}

func (s *service) DropMatchingBlacklist(email string, missingFatal bool, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DropMatchingBlacklist: email:%s missingFatal:%v tx:%v", email, missingFatal, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DropMatchingBlacklist(exit): email:%s missingFatal:%v tx:%v err:%v", email, missingFatal, tx != nil, err))
	}()
	del := "delete from matching_blacklist where excluded = ?"
	res, err := s.Exec(s.db, tx, del, email)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting matching blacklist email '%s' had no effect", email)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DropMatchingBlacklist")
		return
	}
	return
}

func (s *service) DeleteUniqueIdentity(uuid string, archive, missingFatal bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DeleteUniqueIdentity: uuid:%s archive:%v missingFatal:%v tm:%v tx:%v", uuid, archive, missingFatal, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DeleteUniqueIdentity(exit): uuid:%s archive:%v missingFatal:%v tm:%v tx:%v err:%v", uuid, archive, missingFatal, tm, tx != nil, err))
	}()
	if archive {
		err = s.ArchiveUniqueIdentity(uuid, tm, tx)
		if err != nil {
			return
		}
	}
	del := "delete from uidentities where uuid = ?"
	// s.SetOrigin()
	res, err := s.Exec(s.db, tx, del, uuid)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting unique identity uuid '%s' had no effect", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DeleteUniqueIdentity")
		return
	}
	return
}

func (s *service) DeleteEnrollmentArchive(id int64, missingFatal, onlyLast bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DeleteEnrollmentArchive: id:%d missingFatal:%v onlyLast:%v tm:%v tx:%v", id, missingFatal, onlyLast, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DeleteEnrollmentArchive(exit): id:%d missingFatal:%v onlyLast:%v tm:%v tx:%v err:%v", id, missingFatal, onlyLast, tm, tx != nil, err))
	}()
	var res sql.Result
	if tm != nil {
		del := "delete from enrollments_archive where id = ? and archived_at = ?"
		res, err = s.Exec(s.db, tx, del, id, tm)
	} else {
		if onlyLast {
			del := "delete from enrollments_archive where id = ? and archived_at = (" +
				"select max(archived_at) from enrollments_archive where id = ?)"
			res, err = s.Exec(s.db, tx, del, id, id)
		} else {
			del := "delete from enrollments_archive where id = ?"
			res, err = s.Exec(s.db, tx, del, id)
		}
	}
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting archived enrollment id '%d' had no effect", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DeleteEnrollmentArchive")
		return
	}
	return
}

func (s *service) UnarchiveEnrollment(id int64, replace bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("UnarchiveEnrollment: id:%d replace:%v tm:%v tx:%v", id, replace, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("UnarchiveEnrollment(exit): id:%d replace:%v tm:%v tx:%v err:%v", id, replace, tm, tx != nil, err))
	}()
	if replace {
		err = s.DeleteEnrollment(id, false, false, nil, tx)
		if err != nil {
			return
		}
	}
	var res sql.Result
	// s.SetOrigin()
	if tm != nil {
		insert := "insert into enrollments(id, uuid, organization_id, start, end, project_slug, role) " +
			"select id, uuid, organization_id, start, end, project_slug, role from enrollments_archive " +
			"where id = ? and archived_at = ?"
		res, err = s.Exec(s.db, tx, insert, id, tm)
	} else {
		insert := "insert into enrollments(id, uuid, organization_id, start, end, project_slug, role) " +
			"select id, uuid, organization_id, start, end, project_slug, role from enrollments_archive " +
			"where id = ? order by archived_at desc limit 1"
		res, err = s.Exec(s.db, tx, insert, id)
	}
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if affected == 0 {
		err = fmt.Errorf("unachiving enrollment id '%d' created no data", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "UnarchiveEnrollment")
		return
	}
	err = s.DeleteEnrollmentArchive(id, true, tm == nil, tm, tx)
	if err != nil {
		return
	}
	return
}

func (s *service) ArchiveEnrollment(id int64, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("ArchiveEnrollment: id:%d tm:%v tx:%v", id, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("ArchiveEnrollment(exit): id:%d tm:%v tx:%v err:%v", id, tm, tx != nil, err))
	}()
	if tm == nil {
		t := time.Now()
		tm = &t
	}
	insert := "insert into enrollments_archive(id, uuid, organization_id, start, end, project_slug, role, archived_at) " +
		"select id, uuid, organization_id, start, end, project_slug, role, ? from enrollments where id = ? limit 1"
	res, err := s.Exec(s.db, tx, insert, tm, id)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if affected == 0 {
		err = fmt.Errorf("archiving enrollment id '%d' created no data", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ArchiveEnrollment")
		return
	}
	return
}

func (s *service) WithdrawEnrollment(enrollment *models.EnrollmentDataOutput, missingFatal bool, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("WithdrawEnrollment: enrollment:%+v missingFatal:%v tx:%v", enrollment, missingFatal, tx != nil))
	// s.SetOrigin()
	defer func() {
		log.Info(fmt.Sprintf("WithdrawEnrollment(exit): enrollment:%+v missingFatal:%v tx:%v err:%v", enrollment, missingFatal, tx != nil, err))
	}()
	// s.SetOrigin()
	var res sql.Result
	if enrollment.ProjectSlug == nil {
		del := "delete from enrollments where uuid = ? and organization_id = ? and start >= str_to_date(?, ?) and end <= str_to_date(?, ?) and project_slug is null and role = ?"
		res, err = s.Exec(
			s.db,
			tx,
			del,
			enrollment.UUID,
			enrollment.OrganizationID,
			enrollment.Start,
			DateTimeFormat,
			enrollment.End,
			DateTimeFormat,
			enrollment.Role,
		)
	} else {
		del := "delete from enrollments where uuid = ? and organization_id = ? and start >= str_to_date(?, ?) and end <= str_to_date(?, ?) and project_slug = ? and role = ?"
		res, err = s.Exec(
			s.db,
			tx,
			del,
			enrollment.UUID,
			enrollment.OrganizationID,
			enrollment.Start,
			DateTimeFormat,
			enrollment.End,
			DateTimeFormat,
			enrollment.ProjectSlug,
			enrollment.Role,
		)
	}
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting enrollment id '%+v' had no effect", enrollment)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "WithdrawEnrollment")
		return
	}
	return
}

func (s *service) DeleteEnrollment(id int64, archive, missingFatal bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DeleteEnrollment: id:%d archive:%v missingFatal:%v tm:%v tx:%v", id, archive, missingFatal, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DeleteEnrollment(exit): id:%d archive:%v missingFatal:%v tm:%v tx:%v err:%v", id, archive, missingFatal, tm, tx != nil, err))
	}()
	if archive {
		err = s.ArchiveEnrollment(id, tm, tx)
		if err != nil {
			return
		}
	}
	del := "delete from enrollments where id = ?"
	// s.SetOrigin()
	res, err := s.Exec(s.db, tx, del, id)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting enrollment id '%d' had no effect", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DeleteEnrollment")
		return
	}
	return
}

func (s *service) DeleteIdentityArchive(id string, missingFatal, onlyLast bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DeleteIdentityArchive: id:%s missingFatal:%v onlyLast:%v tm:%v tx:%v", id, missingFatal, onlyLast, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DeleteIdentityArchive(exit): id:%s missingFatal:%v onlyLast:%v tm:%v tx:%v err:%v", id, missingFatal, onlyLast, tm, tx != nil, err))
	}()
	var res sql.Result
	if tm != nil {
		del := "delete from identities_archive where id = ? and archived_at = ?"
		res, err = s.Exec(s.db, tx, del, id, tm)
	} else {
		if onlyLast {
			del := "delete from identities_archive where id = ? and archived_at = (" +
				"select max(archived_at) from identities_archive where id = ?)"
			res, err = s.Exec(s.db, tx, del, id, id)
		} else {
			del := "delete from identities_archive where id = ?"
			res, err = s.Exec(s.db, tx, del, id)
		}
	}
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting archived identity id '%s' had no effect", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DeleteIdentityArchive")
		return
	}
	return
}

func (s *service) UnarchiveIdentity(id string, replace bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("UnarchiveIdentity: id:%s replace:%v tm:%v tx:%v", id, replace, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("UnarchiveIdentity(exit): id:%s replace:%v tm:%v tx:%v err:%v", id, replace, tm, tx != nil, err))
	}()
	if replace {
		err = s.DeleteIdentity(id, false, false, nil, tx)
		if err != nil {
			return
		}
	}
	var res sql.Result
	// s.SetOrigin()
	if tm != nil {
		insert := "insert into identities(id, uuid, source, name, email, username, last_modified) " +
			"select id, uuid, source, name, email, username, now() from identities_archive " +
			"where id = ? and archived_at = ?"
		res, err = s.Exec(s.db, tx, insert, id, tm)
	} else {
		insert := "insert into identities(id, uuid, source, name, email, username, last_modified) " +
			"select id, uuid, source, name, email, username, now() from identities_archive " +
			"where id = ? order by archived_at desc limit 1"
		res, err = s.Exec(s.db, tx, insert, id)
	}
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if affected == 0 {
		err = fmt.Errorf("unachiving identity id '%s' created no data", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "UnarchiveIdentity")
		return
	}
	err = s.DeleteIdentityArchive(id, true, tm == nil, tm, tx)
	if err != nil {
		return
	}
	return
}

func (s *service) ArchiveIdentity(id string, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("ArchiveIdentity: id:%s tm:%v tx:%v", id, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("ArchiveIdentity(exit): id:%s tm:%v tx:%v err:%v", id, tm, tx != nil, err))
	}()
	if tm == nil {
		t := time.Now()
		tm = &t
	}
	insert := "insert into identities_archive(id, uuid, source, name, email, username, last_modified, archived_at) " +
		"select id, uuid, source, name, email, username, last_modified, ? from identities where id = ? limit 1"
	res, err := s.Exec(s.db, tx, insert, tm, id)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if affected == 0 {
		err = fmt.Errorf("archiving identity id '%s' created no data", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ArchiveIdentity")
		return
	}
	return
}

func (s *service) DeleteIdentity(id string, archive, missingFatal bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DeleteIdentity: id:%s archive:%v missingFatal:%v tm:%v tx:%v", id, archive, missingFatal, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DeleteIdentity(exit): id:%s archive:%v missingFatal:%v tm:%v tx:%v err:%v", id, archive, missingFatal, tm, tx != nil, err))
	}()
	if archive {
		err = s.ArchiveIdentity(id, tm, tx)
		if err != nil {
			return
		}
	}
	del := "delete from identities where id = ?"
	// s.SetOrigin()
	res, err := s.Exec(s.db, tx, del, id)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting identity id '%s' had no effect", id)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DeleteIdentity")
		return
	}
	return
}

func (s *service) DeleteProfileArchive(uuid string, missingFatal, onlyLast bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DeleteProfileArchive: uuid:%s missingFatal:%v onlyLast:%v tm:%v tx:%v", uuid, missingFatal, onlyLast, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DeleteProfileArchive(exit): uuid:%s missingFatal:%v onlyLast:%v tm:%v tx:%v err:%v", uuid, missingFatal, onlyLast, tm, tx != nil, err))
	}()
	var res sql.Result
	if tm != nil {
		del := "delete from profiles_archive where uuid = ? and archived_at = ?"
		res, err = s.Exec(s.db, tx, del, uuid, tm)
	} else {
		if onlyLast {
			del := "delete from profiles_archive where uuid = ? and archived_at = (" +
				"select max(archived_at) from profiles_archive where uuid = ?)"
			res, err = s.Exec(s.db, tx, del, uuid, uuid)
		} else {
			del := "delete from profiles_archive where uuid = ?"
			res, err = s.Exec(s.db, tx, del, uuid)
		}
	}
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting archived profile uuid '%s' had no effect", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DeleteProfileArchive")
		return
	}
	return
}

func (s *service) UnarchiveProfile(uuid string, replace bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("UnarchiveProfile: uuid:%s replace:%v tm:%v tx:%v", uuid, replace, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("UnarchiveProfile(exit): uuid:%s replace:%v tm:%v tx:%v err:%v", uuid, replace, tm, tx != nil, err))
	}()
	uuid = strings.TrimSpace(uuid)
	if replace {
		err = s.DeleteProfile(uuid, false, false, nil, tx)
		if err != nil {
			return
		}
	}
	var res sql.Result
	// s.SetOrigin()
	if tm != nil {
		insert := "insert into profiles(uuid, name, email, gender, gender_acc, is_bot, country_code) " +
			"select uuid, name, email, gender, gender_acc, is_bot, country_code from profiles_archive " +
			"where uuid = ? and archived_at = ?"
		res, err = s.Exec(s.db, tx, insert, uuid, tm)
	} else {
		insert := "insert into profiles(uuid, name, email, gender, gender_acc, is_bot, country_code) " +
			"select uuid, name, email, gender, gender_acc, is_bot, country_code from profiles_archive " +
			"where uuid = ? order by archived_at desc limit 1"
		res, err = s.Exec(s.db, tx, insert, uuid)
	}
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if affected == 0 {
		err = fmt.Errorf("unachiving profile uuid '%s' created no data", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "UnarchiveProfile")
		return
	}
	err = s.DeleteProfileArchive(uuid, true, tm == nil, tm, tx)
	if err != nil {
		return
	}
	return
}

func (s *service) ArchiveProfile(uuid string, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("ArchiveProfile: uuid:%s tm:%v tx:%v", uuid, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("ArchiveProfile(exit): uuid:%s tm:%v tx:%v err:%v", uuid, tm, tx != nil, err))
	}()
	uuid = strings.TrimSpace(uuid)
	if tm == nil {
		t := time.Now()
		tm = &t
	}
	insert := "insert into profiles_archive(uuid, name, email, gender, gender_acc, is_bot, country_code, archived_at) " +
		"select uuid, name, email, gender, gender_acc, is_bot, country_code, ? from profiles where uuid = ? limit 1"
	res, err := s.Exec(s.db, tx, insert, tm, uuid)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if affected == 0 {
		err = fmt.Errorf("archiving profile uuid '%s' created no data", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ArchiveProfile")
		return
	}
	return
}

func (s *service) DeleteProfile(uuid string, archive, missingFatal bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DeleteProfile: uuid:%s archive:%v missingFatal:%v tm:%v tx:%v", uuid, archive, missingFatal, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DeleteProfile(exit): uuid:%s archive:%v missingFatal:%v tm:%v tx:%v err:%v", uuid, archive, missingFatal, tm, tx != nil, err))
	}()
	if archive {
		err = s.ArchiveProfile(uuid, tm, tx)
		if err != nil {
			return
		}
	}
	del := "delete from profiles where uuid = ?"
	// s.SetOrigin()
	res, err := s.Exec(s.db, tx, del, uuid)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting profile uuid '%s' had no effect", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DeleteProfile")
		return
	}
	return
}

func (s *service) ValidateProfile(profileData *models.ProfileDataOutput, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("ValidateProfile: profileData:%+v tx:%v", profileData, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("ValidateProfile(exit): profileData:%+v tx:%v err:%v", profileData, tx != nil, err))
	}()
	if profileData.UUID == "" {
		err = fmt.Errorf("profile '%+v' uuid is empty", s.ToLocalProfile(profileData))
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateProfile")
		profileData = nil
		return
	}
	if profileData.IsBot != nil && (*profileData.IsBot != 0 && *profileData.IsBot != 1) {
		err = fmt.Errorf("profile '%+v' is_bot should be '0' or '1'", s.ToLocalProfile(profileData))
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateProfile")
		return
	}
	if profileData.CountryCode != nil && *profileData.CountryCode != "" {
		_, err = s.GetCountry(*profileData.CountryCode, tx)
		if err != nil {
			return
		}
	}
	if profileData.Gender != nil {
		if *profileData.Gender != "male" && *profileData.Gender != "female" {
			err = fmt.Errorf("profile '%+v' gender should be 'male' or 'female'", s.ToLocalProfile(profileData))
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateProfile")
			return
		}
		if profileData.GenderAcc != nil && (*profileData.GenderAcc < 1 || *profileData.GenderAcc > 100) {
			err = fmt.Errorf("profile '%+v' gender_acc should be within [1, 100]", s.ToLocalProfile(profileData))
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateProfile")
			return
		}
	}
	if profileData.Gender == nil && profileData.GenderAcc != nil {
		err = fmt.Errorf("profile '%+v' gender_acc can only be set when gender is given", s.ToLocalProfile(profileData))
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateProfile")
		return
	}
	return
}

func (s *service) ValidateOrganization(organizationData *models.OrganizationDataOutput, forUpdate bool) (err error) {
	log.Info(fmt.Sprintf("ValidateOrganization: organizationData:%+v forUpdate:%v", organizationData, forUpdate))
	defer func() {
		log.Info(fmt.Sprintf("ValidateOrganization(exit): organizationData:%+v forUpdate:%v err:%v", organizationData, forUpdate, err))
	}()
	if forUpdate && organizationData.ID < 1 {
		err = fmt.Errorf("organization '%+v' missing id", organizationData)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateOrganization")
		return
	}
	if organizationData.Name == "" {
		err = fmt.Errorf("organization '%+v' missing name", organizationData)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateOrganization")
		return
	}
	return
}

func (s *service) ValidateEnrollment(enrollmentData *models.EnrollmentDataOutput, forUpdate bool) (err error) {
	log.Info(fmt.Sprintf("ValidateEnrollment: enrollmentData:%+v forUpdate:%v", enrollmentData, forUpdate))
	defer func() {
		log.Info(fmt.Sprintf("ValidateEnrollment(exit): enrollmentData:%+v forUpdate:%v err:%v", enrollmentData, forUpdate, err))
	}()
	if enrollmentData.Role == "" {
		enrollmentData.Role = shared.DefaultRole
	}
	correctRole := false
	for _, role := range shared.Roles {
		if enrollmentData.Role == role {
			correctRole = true
			break
		}
	}
	if !correctRole {
		err = fmt.Errorf("enrollment '%+v' incorrect role, allowed: %+v", enrollmentData, shared.Roles)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateEnrollment")
		return
	}
	if forUpdate && enrollmentData.ID < 1 {
		err = fmt.Errorf("enrollment '%+v' missing id", enrollmentData)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateEnrollment")
		return
	}
	if enrollmentData.ProjectSlug != nil && *(enrollmentData.ProjectSlug) == "" {
		err = fmt.Errorf("enrollment '%+v' project_slug must be null or non-empty string", enrollmentData)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateEnrollment")
		return
	}
	if enrollmentData.UUID == "" || enrollmentData.OrganizationID < 1 {
		err = fmt.Errorf("enrollment '%+v' missing uuid or organization_id", enrollmentData)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateEnrollment")
		return
	}
	if time.Time(enrollmentData.Start).Before(shared.MinPeriodDate) || time.Time(enrollmentData.Start).After(shared.MaxPeriodDate) {
		err = fmt.Errorf("enrollment '%+v' start date must be between %v and %v", enrollmentData, shared.MinPeriodDate, shared.MaxPeriodDate)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateEnrollment")
		return
	}
	if time.Time(enrollmentData.End).Before(shared.MinPeriodDate) || time.Time(enrollmentData.End).After(shared.MaxPeriodDate) {
		err = fmt.Errorf("enrollment '%+v' end date must be between %v and %v", enrollmentData, shared.MinPeriodDate, shared.MaxPeriodDate)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateEnrollment")
		return
	}
	if time.Time(enrollmentData.End).Before(time.Time(enrollmentData.Start)) {
		err = fmt.Errorf("enrollment '%+v' end date must be after start date", enrollmentData)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateEnrollment")
		return
	}
	return
}

func (s *service) ValidateIdentity(identityData *models.IdentityDataOutput, forUpdate bool) (err error) {
	log.Info(fmt.Sprintf("ValidateIdentity: identityData:%+v forUpdate:%v", s.ToLocalIdentity(identityData), forUpdate))
	defer func() {
		log.Info(fmt.Sprintf("ValidateIdentity(exit): identityData:%+v forUpdate:%v err:%v", s.ToLocalIdentity(identityData), forUpdate, err))
	}()
	if forUpdate && identityData.ID == "" {
		err = fmt.Errorf("identity '%+v' missing id", s.ToLocalIdentity(identityData))
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateIdentity")
		return
	}
	if !forUpdate {
		if identityData.Source == "" {
			err = fmt.Errorf("identity '%+v' missing source", s.ToLocalIdentity(identityData))
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateIdentity")
			return
		}
		if (identityData.Name == nil || (identityData.Name != nil && *(identityData.Name) == "")) &&
			(identityData.Email == nil || (identityData.Email != nil && *(identityData.Email) == "")) &&
			(identityData.Username == nil || (identityData.Username != nil && *(identityData.Username) == "")) {
			err = fmt.Errorf("identity '%+v' you need to set at least one of (name, email, username)", s.ToLocalIdentity(identityData))
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ValidateIdentity")
			return
		}
		return
	}
	return
}

func (s *service) ProfileUUIDHash(profile *models.ProfileDataOutput) (idHash string, err error) {
	log.Info(fmt.Sprintf("ProfileUUIDHash: profile:%+v", s.ToLocalProfile(profile)))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"ProfileUUIDHash(exit): profile:%+v idHash:%s err:%v",
				s.ToLocalProfile(profile),
				idHash,
				err,
			),
		)
	}()
	stripF := func(str string) string {
		isOk := func(r rune) bool {
			return r < 32 || r >= 127
		}
		t := transform.Chain(norm.NFKD, transform.RemoveFunc(isOk))
		str, _, _ = transform.String(t, str)
		return str
	}
	name := ""
	if profile.Name != nil {
		name = *(profile.Name)
	}
	email := ""
	if profile.Email != nil {
		email = *(profile.Email)
	}
	gender := ""
	if profile.Gender != nil {
		gender = *(profile.Gender)
	}
	genderAcc := ""
	if profile.GenderAcc != nil {
		genderAcc = fmt.Sprintf("%d", *(profile.GenderAcc))
	}
	isBot := ""
	if profile.IsBot != nil {
		isBot = fmt.Sprintf("%d", *(profile.IsBot))
	}
	countryCode := ""
	if profile.CountryCode != nil {
		countryCode = *(profile.CountryCode)
	}
	arg := stripF(name) + ":" + stripF(email) + ":" + stripF(gender) + ":" + genderAcc + ":" + isBot + ":" + stripF(countryCode)
	hash := sha1.New()
	_, err = hash.Write([]byte(arg))
	if err != nil {
		return
	}
	idHash = hex.EncodeToString(hash.Sum(nil))
	return
}

// ToLowerAndNone takes a string if it is empty it is set to none
// else it's contents are changed to the lower case
func ToLowerAndNone(value string) string {
	if value == "" {
		return "none"
	}
	return strings.ToLower(value)
}

func (s *service) IdentityIDHash(identity *models.IdentityDataOutput) (idHash string, err error) {
	log.Info(fmt.Sprintf("IdentityIDHash: identity:%+v", s.ToLocalIdentity(identity)))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"IdentityIDHash(exit): identity:%+v idHash:%s err:%v",
				s.ToLocalIdentity(identity),
				idHash,
				err,
			),
		)
	}()
	stripF := func(str string) string {
		isOk := func(r rune) bool {
			return r < 32 || r >= 127
		}
		t := transform.Chain(norm.NFKD, transform.RemoveFunc(isOk))
		str, _, _ = transform.String(t, str)
		return str
	}
	email := ""
	if identity.Email != nil {
		email = *(identity.Email)
	}
	name := ""
	if identity.Name != nil {
		name = *(identity.Name)
	}
	username := ""
	if identity.Username != nil {
		username = *(identity.Username)
	}
	if identity.Source == "" {
		err = fmt.Errorf("identity without source is not allowed")
		return
	}
	if email == "" && name == "" && username == "" {
		err = fmt.Errorf("identity data(name, email, username) can not be empty")
		return
	}
	email = ToLowerAndNone(email)
	name = ToLowerAndNone(name)
	username = ToLowerAndNone(username)
	arg := stripF(identity.Source) + ":" + stripF(email) + ":" + stripF(name) + ":" + stripF(username)
	hash := sha1.New()
	_, err = hash.Write([]byte(arg))
	if err != nil {
		return
	}
	idHash = hex.EncodeToString(hash.Sum(nil))
	return
}

func (s *service) AddNestedIdentity(identity *models.IdentityDataOutput) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	log.Info(fmt.Sprintf("AddNestedIdentity: identity:%+v", s.ToLocalIdentity(identity)))
	// s.SetOrigin()
	uid = &models.UniqueIdentityNestedDataOutput{}
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddNestedIdentity(exit): identity:%+v uid:%+v err:%v",
				s.ToLocalIdentity(identity),
				s.ToLocalNestedUniqueIdentity(uid),
				err,
			),
		)
	}()
	err = s.ValidateIdentity(identity, false)
	if err != nil {
		uid = nil
		return
	}
	idHash := ""
	idHash, err = s.IdentityIDHash(identity)
	if err != nil {
		uid = nil
		return
	}
	email := ""
	if identity.Email != nil {
		email = *(identity.Email)
	}
	name := ""
	if identity.Name != nil {
		name = *(identity.Name)
	}
	username := ""
	if identity.Username != nil {
		username = *(identity.Username)
	}
	identity.ID = idHash
	var identities []*models.IdentityDataOutput
	identities, err = s.FindIdentities(
		[]string{"source", "email", "name", "username"},
		[]interface{}{identity.Source, identity.Email, identity.Name, identity.Username},
		[]bool{false, false, false, false},
		false,
		nil,
	)
	if err != nil {
		uid = nil
		return
	}
	if len(identities) > 0 {
		err = fmt.Errorf("Identity (source, email, name, username) = (%s, %s, %s, %s) already exists", identity.Source, email, name, username)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "AddNestedIdentity")
		uid = nil
		return
	}
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	profile := &models.ProfileDataOutput{}
	if identity.UUID != nil {
		_, err = s.GetUniqueIdentity(*(identity.UUID), true, nil)
		if err != nil {
			uid = nil
			return
		}
		profile, err = s.GetProfile(*(identity.UUID), false, nil)
		if err != nil {
			uid = nil
			return
		}
	} else {
		_, err = s.AddUniqueIdentity(
			&models.UniqueIdentityDataOutput{
				UUID: identity.ID,
			},
			false,
			tx,
		)
		if err != nil {
			return
		}
		profile, err = s.AddProfile(
			&models.ProfileDataOutput{
				UUID:  identity.ID,
				Name:  identity.Name,
				Email: identity.Email,
			},
			false,
			tx,
		)
		if err != nil {
			return
		}
		identity.UUID = &(identity.ID)
	}
	_, err = s.AddIdentity(identity, false, false, tx)
	if err != nil {
		return
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	uid.UUID = *identity.UUID
	uid.LastModified = s.Now()
	uid.Profile = profile
	uid.Identities = append(uid.Identities, identity)
	tx = nil
	return
}

func (s *service) AddIdentity(inIdentityData *models.IdentityDataOutput, ignore, refresh bool, tx *sql.Tx) (identityData *models.IdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("AddIdentity: inIdentityData:%+v ignore:%v refresh:%v tx:%v", s.ToLocalIdentity(inIdentityData), ignore, refresh, tx != nil))
	identityData = inIdentityData
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddIdentity(exit): inIdentityData:%+v ignore:%v refresh:%v tx:%v identityData:%+v err:%v",
				s.ToLocalIdentity(inIdentityData),
				ignore,
				refresh,
				tx != nil,
				s.ToLocalIdentity(identityData),
				err,
			),
		)
	}()
	s.SanitizeIdentity(identityData)
	if identityData.LastModified == nil {
		identityData.LastModified = s.Now()
	}
	err = s.ValidateIdentity(identityData, false)
	if err != nil {
		identityData = nil
		return
	}
	root := "insert"
	if ignore {
		root += " ignore"
	}
	insert := root + " into identities(id, uuid, source, name, email, username, last_modified) select ?, ?, ?, ?, ?, ?, str_to_date(?, ?)"
	var res sql.Result
	// s.SetOrigin()
	res, err = s.Exec(
		s.db,
		tx,
		insert,
		identityData.ID,
		identityData.UUID,
		identityData.Source,
		identityData.Name,
		identityData.Email,
		identityData.Username,
		identityData.LastModified,
		DateTimeFormat,
	)
	if err != nil {
		identityData = nil
		return
	}
	affected := int64(0)
	affected, err = res.RowsAffected()
	if err != nil {
		identityData = nil
		return
	}
	if affected > 1 {
		err = fmt.Errorf("identity '%+v' insert affected %d rows", s.ToLocalIdentity(identityData), affected)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "AddIdentity")
		identityData = nil
		return
	} else if affected == 1 {
		affected2 := int64(0)
		// Mark identity's matching unique identity as modified
		if identityData.UUID != nil {
			affected2, err = s.TouchUniqueIdentity(*(identityData.UUID), tx)
			if err != nil {
				identityData = nil
				return
			}
			if affected2 != 1 {
				err = fmt.Errorf("identity '%+v' unique identity update affected %d rows", s.ToLocalIdentity(identityData), affected2)
				err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "AddIdentity")
				identityData = nil
				return
			}
		}
	} else {
		if !ignore {
			err = fmt.Errorf("adding identity '%+v' didn't affected any rows", s.ToLocalIdentity(identityData))
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "AddIdentity")
			identityData = nil
			return
		}
	}
	if refresh {
		identityData, err = s.GetIdentity(identityData.ID, true, tx)
		if err != nil {
			identityData = nil
			return
		}
	}
	return
}

func (s *service) FindIdentities(columns []string, values []interface{}, isDate []bool, missingFatal bool, tx *sql.Tx) (identities []*models.IdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("FindIdentities: columns:%+v values:%+v isDate:%+v missingFatal:%v tx:%v", columns, values, isDate, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"FindIdentities(exit): columns:%+v values:%+v isDate:%+v missingFatal:%v tx:%v identities:%+v err:%v",
				columns,
				values,
				isDate,
				missingFatal,
				tx != nil,
				s.ToLocalIdentities(identities),
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	sel := "select id, name, email, username, source, uuid, last_modified from identities"
	vals := []interface{}{}
	nColumns := len(columns)
	lastIndex := nColumns - 1
	if nColumns > 0 {
		sel += " where"
	}
	for index := range columns {
		column := columns[index]
		value := values[index]
		date := isDate[index]
		isNil := false
		v, ok := value.(*string)
		if ok {
			isNil = v == nil
		}
		if isNil {
			sel += " " + column + " is null"
		} else {
			if date {
				sel += " " + column + " = str_to_date(?, ?)"
				vals = append(vals, value)
				vals = append(vals, DateTimeFormat)
			} else {
				sel += " " + column + " = ?"
				vals = append(vals, value)
			}
		}
		if index < lastIndex {
			sel += " and"
		}
	}
	sel += " order by id"
	rows, err := s.Query(sdb, tx, sel, vals...)
	if err != nil {
		return
	}
	for rows.Next() {
		identity := &models.IdentityDataOutput{}
		err = rows.Scan(
			&identity.ID,
			&identity.Name,
			&identity.Email,
			&identity.Username,
			&identity.Source,
			&identity.UUID,
			&identity.LastModified,
		)
		if err != nil {
			return
		}
		identities = append(identities, identity)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && len(identities) == 0 {
		err = fmt.Errorf("cannot find identities for %+v/%+v", columns, values)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "FindIdentities")
		return
	}
	return
}

func (s *service) AddNestedUniqueIdentity(uuid string) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	log.Info(fmt.Sprintf("AddNestedUniqueIdentity: uuid:%s", uuid))
	// s.SetOrigin()
	uid = &models.UniqueIdentityNestedDataOutput{}
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddNestedUniqueIdentity(exit): uuid:%s uid:%+v err:%v",
				uuid,
				s.ToLocalNestedUniqueIdentity(uid),
				err,
			),
		)
	}()
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	_, err = s.AddUniqueIdentity(
		&models.UniqueIdentityDataOutput{
			UUID: uuid,
		},
		false,
		tx,
	)
	if err != nil {
		return
	}
	profile := &models.ProfileDataOutput{}
	profile, err = s.AddProfile(
		&models.ProfileDataOutput{
			UUID: uuid,
		},
		false,
		tx,
	)
	if err != nil {
		return
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	tx = nil
	uid.UUID = uuid
	uid.LastModified = s.Now()
	uid.Profile = profile
	return
}

func (s *service) AddProfile(inProfileData *models.ProfileDataOutput, refresh bool, tx *sql.Tx) (profileData *models.ProfileDataOutput, err error) {
	log.Info(fmt.Sprintf("AddProfile: inProfileData:%+v refresh:%v tx:%v", s.ToLocalProfile(inProfileData), refresh, tx != nil))
	profileData = inProfileData
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddProfile(exit): inProfileData:%+v refresh:%v tx:%v profileData:%+v err:%v",
				s.ToLocalProfile(inProfileData),
				refresh,
				tx != nil,
				s.ToLocalProfile(profileData),
				err,
			),
		)
	}()
	s.SanitizeProfile(profileData)
	err = s.ValidateProfile(profileData, tx)
	if err != nil {
		profileData = nil
		return
	}
	insert := "insert into profiles(uuid, name, email, gender, gender_acc, is_bot, country_code) select ?, ?, ?, ?, ?, ?, ?"
	var res sql.Result
	// s.SetOrigin()
	res, err = s.Exec(
		s.db,
		tx,
		insert,
		profileData.UUID,
		profileData.Name,
		profileData.Email,
		profileData.Gender,
		profileData.GenderAcc,
		profileData.IsBot,
		profileData.CountryCode,
	)
	if err != nil {
		profileData = nil
		return
	}
	affected := int64(0)
	affected, err = res.RowsAffected()
	if err != nil {
		profileData = nil
		return
	}
	if affected > 1 {
		err = fmt.Errorf("profile '%+v' insert affected %d rows", s.ToLocalProfile(profileData), affected)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "AddProfile")
		profileData = nil
		return
	} else if affected == 1 {
		affected2 := int64(0)
		// Mark profile's matching unique identity as modified
		affected2, err = s.TouchUniqueIdentity(profileData.UUID, tx)
		if err != nil {
			profileData = nil
			return
		}
		if affected2 != 1 {
			err = fmt.Errorf("profile '%+v' unique identity update affected %d rows", s.ToLocalProfile(profileData), affected2)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "AddProfile")
			profileData = nil
			return
		}
	} else {
		err = fmt.Errorf("adding profile '%+v' didn't affected any rows", s.ToLocalProfile(profileData))
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "AddProfile")
		profileData = nil
		return
	}
	if refresh {
		profileData, err = s.GetProfile(profileData.UUID, true, tx)
		if err != nil {
			profileData = nil
			return
		}
	}
	return
}

func (s *service) AddEnrollment(inEnrollmentData *models.EnrollmentDataOutput, ignore, refresh bool, tx *sql.Tx) (enrollmentData *models.EnrollmentDataOutput, err error) {
	log.Info(fmt.Sprintf("AddEnrollment: inEnrollmentData:%+v ignore:%v refresh:%v tx:%v", inEnrollmentData, ignore, refresh, tx != nil))
	enrollmentData = inEnrollmentData
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddEnrollment(exit): inEnrollmentData:%+v ignore:%v refresh:%v tx:%v enrollmentData:%+v err:%v",
				inEnrollmentData,
				ignore,
				refresh,
				tx != nil,
				enrollmentData,
				err,
			),
		)
	}()
	if enrollmentData.Role == "" {
		enrollmentData.Role = shared.DefaultRole
	}
	err = s.ValidateEnrollment(enrollmentData, false)
	if err != nil {
		enrollmentData = nil
		return
	}
	root := "insert"
	if ignore {
		root += " ignore"
	}
	insert := root + " into enrollments(uuid, organization_id, role, start, end, project_slug) select ?, ?, ?, str_to_date(?, ?), str_to_date(?, ?), ?"
	var res sql.Result
	// s.SetOrigin()
	res, err = s.Exec(
		s.db,
		tx,
		insert,
		enrollmentData.UUID,
		enrollmentData.OrganizationID,
		enrollmentData.Role,
		enrollmentData.Start,
		DateTimeFormat,
		enrollmentData.End,
		DateTimeFormat,
		enrollmentData.ProjectSlug,
	)
	if err != nil {
		enrollmentData = nil
		return
	}
	affected := int64(0)
	affected, err = res.RowsAffected()
	if err != nil {
		enrollmentData = nil
		return
	}
	if affected > 1 {
		err = fmt.Errorf("enrollment '%+v' insert affected %d rows", enrollmentData, affected)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "AddEnrollment")
		enrollmentData = nil
		return
	} else if affected == 1 {
		affected2 := int64(0)
		// Mark enrollment's matching unique identity as modified
		affected2, err = s.TouchUniqueIdentity(enrollmentData.UUID, tx)
		if err != nil {
			enrollmentData = nil
			return
		}
		if affected2 != 1 {
			err = fmt.Errorf("enrollment '%+v' unique identity update affected %d rows", enrollmentData, affected2)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "AddEnrollment")
			enrollmentData = nil
			return
		}
	} else {
		if !ignore {
			err = fmt.Errorf("adding enrollment '%+v' didn't affected any rows", enrollmentData)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "AddEnrollment")
			enrollmentData = nil
			return
		}
	}
	if refresh {
		id := int64(0)
		id, err = res.LastInsertId()
		if err != nil {
			enrollmentData = nil
			return
		}
		enrollmentData.ID = id
		enrollmentData, err = s.GetEnrollment(enrollmentData.ID, true, tx)
		if err != nil {
			enrollmentData = nil
			return
		}
	}
	return
}

func (s *service) EditOrganization(inOrganizationData *models.OrganizationDataOutput, refresh bool, tx *sql.Tx) (organizationData *models.OrganizationDataOutput, err error) {
	log.Info(fmt.Sprintf("EditOrganization: inOrganizationData:%+v refresh:%v tx:%v", inOrganizationData, refresh, tx != nil))
	organizationData = inOrganizationData
	defer func() {
		log.Info(
			fmt.Sprintf(
				"EditOrganization(exit): inOrganizationData:%+v refresh:%v tx:%v organizationData:%+v err:%v",
				inOrganizationData,
				refresh,
				tx != nil,
				organizationData,
				err,
			),
		)
	}()
	organizationData.Name = strings.TrimSpace(organizationData.Name)
	err = s.ValidateOrganization(organizationData, true)
	if err != nil {
		err = fmt.Errorf("organization '%+v' didn't pass update validation", organizationData)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "EditOrganization")
		organizationData = nil
		return
	}
	update := "update organizations set name = ? where id = ?"
	var res sql.Result
	// s.SetOrigin()
	res, err = s.Exec(
		s.db,
		tx,
		update,
		organizationData.Name,
		organizationData.ID,
	)
	if err != nil {
		organizationData = nil
		return
	}
	affected := int64(0)
	affected, err = res.RowsAffected()
	if err != nil {
		organizationData = nil
		return
	}
	if affected == 0 {
		log.Info(fmt.Sprintf("EditOrganization: organization '%+v' update didn't affected any rows", organizationData))
	}
	if refresh {
		organizationData, err = s.GetOrganization(organizationData.ID, true, tx)
		if err != nil {
			organizationData = nil
			return
		}
	}
	return
}

func (s *service) EditEnrollment(inEnrollmentData *models.EnrollmentDataOutput, refresh bool, tx *sql.Tx) (enrollmentData *models.EnrollmentDataOutput, err error) {
	log.Info(fmt.Sprintf("EditEnrollment: inEnrollmentData:%+v refresh:%v tx:%v", inEnrollmentData, refresh, tx != nil))
	enrollmentData = inEnrollmentData
	defer func() {
		log.Info(
			fmt.Sprintf(
				"EditEnrollment(exit): inEnrollmentData:%+v refresh:%v tx:%v enrollmentData:%+v err:%v",
				inEnrollmentData,
				refresh,
				tx != nil,
				enrollmentData,
				err,
			),
		)
	}()
	if enrollmentData.Role == "" {
		enrollmentData.Role = shared.DefaultRole
	}
	err = s.ValidateEnrollment(enrollmentData, true)
	if err != nil {
		err = fmt.Errorf("enrollment '%+v' didn't pass update validation", enrollmentData)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "EditEnrollment")
		enrollmentData = nil
		return
	}
	update := "update enrollments set uuid = ?, organization_id = ?, start = str_to_date(?, ?), end = str_to_date(?, ?), project_slug = ?, role = ? where id = ?"
	var res sql.Result
	// s.SetOrigin()
	res, err = s.Exec(
		s.db,
		tx,
		update,
		enrollmentData.UUID,
		enrollmentData.OrganizationID,
		enrollmentData.Start,
		DateTimeFormat,
		enrollmentData.End,
		DateTimeFormat,
		enrollmentData.ProjectSlug,
		enrollmentData.Role,
		enrollmentData.ID,
	)
	if err != nil {
		enrollmentData = nil
		return
	}
	affected := int64(0)
	affected, err = res.RowsAffected()
	if err != nil {
		enrollmentData = nil
		return
	}
	if affected > 1 {
		err = fmt.Errorf("enrollment '%+v' update affected %d rows", enrollmentData, affected)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "EditEnrollment")
		enrollmentData = nil
		return
	} else if affected == 1 {
		affected2 := int64(0)
		// Mark enrollment's matching unique identity as modified
		affected2, err = s.TouchUniqueIdentity(enrollmentData.UUID, tx)
		if err != nil {
			enrollmentData = nil
			return
		}
		if affected2 != 1 {
			err = fmt.Errorf("enrollment '%+v' unique identity update affected %d rows", enrollmentData, affected2)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "EditEnrollment")
			enrollmentData = nil
			return
		}
	} else {
		log.Info(fmt.Sprintf("EditEnrollment: enrollment '%+v' update didn't affected any rows", enrollmentData))
	}
	if refresh {
		enrollmentData, err = s.GetEnrollment(enrollmentData.ID, true, tx)
		if err != nil {
			enrollmentData = nil
			return
		}
	}
	return
}

func (s *service) EditIdentity(inIdentityData *models.IdentityDataOutput, refresh bool, tx *sql.Tx) (identityData *models.IdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("EditIdentity: inIdentityData:%+v refresh:%v tx:%v", s.ToLocalIdentity(inIdentityData), refresh, tx != nil))
	identityData = inIdentityData
	defer func() {
		log.Info(
			fmt.Sprintf(
				"EditIdentity(exit): inIdentityData:%+v refresh:%v tx:%v identityData:%+v err:%v",
				s.ToLocalIdentity(inIdentityData),
				refresh,
				tx != nil,
				s.ToLocalIdentity(identityData),
				err,
			),
		)
	}()
	s.SanitizeIdentity(identityData)
	if identityData.ID == "" || identityData.Source == "" {
		err = fmt.Errorf("identity '%+v' missing id or source", s.ToLocalIdentity(identityData))
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "EditIdentity")
		identityData = nil
		return
	}
	if identityData.LastModified == nil {
		identityData.LastModified = s.Now()
	}
	columns := []string{"id", "uuid", "source"}
	values := []interface{}{identityData.ID, identityData.UUID, identityData.Source}
	if identityData.Name != nil && *identityData.Name != "" {
		columns = append(columns, "name")
		values = append(values, *identityData.Name)
	}
	if identityData.Username != nil && *identityData.Username != "" {
		columns = append(columns, "username")
		values = append(values, *identityData.Username)
	}
	if identityData.Email != nil && *identityData.Email != "" {
		columns = append(columns, "email")
		values = append(values, *identityData.Email)
	}
	update := "update identities set "
	for _, column := range columns {
		update += fmt.Sprintf("%s = ?, ", column)
	}
	update += " last_modified = str_to_date(?, ?) where id = ?"
	values = append(values, identityData.LastModified)
	values = append(values, DateTimeFormat)
	values = append(values, identityData.ID)
	var res sql.Result
	// s.SetOrigin()
	res, err = s.Exec(s.db, tx, update, values...)
	if err != nil {
		identityData = nil
		return
	}
	affected := int64(0)
	affected, err = res.RowsAffected()
	if err != nil {
		identityData = nil
		return
	}
	if affected > 1 {
		err = fmt.Errorf("identity '%+v' update affected %d rows", s.ToLocalIdentity(identityData), affected)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "EditIdentity")
		identityData = nil
		return
	} else if affected == 1 {
		affected2 := int64(0)
		// Mark identity's matching unique identity as modified
		if identityData.UUID != nil {
			affected2, err = s.TouchUniqueIdentity(*(identityData.UUID), tx)
			if err != nil {
				identityData = nil
				return
			}
			if affected2 != 1 {
				err = fmt.Errorf("identity '%+v' unique identity update affected %d rows", s.ToLocalIdentity(identityData), affected2)
				err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "EditIdentity")
				identityData = nil
				return
			}
		}
	} else {
		log.Info(fmt.Sprintf("EditIdentity: identity '%+v' update didn't affected any rows", s.ToLocalIdentity(identityData)))
	}
	if refresh {
		identityData, err = s.GetIdentity(identityData.ID, true, tx)
		if err != nil {
			identityData = nil
			return
		}
	}
	return
}

func (s *service) EditProfile(inProfileData *models.ProfileDataOutput, refresh bool, tx *sql.Tx) (profileData *models.ProfileDataOutput, err error) {
	log.Info(fmt.Sprintf("EditProfile: inProfileData:%+v refresh:%v tx:%v", s.ToLocalProfile(inProfileData), refresh, tx != nil))
	profileData = inProfileData
	defer func() {
		log.Info(
			fmt.Sprintf(
				"EditProfile(exit): inProfileData:%+v refresh:%v tx:%v profileData:%+v err:%v",
				s.ToLocalProfile(inProfileData),
				refresh,
				tx != nil,
				s.ToLocalProfile(profileData),
				err,
			),
		)
	}()
	s.SanitizeProfile(profileData)
	err = s.ValidateProfile(profileData, tx)
	if err != nil {
		profileData = nil
		return
	}
	columns := []string{}
	values := []interface{}{}
	if profileData.Name != nil && *profileData.Name != "" {
		columns = append(columns, "name")
		values = append(values, *profileData.Name)
	}
	if profileData.Email != nil && *profileData.Email != "" {
		columns = append(columns, "email")
		values = append(values, *profileData.Email)
	}
	// Database doesn't have null, but we can use to to call EditProfile and skip updating is_bot
	if profileData.IsBot != nil {
		columns = append(columns, "is_bot")
		values = append(values, *profileData.IsBot)
	}
	if profileData.CountryCode != nil && *profileData.CountryCode != "" {
		columns = append(columns, "country_code")
		values = append(values, *profileData.CountryCode)
	}
	if profileData.Gender != nil {
		columns = append(columns, "gender")
		values = append(values, *profileData.Gender)
		columns = append(columns, "gender_acc")
		if profileData.GenderAcc == nil {
			values = append(values, 100)
		} else {
			values = append(values, *profileData.GenderAcc)
		}
	}
	nColumns := len(columns)
	if nColumns > 0 {
		lastIndex := nColumns - 1
		update := "update profiles set "
		for index, column := range columns {
			update += fmt.Sprintf("%s = ?", column)
			if index != lastIndex {
				update += ", "
			}
		}
		update += " where uuid = ?"
		values = append(values, profileData.UUID)
		var res sql.Result
		// s.SetOrigin()
		res, err = s.Exec(s.db, tx, update, values...)
		if err != nil {
			profileData = nil
			return
		}
		affected := int64(0)
		affected, err = res.RowsAffected()
		if err != nil {
			profileData = nil
			return
		}
		if affected > 1 {
			err = fmt.Errorf("profile '%+v' update affected %d rows", s.ToLocalProfile(profileData), affected)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "EditProfile")
			profileData = nil
			return
		} else if affected == 1 {
			affected2 := int64(0)
			// Mark profile's unique identity as modified
			affected2, err = s.TouchUniqueIdentity(profileData.UUID, tx)
			if err != nil {
				profileData = nil
				return
			}
			if affected2 != 1 {
				err = fmt.Errorf("profile '%+v' unique identity update affected %d rows", s.ToLocalProfile(profileData), affected2)
				err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "EditProfile")
				profileData = nil
				return
			}
		} else {
			log.Info(fmt.Sprintf("EditProfile: profile '%+v' update didn't affected any rows", s.ToLocalProfile(profileData)))
		}
	} else {
		log.Info(fmt.Sprintf("EditProfile: profile '%+v' nothing to update", s.ToLocalProfile(profileData)))
	}
	if refresh {
		profileData, err = s.GetProfile(profileData.UUID, true, tx)
		if err != nil {
			profileData = nil
			return
		}
	}
	return
}

func (s *service) UnarchiveUUID(uuid string, tm time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("UnarchiveUUID: uuid:%s tm:%v tx:%v", uuid, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("UnarchiveUUID(exit): uuid:%s tm:%v tx:%v err:%v", uuid, tm, tx != nil, err))
	}()
	if uuid == "" {
		err = fmt.Errorf("cannot unarchive empty uuid")
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "UnarchiveUUID")
		return
	}
	err = s.UnarchiveUniqueIdentity(uuid, true, &tm, tx)
	if err != nil {
		return
	}
	err = s.UnarchiveProfile(uuid, true, &tm, tx)
	if err != nil {
		return
	}
	identities, err := s.GetArchiveUniqueIdentityIdentities(uuid, tm, false, tx)
	if err != nil {
		return
	}
	for _, identity := range identities {
		err = s.UnarchiveIdentity(identity.ID, true, &tm, tx)
		if err != nil {
			return
		}
	}
	enrollments, err := s.GetArchiveUniqueIdentityEnrollments(uuid, tm, false, tx)
	if err != nil {
		return
	}
	for _, enrollment := range enrollments {
		err = s.UnarchiveEnrollment(enrollment.ID, true, &tm, tx)
		if err != nil {
			if strings.Contains(err.Error(), "Error 1452: Cannot add or update a child row") {
				log.Warn(fmt.Sprintf("UnarchiveEnrollment: id:%d tm:%v err:%v", enrollment.ID, tm, err))
			} else {
				return
			}
		}
	}
	// Check if profile's name or email is empty
	// if there is only one email across identities, update profile's email to this email
	// if there is only one name across identities, update profile's name to this name
	err = s.SetProfileEmptyDataFromIdentities(uuid, identities, tx)
	if err != nil {
		log.Warn(fmt.Sprintf("UnarchiveUUID: SetProfileEmptyDataFromIdentities: uuid:%s err:%v", uuid, err))
		return
	}
	return
}

func (s *service) ArchiveUUID(uuid string, itm *time.Time, tx *sql.Tx) (tm *time.Time, err error) {
	log.Info(fmt.Sprintf("ArchiveUUID: uuid:%s itm:%v tx:%v", uuid, itm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("ArchiveUUID(exit): uuid:%s itm:%v tx:%v tm:%v err:%v", uuid, itm, tx != nil, tm, err))
	}()
	if uuid == "" {
		err = fmt.Errorf("cannot archive empty uuid")
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ArchiveUUID")
		return
	}
	tm = itm
	if tm == nil {
		t := time.Now()
		tm = &t
	}
	err = s.ArchiveUniqueIdentity(uuid, tm, tx)
	if err != nil {
		return
	}
	identities, err := s.GetUniqueIdentityIdentities(uuid, false, tx)
	if err != nil {
		return
	}
	// Check if profile's name or email is empty
	// if there is only one email across identities, update profile's email to this email
	// if there is only one name across identities, update profile's name to this name
	err = s.SetProfileEmptyDataFromIdentities(uuid, identities, tx)
	if err != nil {
		log.Warn(fmt.Sprintf("UnarchiveUUID: SetProfileEmptyDataFromIdentities: uuid:%s err:%v", uuid, err))
		return
	}
	err = s.ArchiveProfile(uuid, tm, tx)
	if err != nil {
		return
	}
	for _, identity := range identities {
		err = s.ArchiveIdentity(identity.ID, tm, tx)
		if err != nil {
			return
		}
	}
	enrollments, err := s.GetUniqueIdentityEnrollments(uuid, false, tx)
	if err != nil {
		return
	}
	for _, enrollment := range enrollments {
		err = s.ArchiveEnrollment(enrollment.ID, tm, tx)
		if err != nil {
			return
		}
	}
	return
}

func (s *service) SetProfileEmptyDataFromIdentities(uuid string, identities []*models.IdentityDataOutput, tx *sql.Tx) (err error) {
	nIdents := len(identities)
	name, email := "", ""
	log.Info(fmt.Sprintf("SetProfileEmptyDataFromIdentities: uuid:%s identities:%d tx:%v", uuid, nIdents, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("SetProfileEmptyDataFromIdentities(exit): uuid:%s identities:%d tx:%v name:%s email:%s err:%v", uuid, nIdents, tx != nil, name, email, err))
	}()
	if nIdents == 0 {
		return
	}
	nameSet := make(map[string]struct{})
	emailSet := make(map[string]struct{})
	for _, identity := range identities {
		if identity.Name != nil {
			name = *identity.Name
			nameSet[name] = struct{}{}
		}
		if identity.Email != nil {
			email = *identity.Email
			emailSet[email] = struct{}{}
		}
	}
	// fmt.Printf("nameSet: %+v\nemailSet: %+v\n", nameSet, emailSet)
	if len(nameSet) > 1 {
		name = ""
	}
	if len(emailSet) > 1 {
		email = ""
	}
	if name == "" && email == "" {
		return
	}
	var (
		pName  *string
		pEmail *string
		rows   *sql.Rows
	)
	// This uses RW connection, because this value will be updated soon
	rows, err = s.Query(s.db, tx, "select name, email from profiles where uuid = ?", uuid)
	if err != nil {
		return
	}
	for rows.Next() {
		err = rows.Scan(&pName, &pEmail)
		if err != nil {
			return
		}
		break
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if pName != nil && *pName != "" {
		// fmt.Printf("profile already has a name %s\n", *pName)
		name = ""
	}
	if pEmail != nil && *pEmail != "" {
		// fmt.Printf("profile already has an email %s\n", *pEmail)
		email = ""
	}
	if name == "" && email == "" {
		return
	}
	args := []interface{}{}
	q := "update profiles set "
	if name != "" {
		q += "name = ?"
		args = append(args, name)
		if email != "" {
			q += ", email = ?"
			args = append(args, email)
		}
	} else {
		q += "email = ?"
		args = append(args, email)
	}
	q += " where uuid = ?"
	args = append(args, uuid)
	// fmt.Printf("%s %+v\n", q, args)
	_, err = s.Exec(s.db, tx, q, args...)
	if err != nil {
		return
	}
	return
}

func (s *service) DedupEnrollments() (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	var rows *sql.Rows
	// This uses RW connection, even for selects - because it will eventually update data
	rows, err = s.Query(
		s.db,
		tx,
		"select uuid, organization_id, start, end, project_slug, count(id) as cnt "+
			"from enrollments where project_slug is null "+
			"group by uuid, organization_id, start, end, project_slug "+
			"having cnt > 1",
	)
	if err != nil {
		return
	}
	var (
		uuid           string
		orgID          int64
		start          strfmt.DateTime
		end            strfmt.DateTime
		projectSlug    *string
		cnt            int
		uuidAry        []string
		orgIDAry       []int64
		startAry       []strfmt.DateTime
		endAry         []strfmt.DateTime
		projectSlugAry []*string
		cntAry         []int
	)
	for rows.Next() {
		err = rows.Scan(&uuid, &orgID, &start, &end, &projectSlug, &cnt)
		if err != nil {
			return
		}
		uuidAry = append(uuidAry, uuid)
		orgIDAry = append(orgIDAry, orgID)
		startAry = append(startAry, start)
		endAry = append(endAry, end)
		projectSlugAry = append(projectSlugAry, projectSlug)
		cntAry = append(cntAry, cnt)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	rids := make(map[int]struct{})
	for idx := range uuidAry {
		uuid = uuidAry[idx]
		orgID = orgIDAry[idx]
		start = startAry[idx]
		end = endAry[idx]
		projectSlug = projectSlugAry[idx]
		cnt = cntAry[idx]
		var rows *sql.Rows
		rows, err = s.Query(
			s.db,
			tx,
			"select id from enrollments where uuid = ? and organization_id = ? "+
				"and start = ? and end = ? and project_slug is null order by id desc limit ?",
			uuid,
			orgID,
			time.Time(start),
			time.Time(end),
			cnt-1,
		)
		if err != nil {
			return
		}
		rid := 0
		for rows.Next() {
			err = rows.Scan(&rid)
			if err != nil {
				return
			}
			rids[rid] = struct{}{}
		}
		err = rows.Err()
		if err != nil {
			return
		}
		err = rows.Close()
		if err != nil {
			return
		}
	}
	ridsAry := []interface{}{}
	for rid := range rids {
		ridsAry = append(ridsAry, rid)
	}
	packSize := 1000
	nRids := len(ridsAry)
	nPacks := nRids / packSize
	if nRids%packSize != 0 {
		nPacks++
	}
	for i := 0; i < nPacks; i++ {
		from := packSize * i
		to := from + packSize
		if to > nRids {
			to = nRids
		}
		pack := ridsAry[from:to]
		query := "delete from enrollments where id in ("
		for range pack {
			query += "?,"
		}
		query = query[:len(query)-1] + ")"
		_, err = s.Exec(s.db, tx, query, pack...)
		if err != nil {
			for _, rid := range pack {
				_, err = s.Exec(s.db, tx, "delete from enrollments where id = ?", rid)
				if err != nil {
					log.Info(fmt.Sprintf("failed to delete enrollment id=%d: %+v", rid, err))
					return
				}
			}
			return
		}
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	// set tx to nil to mark success so deferred rollback will not happen
	tx = nil
	return
}

func (s *service) MapOrgNames() (status string, err error) {
	log.Info("MapOrgNames")
	// s.SetOrigin()
	status = ""
	defer func() {
		log.Info(fmt.Sprintf("MapOrgNames(exit): status:%s err:%v", status, err))
	}()
	e := s.DedupEnrollments()
	if e != nil {
		log.Warn(fmt.Sprintf("dedupEnrollments: %v", e))
	}
	if !s.mappingsLoaded {
		// orgNamesMappings allMappings
		var data []byte
		data, err = ioutil.ReadFile(MapOrgNamesFile)
		if err != nil {
			return
		}
		err = yaml.Unmarshal(data, &s.orgNamesMappings)
		if err != nil {
			return
		}
		s.mappingsLoaded = true
	}
	// Entire map org names API uses RW connection only
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	nids := []int64{}
	inf := ""
	added := 0
	updated := 0
	deleted := 0
	skipped := 0
	conflicts := 0
	archivedConflicts := 0
	rolsUpdated := int64(0)
	for _, mapping := range s.orgNamesMappings.Mappings {
		re := mapping[0]
		to := mapping[1]
		// fmt.Printf("Processing '%s' -> '%s'\n", re, to)
		var rows *sql.Rows
		rows, err = s.Query(s.db, tx, "select id, name from organizations where name = ?", to)
		if err != nil {
			return
		}
		fetched := false
		id := int64(0)
		actualName := ""
		for rows.Next() {
			if fetched {
				err = fmt.Errorf("multiple companies found for name: %s, including %s", to, actualName)
				return
			}
			err = rows.Scan(&id, &actualName)
			if err != nil {
				return
			}
			fetched = true
		}
		err = rows.Err()
		if err != nil {
			return
		}
		err = rows.Close()
		if err != nil {
			return
		}
		if !fetched {
			var res sql.Result
			res, err = s.Exec(s.db, tx, "insert into organizations(name) values(?)", to)
			if err != nil {
				return
			}
			id, err = res.LastInsertId()
			if err != nil {
				return
			}
			inf = fmt.Sprintf("Added organization '%s' (id=%d)", to, id)
			status += inf + ", "
			log.Info(inf)
			added++
		} else if actualName != to {
			_, err = s.Exec(s.db, tx, "update organizations set name = ? where id = ?", to, id)
			if err != nil {
				return
			}
			inf = fmt.Sprintf("Updated organization name '%s' -> '%s' (id=%d)", actualName, to, id)
			status += inf + ", "
			log.Info(inf)
			updated++
		}
		// Because sql.Query escapes \ --> \\ and mysql special characters regexp is '\\.'
		re = strings.Replace(re, "\\\\", "\\", -1)
		//fmt.Printf("RE: %s\n", re)
		rows, err = s.Query(s.db, tx, "select id, name from organizations where name regexp ? and name != ?", re, to)
		//rows, err = s.Query(s.db, tx, "select id, name from organizations where name = ?", re)
		//rows, err = s.Query(s.db, tx, `select id, name from organizations where name regexp '` + re + `'`)
		if err != nil {
			return
		}
		nid := int64(0)
		name := ""
		nidAry := []int64{}
		nameAry := []string{}
		for rows.Next() {
			err = rows.Scan(&nid, &name)
			if err != nil {
				return
			}
			nidAry = append(nidAry, nid)
			nameAry = append(nameAry, name)
		}
		err = rows.Err()
		if err != nil {
			return
		}
		err = rows.Close()
		if err != nil {
			return
		}
		for idx, nid := range nidAry {
			name = nameAry[idx]
			//fmt.Printf("'%s'(%d)\n", name, nid)
			if nid == id {
				inf = fmt.Sprintf("'%s' (id=%d) matching '%s' already maps into '%s' (id=%d), skipping", name, nid, re, to, id)
				status += inf + ", "
				log.Info(inf)
				skipped++
				continue
			}
			var res sql.Result
			// Update current enrollments
			affected := int64(0)
			res, err = s.Exec(s.db, tx, "update enrollments set organization_id = ? where organization_id = ?", id, nid)
			if err != nil {
				if !strings.Contains(err.Error(), "Error 1062: Duplicate entry") {
					log.Warn(fmt.Sprintf("Error: cannot update enrollments organization '%s' (id=%d) to '%s' (id=%d): %v", name, nid, to, id, err))
					return
				}
				var rows *sql.Rows
				rows, err = s.Query(s.db, tx, "select id from enrollments where organization_id = ?", nid)
				if err != nil {
					return
				}
				rid := 0
				for rows.Next() {
					err = rows.Scan(&rid)
					if err != nil {
						return
					}
					res, err = s.Exec(s.db, tx, "update enrollments set organization_id = ? where id = ? and organization_id = ?", id, rid, nid)
					if err != nil && !strings.Contains(err.Error(), "Error 1062: Duplicate entry") {
						log.Warn(fmt.Sprintf("Error: cannot update enrollment (id=%d) organization '%s' (id=%d) to '%s' (id=%d): %v", rid, name, nid, to, id, err))
						return
					}
					if err != nil {
						conflicts++
					}
				}
				err = rows.Err()
				if err != nil {
					return
				}
				err = rows.Close()
				if err != nil {
					return
				}
				affected++
			} else {
				affected, err = res.RowsAffected()
			}
			if affected > 0 {
				rolsUpdated += affected
				inf = fmt.Sprintf("Updated organization '%s' -> '%s' on %d enrollments", name, to, affected)
				status += inf + ", "
				log.Info(inf)
			}
			// Update archived enrollments
			affected = int64(0)
			res, err = s.Exec(s.db, tx, "update enrollments_archive set organization_id = ? where organization_id = ?", id, nid)
			if err != nil {
				if !strings.Contains(err.Error(), "Error 1062: Duplicate entry") {
					log.Warn(fmt.Sprintf("Error: cannot update archived enrollments organization '%s' (id=%d) to '%s' (id=%d): %v", name, nid, to, id, err))
					return
				}
				var rows *sql.Rows
				rows, err = s.Query(s.db, tx, "select id, archived_at from enrollments_archive where organization_id = ?", nid)
				if err != nil {
					return
				}
				rid := 0
				var archivedAt time.Time
				for rows.Next() {
					err = rows.Scan(&rid, &archivedAt)
					if err != nil {
						return
					}
					res, err = s.Exec(s.db, tx, "update enrollments_archive set organization_id = ? where id = ? and archived_at = ? and organization_id = ?", id, rid, archivedAt, nid)
					if err != nil && !strings.Contains(err.Error(), "Error 1062: Duplicate entry") {
						log.Warn(fmt.Sprintf("Error: cannot update archived enrollment (id=%d, archived_at=%+v) organization '%s' (id=%d) to '%s' (id=%d): %v", rid, archivedAt, name, nid, to, id, err))
						return
					}
					if err != nil {
						archivedConflicts++
					}
				}
				err = rows.Err()
				if err != nil {
					return
				}
				err = rows.Close()
				if err != nil {
					return
				}
				affected++
			} else {
				affected, err = res.RowsAffected()
			}
			if affected > 0 {
				rolsUpdated += affected
				inf = fmt.Sprintf("Updated organization '%s' -> '%s' on %d archived enrollments", name, to, affected)
				status += inf + ", "
				log.Info(inf)
			}
			res, err = s.Exec(s.db, tx, "delete from organizations where id = ?", nid)
			if err != nil {
				log.Warn(fmt.Sprintf("Error: cannot delete organization '%s' (id=%d)", name, nid))
				nids = append(nids, nid)
			} else {
				affected, err = res.RowsAffected()
				if affected > 0 {
					inf = fmt.Sprintf("Deleted organization '%s' (id=%d)", name, nid)
					status += inf + ", "
					log.Info(inf)
					deleted++
				}
			}
		}
	}
	if status == "" {
		status = "Nothing to update"
	} else {
		status += fmt.Sprintf(
			"Organizations: added:%d renamed:%d deleted:%d skipped:%d, Enrollments: conflicts:%d archive conflicts:%d updated:%d",
			added,
			updated,
			deleted,
			skipped,
			conflicts,
			archivedConflicts,
			rolsUpdated,
		)
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	// Set tx to nil, so deferred rollback will not happen
	tx = nil
	if len(nids) > 0 {
		log.Info(fmt.Sprintf("Deleting %d pending orgs", len(nids)))
		for _, nid := range nids {
			var res sql.Result
			res, err = s.Exec(s.db, nil, "delete from organizations where id = ?", nid)
			if err != nil {
				log.Warn(fmt.Sprintf("Error: cannot delete organization id=%d", nid))
				continue
			}
			var affected int64
			affected, err = res.RowsAffected()
			if affected > 0 {
				inf = fmt.Sprintf("Deleted organization id=%d", nid)
				status += inf + ", "
				log.Info(inf)
				deleted++
			}
		}
	}
	return
}

func (s *service) HideEmails() (status string, err error) {
	log.Info("HideEmails")
	// s.SetOrigin()
	status = ""
	defer func() {
		log.Info(fmt.Sprintf("HideEmails(exit): status:%s err:%v", status, err))
	}()
	updates := [][2]string{
		{"profiles", "name"},
		{"identities", "name"},
		{"identities", "username"},
	}
	thrN := runtime.NumCPU()
	var mtx *sync.Mutex
	if thrN > 1 {
		mtx = &sync.Mutex{}
	}
	updateFunc := func(ch chan error, update [2]string) (err error) {
		defer func() {
			if ch != nil {
				ch <- err
			}
		}()
		table := update[0]
		column := update[1]
		re := "^[^@]+@[^@]+$"
		updateSQL := fmt.Sprintf(
			"update %[1]s set %[2]s = substring_index(%[2]s, '@', 1) where %[2]s regexp '%[3]s'",
			table,
			column,
			re,
		)
		var res sql.Result
		conflict := false
		res, err = s.Exec(s.db, nil, updateSQL)
		if err != nil {
			if !strings.Contains(err.Error(), "Error 1062: Duplicate entry") {
				return err
			}
			updateSQL := fmt.Sprintf(
				"update ignore %[1]s set %[2]s = substring_index(%[2]s, '@', 1) where %[2]s regexp '%[3]s'",
				table,
				column,
				re,
			)
			res, err = s.Exec(s.db, nil, updateSQL)
			if err != nil {
				return
			}
			conflict = true
		}
		affected := int64(0)
		affected, err = res.RowsAffected()
		if err != nil {
			return
		}
		if conflict {
			updateSQL := fmt.Sprintf(
				"update %[1]s set %[2]s = concat(substring_index(%[2]s, '@', 1), '-redacted') where %[2]s regexp '%[3]s'",
				table,
				column,
				re,
			)
			res, err = s.Exec(s.db, nil, updateSQL)
			if err != nil {
				return
			}
			affected2 := int64(0)
			affected2, err = res.RowsAffected()
			if err != nil {
				return
			}
			if affected2 > 0 {
				affected += affected2
				log.Warn(fmt.Sprintf("%d conflicts on column %s table %s, added '-redacted' suffix", affected2, column, table))
			}
		}
		if mtx != nil {
			mtx.Lock()
		}
		if status == "" {
			status = fmt.Sprintf("Updated %d %s values on %s table, ", affected, column, table)
		} else {
			status += fmt.Sprintf("%d %s values on %s table, ", affected, column, table)
		}
		if mtx != nil {
			mtx.Unlock()
		}
		return
	}
	ch := make(chan error)
	nThreads := 0
	if thrN > 0 {
		for _, update := range updates {
			go updateFunc(ch, update)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				nThreads--
				if err != nil {
					return
				}
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
	} else {
		for _, update := range updates {
			err = updateFunc(nil, update)
			if err != nil {
				return
			}
		}
	}
	if len(status) > 2 {
		status = status[0 : len(status)-2]
	}
	log.Warn(status)
	return
}

func (s *service) MergeAll(debug int, dry bool) (status string, err error) {
	log.Info(fmt.Sprintf("MergeAll: debug:%d dry:%v", debug, dry))
	// s.SetOrigin()
	defer func() {
		log.Info(fmt.Sprintf("MergeAll(exit): debug:%d dry:%v status:%s err:%v", debug, dry, status, err))
	}()
	emailRE := `^[^@]+@[^@]+$`
	//reVal := `[[:^alpha:]]`
	reVal := `[]['"“”・·,;!#$%^&*()_+{}:|\/?.,><~£§ -]`
	reStr := `regexp_replace(lower(concat(trim(email), '@@@', trim(name))), ?, '')`
	tables := []string{"identities", "profiles"}
	for _, table := range tables {
		log.Warn("Merging using " + table + " table.")
		var rows *sql.Rows
		query := fmt.Sprintf(
			"select k, cnt from (select %s as k, count(distinct uuid) as cnt from %s "+
				"where name is not null and email is not null and email regexp ? "+
				"group by k) sub where sub.cnt > 1",
			reStr,
			table,
		)
		if debug > 0 {
			log.Info(fmt.Sprintf("main query[%s, %s]: %s\n", reVal, emailRE, query))
			fmt.Printf("main query[%s, %s]: %s\n", reVal, emailRE, query)
		}
		// Only RW connection is used - this API mass updates data
		rows, err = s.Query(s.db, nil, query, reVal, emailRE)
		if err != nil {
			return
		}
		packSize := 1000
		rawKey := ""
		key := ""
		keysPacks := [][]interface{}{}
		keys := []interface{}{}
		n := 0
		cnt := 0
		for rows.Next() {
			err = rows.Scan(&rawKey, &cnt)
			if err != nil {
				return
			}
			// key = strings.TrimSpace(strings.ToLower(s.StripUnicode(rawKey)))
			key = s.StripUnicode(rawKey)
			if strings.HasSuffix(key, "@@@") {
				key = rawKey
			}
			if debug > 0 {
				log.Info(fmt.Sprintf("%s --> %d\n", key, cnt))
			}
			keys = append(keys, key)
			n++
			if n == packSize {
				keysPacks = append(keysPacks, keys)
				keys = []interface{}{}
				n = 0
			}
			if key != rawKey {
				if debug > 0 {
					log.Info(fmt.Sprintf("special %s,%s --> %d\n", rawKey, key, cnt))
				}
				keys = append(keys, rawKey)
				n++
				if n == packSize {
					keysPacks = append(keysPacks, keys)
					keys = []interface{}{}
					n = 0
				}
			}
		}
		err = rows.Err()
		if err != nil {
			return
		}
		err = rows.Close()
		if err != nil {
			return
		}
		if n > 0 {
			keysPacks = append(keysPacks, keys)
		}
		nKeysPacks := len(keysPacks)
		log.Warn(fmt.Sprintf("Keys %d-packs to merge: %d\n", packSize, nKeysPacks))
		nKeys := 0
		for _, keys := range keysPacks {
			nKeys += len(keys)
		}
		log.Warn(fmt.Sprintf("Profiles to merge: %d\n", nKeys))
		merges := map[string]map[string]struct{}{}
		thrN := runtime.NumCPU()
		runtime.GOMAXPROCS(thrN)
		var mtx *sync.Mutex
		if thrN > 1 {
			mtx = &sync.Mutex{}
		}
		processPack := func(ch chan error, i int, keys []interface{}) (err error) {
			defer func() {
				if ch != nil {
					ch <- err
				}
			}()
			query := fmt.Sprintf("select %s, uuid from %s where name is not null and email is not null and %s in (", reStr, table, reStr)
			args := []interface{}{reVal, reVal}
			for _, key := range keys {
				query += "?,"
				args = append(args, key)
			}
			query = query[0:len(query)-1] + ")"
			var rows *sql.Rows
			if debug > 0 {
				log.Info(fmt.Sprintf("#%d pack query:\n%s\n%+v\n", i, query, args))
			}
			rows, err = s.Query(s.db, nil, query, args...)
			if err != nil {
				return
			}
			uuid := ""
			key := ""
			rawKey := ""
			uuids := make(map[string]map[string]struct{})
			for rows.Next() {
				err = rows.Scan(&rawKey, &uuid)
				if err != nil {
					return
				}
				// key = strings.TrimSpace(strings.ToLower(s.StripUnicode(rawKey)))
				key = s.StripUnicode(rawKey)
				if strings.HasSuffix(key, "@@@") {
					key = rawKey
				}
				if debug > 0 {
					log.Info(fmt.Sprintf("key: %s: uuid: %s\n", key, uuid))
				}
				_, ok := uuids[key]
				if !ok {
					uuids[key] = make(map[string]struct{})
				}
				uuids[key][uuid] = struct{}{}
				if key != rawKey {
					if debug > 0 {
						log.Info(fmt.Sprintf("special rawKey: %s key: %s: uuid: %s\n", rawKey, key, uuid))
					}
					_, ok := uuids[rawKey]
					if !ok {
						uuids[rawKey] = make(map[string]struct{})
					}
				}
			}
			err = rows.Err()
			if err != nil {
				return
			}
			err = rows.Close()
			if err != nil {
				return
			}
			if mtx != nil {
				mtx.Lock()
			}
			for key, ids := range uuids {
				merges[key] = make(map[string]struct{})
				for uuid := range ids {
					merges[key][uuid] = struct{}{}
				}
			}
			if mtx != nil {
				mtx.Unlock()
			}
			nKeys := len(keys)
			log.Warn(fmt.Sprintf("%d/%d packs %d keys\n", i+1, nKeysPacks, nKeys))
			return
		}
		if thrN > 1 {
			ch := make(chan error)
			nThreads := 0
			for i, keys := range keysPacks {
				go processPack(ch, i, keys)
				nThreads++
				if nThreads == thrN {
					err = <-ch
					nThreads--
					if err != nil {
						return
					}
				}
			}
			for nThreads > 0 {
				err = <-ch
				nThreads--
				if err != nil {
					return
				}
			}
		} else {
			for i, keys := range keysPacks {
				err = processPack(nil, i, keys)
				if err != nil {
					return
				}
			}
		}
		for key, uuids := range merges {
			l := len(uuids)
			if l <= 1 {
				log.Warn(fmt.Sprintf("Key %+v deleted - had only %d uuids: %+v\n", strings.Split(key, "@@@"), l, uuids))
				delete(merges, key)
				continue
			}
			if l > 25 {
				log.Warn(fmt.Sprintf("Key %+v deleted - had more than 25 uuids (%d): %+v\n", strings.Split(key, "@@@"), l, uuids))
				delete(merges, key)
				continue
			}
			if debug > 0 || l > 10 {
				log.Info(fmt.Sprintf("Key %+v has %d uuids: %+v\n", strings.Split(key, "@@@"), l, uuids))
			}
		}
		nMergeOps := len(merges)
		nMerges := 0
		for _, uuids := range merges {
			nMerges += len(uuids) - 1
		}
		log.Warn(fmt.Sprintf("UUIDs to merge: %d in %d operations (before dedup)\n", nMerges, nMergeOps))
		if nMergeOps == 0 || nMerges == 0 {
			if status == "" {
				status = table + ": Nothing to merge"
			} else {
				status += ", " + table + ": Nothing to merge"
			}
			continue
		}
		iter := 0
		allHits := 0
		processed := make(map[string]struct{})
		for {
			iter++
			hits := 0
			for key, uuids := range merges {
				if debug > 0 {
					log.Info(fmt.Sprintf("merge key:%s\n", key))
				}
				for uuid := range uuids {
					if debug > 0 {
						log.Info(fmt.Sprintf(">> key:%s,uuid:%s\n", key, uuid))
					}
					for key2, uuids2 := range merges {
						if key2 == key {
							continue
						}
						_, ok := processed[key]
						if ok {
							continue
						}
						_, ok = merges[key2][uuid]
						if ok {
							hits++
							if debug > 1 {
								log.Info(fmt.Sprintf("iter #%d (hits %d) %s present in %+v\n", iter, hits, uuid, uuids2))
							}
							for uuid2 := range uuids2 {
								merges[key][uuid2] = struct{}{}
							}
							delete(merges, key2)
						}
					}
				}
				processed[key] = struct{}{}
			}
			log.Warn(fmt.Sprintf("Dedup step #%d finished with %d hits\n", iter, hits))
			if hits == 0 {
				break
			}
			allHits += hits
			if iter > 50 {
				log.Warn("Wasn't able to fully dedup in 50 steps, using single-threaded merge to avoid transaction deadlocks\n")
				thrN = 1
				break
			}
		}
		log.Warn(fmt.Sprintf("Dedup finished with %d hits\n", allHits))
		for key, uuids := range merges {
			l := len(uuids)
			if l <= 1 {
				log.Warn(fmt.Sprintf("Key %+v deleted - had only %d uuids: %+v\n", strings.Split(key, "@@@"), l, uuids))
				delete(merges, key)
				continue
			}
			if l > 25 {
				log.Warn(fmt.Sprintf("Key %+v deleted - had more than 25 uuids (%d): %+v\n", strings.Split(key, "@@@"), l, uuids))
				delete(merges, key)
				continue
			}
			if debug > 0 {
				log.Info(fmt.Sprintf("Key %+v has %d uuids: %+v\n", strings.Split(key, "@@@"), l, uuids))
			}
			if debug > 0 || l > 10 {
				log.Warn(fmt.Sprintf("Key %+v has %d uuids: %+v\n", strings.Split(key, "@@@"), l, uuids))
			}
		}
		nMergeOps = len(merges)
		nMerges = 0
		for _, uuids := range merges {
			nMerges += len(uuids) - 1
		}
		log.Warn(fmt.Sprintf("UUIDs to merge: %d in %d operations (after dedup in %d steps)\n", nMerges, nMergeOps, iter))
		if nMergeOps == 0 || nMerges == 0 {
			if status == "" {
				status = table + ": Nothing to merge"
			} else {
				status += ", " + table + ": Nothing to merge"
			}
			continue
		}
		currIndex := 0
		actualMerges := 0
		type mergeResult struct {
			key string
			err error
		}
		mergeFunc := func(ch chan mergeResult, key string, uuids []string) (result mergeResult) {
			toUUID := uuids[0]
			var err error
			defer func() {
				result = mergeResult{err: err, key: strings.Join(uuids, ",")}
				if ch != nil {
					if err != nil {
						err = errs.Wrap(err, toUUID)
					}
					ch <- result
				}
			}()
			query := "select uuid, count(distinct id) as cnt from identities where uuid in ("
			args := []interface{}{}
			for _, uuid := range uuids {
				query += "?,"
				args = append(args, uuid)
			}
			query = query[0:len(query)-1] + ") group by uuid order by cnt desc limit 1"
			var rows *sql.Rows
			rows, err = s.Query(s.db, nil, query, args...)
			if err != nil {
				return
			}
			cnt := 0
			for rows.Next() {
				err = rows.Scan(&toUUID, &cnt)
				if err != nil {
					return
				}
			}
			err = rows.Err()
			if err != nil {
				return
			}
			err = rows.Close()
			if err != nil {
				return
			}
			if dry {
				log.Info(fmt.Sprintf("dry-run: would merge %+v into %s (which has %d identities)\n", uuids, toUUID, cnt))
				return
			}
			if debug > 0 {
				log.Info(fmt.Sprintf("merging %+v into %s (which has %d identities)\n", uuids, toUUID, cnt))
			}
			tx, err := s.db.Begin()
			if err != nil {
				return
			}
			defer func() {
				if tx != nil {
					tx.Rollback()
				}
			}()
			didMerges := 0
			nUUIDs := len(uuids)
			for idx, fromUUID := range uuids {
				if fromUUID == toUUID {
					continue
				}
				_, e := s.GetUniqueIdentity(fromUUID, true, nil)
				if e != nil {
					err = e
					return
				}
				toUU, e := s.GetUniqueIdentity(toUUID, true, nil)
				if e != nil {
					err = e
					return
				}
				from, e := s.GetProfile(fromUUID, false, nil)
				if e != nil {
					err = e
					return
				}
				to, e := s.GetProfile(toUUID, false, nil)
				if e != nil {
					err = e
					return
				}
				archivedDate := time.Now()
				_, e = s.ArchiveUUID(fromUUID, &archivedDate, tx)
				if e != nil {
					err = e
					return
				}
				_, e = s.ArchiveUUID(toUUID, &archivedDate, tx)
				if e != nil {
					err = e
					return
				}
				if from != nil && to != nil {
					if to.Name == nil || (to.Name != nil && *to.Name == "") {
						to.Name = from.Name
					}
					if to.Email == nil || (to.Email != nil && *to.Email == "") {
						to.Email = from.Email
					}
					if to.CountryCode == nil || (to.CountryCode != nil && *to.CountryCode == "") {
						to.CountryCode = from.CountryCode
					}
					if to.Gender == nil || (to.Gender != nil && *to.Gender == "") {
						to.Gender = from.Gender
						to.GenderAcc = from.GenderAcc
					}
					if from.IsBot != nil && *from.IsBot == 1 {
						isBot := int64(1)
						to.IsBot = &isBot
					}
					// Update profile and refresh after update
					to, e = s.EditProfile(to, true, tx)
					if e != nil {
						err = e
						return
					}
				}
				identities, e := s.GetUniqueIdentityIdentities(fromUUID, false, tx)
				if e != nil {
					err = e
					return
				}
				for _, identity := range identities {
					e = s.MoveIdentityToUniqueIdentity(identity, toUU, false, tx)
					if e != nil {
						err = e
						return
					}
				}
				enrollments, e := s.GetUniqueIdentityEnrollments(fromUUID, false, tx)
				if e != nil {
					err = e
					return
				}
				for _, rol := range enrollments {
					rols := []*models.EnrollmentDataOutput{}
					rols, e = s.FindEnrollments(
						[]string{"uuid", "organization_id", "start", "end", "project_slug", "role"},
						[]interface{}{toUUID, rol.OrganizationID, rol.Start, rol.End, rol.ProjectSlug, rol.Role},
						[]bool{false, false, true, true, false, false},
						false,
						tx,
					)
					if e != nil {
						err = e
						return
					}
					if len(rols) == 0 {
						e = s.MoveEnrollmentToUniqueIdentity(rol, toUU, tx)
						if e != nil {
							err = e
							return
						}
					}
				}
				// Delete unique identity archiving it to uidentities_archive
				e = s.DeleteUniqueIdentity(fromUUID, false, true, nil, tx)
				if e != nil {
					err = e
					return
				}
				orgs, e := s.FindUniqueIdentityOrganizations(toUUID, false, tx)
				if e != nil {
					err = e
					return
				}
				for _, org := range orgs {
					e = s.MergeEnrollments(toUU, org, nil, true, true, tx)
					if e != nil {
						err = e
						return
					}
				}
				didMerges++
				if debug > 0 {
					fmt.Printf("merged %d/%d %s --> %s\n", idx+1, nUUIDs, fromUUID, toUUID)
				}
			}
			err = tx.Commit()
			if err != nil {
				return
			}
			// Set tx to nil, so deferred rollback will not happen
			tx = nil
			if mtx != nil {
				mtx.Lock()
			}
			currIndex++
			i := currIndex
			actualMerges += didMerges
			soFar := actualMerges
			if mtx != nil {
				mtx.Unlock()
			}
			if debug > 0 {
				log.Info(fmt.Sprintf("merged %d %+v\n", nUUIDs, uuids))
			}
			log.Info(fmt.Sprintf("%d/%d merges (%s, %d profiles, %d merges so far)\n", i, nMergeOps, key, nUUIDs, soFar))
			return
		}
		nErrs := 0
		errsStr := ""
		merging := make(map[string]struct{})
		nProc := 0
		infoMerging := func() {
			if debug > 0 && nProc%10 == 0 {
				log.Info(fmt.Sprintf("currently merging %d (%d/%d finished): %+v\n", len(merging), nProc, nMergeOps, merging))
			}
		}
		if thrN > 1 {
			ch := make(chan mergeResult)
			nThreads := 0
			for key, uuidsMap := range merges {
				uuids := []string{}
				for uuid := range uuidsMap {
					uuids = append(uuids, uuid)
				}
				merging[strings.Join(uuids, ",")] = struct{}{}
				go mergeFunc(ch, key, uuids[:])
				nThreads++
				if nThreads == thrN {
					res := <-ch
					delete(merging, res.key)
					e := res.err
					nThreads--
					if e != nil {
						log.Warn("Merge error: " + e.Error())
						errsStr += e.Error() + " "
						nErrs++
					}
					nProc++
					infoMerging()
				}
			}
			for nThreads > 0 {
				res := <-ch
				delete(merging, res.key)
				e := res.err
				nThreads--
				if e != nil {
					log.Warn("Merge error: " + e.Error())
					errsStr += e.Error() + " "
					nErrs++
				}
				nProc++
				infoMerging()
			}
		} else {
			for key, uuidsMap := range merges {
				uuids := []string{}
				for uuid := range uuidsMap {
					uuids = append(uuids, uuid)
				}
				merging[strings.Join(uuids, ",")] = struct{}{}
				res := mergeFunc(nil, key, uuids)
				delete(merging, res.key)
				e := res.err
				if e != nil {
					log.Warn("Merge error: " + e.Error())
					errsStr += e.Error() + " "
					nErrs++
				}
				nProc++
				infoMerging()
			}
		}
		sep := ""
		if status == "" {
			sep = table
		} else {
			sep = ", " + table
		}
		sep += ": "
		if nErrs > 0 {
			status += fmt.Sprintf("%sMerged %d profiles, %d errors: %s", sep, actualMerges, nErrs, errsStr)
		} else {
			status += fmt.Sprintf("%sMerged %d profiles", sep, actualMerges)
		}
	}
	if dry {
		status = "Dry-run: " + status
	}
	return
}

func (s *service) MergeUniqueIdentities(fromUUID, toUUID string, archive bool) (updateESUUID string, updateESIsBot bool, err error) {
	log.Info(fmt.Sprintf("MergeUniqueIdentities: fromUUID:%s toUUID:%s archive:%v", fromUUID, toUUID, archive))
	// s.SetOrigin()
	defer func() {
		log.Info(fmt.Sprintf("MergeUniqueIdentities(exit): fromUUID:%s toUUID:%s archive:%v updateESUUID:%s updateESIsBot:%v err:%v", fromUUID, toUUID, archive, updateESUUID, updateESIsBot, err))
	}()
	if fromUUID == toUUID {
		return
	}
	_, err = s.GetUniqueIdentity(fromUUID, true, nil)
	if err != nil {
		return
	}
	toUU, err := s.GetUniqueIdentity(toUUID, true, nil)
	if err != nil {
		return
	}
	from, err := s.GetProfile(fromUUID, false, nil)
	if err != nil {
		return
	}
	to, err := s.GetProfile(toUUID, false, nil)
	if err != nil {
		return
	}
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	// Rollback unless tx was set to nil after successful commit
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	// Archive fromUUID and toUUID objects, all with the same archived_at date
	if archive {
		archivedDate := time.Now()
		_, err = s.ArchiveUUID(fromUUID, &archivedDate, tx)
		if err != nil {
			return
		}
		_, err = s.ArchiveUUID(toUUID, &archivedDate, tx)
		if err != nil {
			return
		}
	}
	if from != nil && to != nil {
		if to.Name == nil || (to.Name != nil && *to.Name == "") {
			to.Name = from.Name
		}
		if to.Email == nil || (to.Email != nil && *to.Email == "") {
			to.Email = from.Email
		}
		if to.CountryCode == nil || (to.CountryCode != nil && *to.CountryCode == "") {
			to.CountryCode = from.CountryCode
		}
		if to.Gender == nil || (to.Gender != nil && *to.Gender == "") {
			to.Gender = from.Gender
			to.GenderAcc = from.GenderAcc
		}
		// Do we need to mass update is_bot on all ES indices
		// on the fromUUID profile that will be merged into toUUID?
		if from.IsBot != nil && to.IsBot != nil && *from.IsBot != *to.IsBot {
			updateESUUID = fromUUID
			if *from.IsBot == 1 {
				updateESIsBot = true
			} else {
				updateESIsBot = false
			}
		}
		if from.IsBot != nil && *from.IsBot == 1 {
			isBot := int64(1)
			to.IsBot = &isBot
		}
		// Update profile and refresh after update
		to, err = s.EditProfile(to, true, tx)
		if err != nil {
			return
		}
	}
	identities, err := s.GetUniqueIdentityIdentities(fromUUID, false, tx)
	if err != nil {
		return
	}
	for _, identity := range identities {
		err = s.MoveIdentityToUniqueIdentity(identity, toUU, false, tx)
		if err != nil {
			return
		}
	}
	enrollments, err := s.GetUniqueIdentityEnrollments(fromUUID, false, tx)
	if err != nil {
		return
	}
	for _, rol := range enrollments {
		rols := []*models.EnrollmentDataOutput{}
		rols, err = s.FindEnrollments(
			[]string{"uuid", "organization_id", "start", "end", "project_slug", "role"},
			[]interface{}{toUUID, rol.OrganizationID, rol.Start, rol.End, rol.ProjectSlug, rol.Role},
			[]bool{false, false, true, true, false, false},
			false,
			tx,
		)
		if err != nil {
			return
		}
		if len(rols) == 0 {
			err = s.MoveEnrollmentToUniqueIdentity(rol, toUU, tx)
			if err != nil {
				return
			}
		}
	}
	// Delete unique identity archiving it to uidentities_archive
	err = s.DeleteUniqueIdentity(fromUUID, false, true, nil, tx)
	if err != nil {
		return
	}
	orgs, err := s.FindUniqueIdentityOrganizations(toUUID, false, tx)
	if err != nil {
		return
	}
	for _, org := range orgs {
		err = s.MergeEnrollments(toUU, org, nil, true, true, tx)
		if err != nil {
			return
		}
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	// Set tx to nil, so deferred rollback will not happen
	tx = nil
	return
}

func (s *service) Unarchive(id, uuid string) (unarchived bool, err error) {
	log.Info(fmt.Sprintf("Unarchive: ID:%s UUID:%s", id, uuid))
	defer func() {
		log.Info(fmt.Sprintf("Unarchive(exit): ID:%s UUID:%s unarchived:%v err:%v", id, uuid, unarchived, err))
	}()
	// Unarchive uses RW connection, also for selects
	rows, err := s.Query(s.db, nil, "select max(archived_at) from identities_archive where id = ?", id)
	if err != nil {
		return
	}
	var archivedAt [2]*time.Time
	fetched := false
	for rows.Next() {
		err = rows.Scan(&archivedAt[0])
		if err != nil {
			return
		}
		if archivedAt[0] != nil {
			fetched = true
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if !fetched {
		return
	}
	rows, err = s.Query(s.db, nil, "select max(archived_at) from uidentities_archive where uuid = ?", uuid)
	if err != nil {
		return
	}
	fetched = false
	for rows.Next() {
		err = rows.Scan(&archivedAt[1])
		if err != nil {
			return
		}
		if archivedAt[1] != nil {
			fetched = true
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if !fetched {
		return
	}
	if *archivedAt[0] != *archivedAt[1] {
		log.Info(fmt.Sprintf("archives exists, but not from the same archiving process: %v differs from %v", *archivedAt[0], *archivedAt[1]))
		return
	}
	tm := *archivedAt[0]
	rows, err = s.Query(s.db, nil, "select distinct uuid from uidentities_archive where archived_at = ?", tm)
	if err != nil {
		return
	}
	uuids := []string{}
	uu := ""
	for rows.Next() {
		err = rows.Scan(&uu)
		if err != nil {
			return
		}
		uuids = append(uuids, uu)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if len(uuids) != 2 {
		log.Info(fmt.Sprintf("there should be exactly 2 uuids archived at %v, found %+v", tm, uuids))
		return
	}
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	// Rollback unless tx was set to nil after successful commit
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	for _, uu := range uuids {
		err = s.UnarchiveUUID(uu, tm, tx)
		if err != nil {
			return
		}
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	// Set tx to nil, so deferred rollback will not happen
	tx = nil
	unarchived = true
	return
}

func (s *service) MoveIdentity(fromID, toUUID string, archive bool) (err error) {
	log.Info(fmt.Sprintf("MoveIdentity: fromID:%s toUUID:%s archive:%v", fromID, toUUID, archive))
	// s.SetOrigin()
	defer func() {
		log.Info(fmt.Sprintf("MoveIdentity(exit): fromID:%s toUUID:%s archive:%v err:%v", fromID, toUUID, archive, err))
	}()
	if archive {
		unarchived := false
		unarchived, err = s.Unarchive(fromID, toUUID)
		if err != nil {
			return
		}
		if unarchived {
			return
		}
		log.Info(fmt.Sprintf("MoveIdentity: fromID:%s toUUID:%s nothing unarchived", fromID, toUUID))
	}
	from, err := s.GetIdentity(fromID, true, nil)
	if err != nil {
		return
	}
	to, err := s.GetUniqueIdentity(toUUID, false, nil)
	if err != nil {
		return
	}
	if to == nil && fromID != toUUID {
		err = fmt.Errorf("unique identity uuid '%s' is not found and identity id is different: '%+v'", toUUID, s.ToLocalIdentity(from))
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "MoveIdentity")
		return
	}
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	// Rollback unless tx was set to nil after successful commit
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	if to == nil {
		to, err = s.AddUniqueIdentity(
			&models.UniqueIdentityDataOutput{
				UUID: toUUID,
			},
			false,
			tx,
		)
		if err != nil {
			return
		}
		_, err = s.AddProfile(
			&models.ProfileDataOutput{
				UUID:  toUUID,
				Name:  from.Name,
				Email: from.Email,
			},
			false,
			tx,
		)
		if err != nil {
			return
		}
	}
	err = s.MoveIdentityToUniqueIdentity(from, to, true, tx)
	if err != nil {
		return
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	// Set tx to nil, so deferred rollback will not happen
	tx = nil
	return
}

func (s *service) QueryOrganizationsDomains(orgID int64, q string, rows, page int64, tx *sql.Tx) (domains []*models.DomainDataOutput, nRows int64, err error) {
	log.Info(fmt.Sprintf("QueryOrganizationsDomains: orgID:%d q:%s rows:%d page:%d tx:%v", orgID, q, rows, page, tx != nil))
	defer func() {
		list := ""
		nDoms := len(domains)
		if nDoms > shared.LogListMax {
			list = fmt.Sprintf("%d", nDoms)
		} else {
			list = fmt.Sprintf("%+v", s.ToLocalDomains(domains))
		}
		log.Info(
			fmt.Sprintf(
				"QueryOrganizationsDomains(exit): orgID:%d q:%s rows:%d page:%d tx:%v orgs:%s n_rows:%d err:%v",
				orgID,
				q,
				rows,
				page,
				tx != nil,
				list,
				nRows,
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	args := []interface{}{}
	selRoot := "select o.id, o.name, do.id, do.domain, do.is_top_domain"
	sel := " from domains_organizations do, organizations o where do.organization_id = o.id"
	if q != "" {
		q = strings.TrimSpace(q)
		args = append(args, "%"+q+"%")
		sel += " and do.domain like ?"
	}
	if orgID > 0 {
		args = append(args, orgID)
		sel += " and o.id = ?"
	}
	sel += " order by o.name, do.domain"
	sel += fmt.Sprintf(" limit %d offset %d", rows, (page-1)*rows)
	var qrows *sql.Rows
	qrows, err = s.Query(sdb, tx, selRoot+sel, args...)
	if err != nil {
		return
	}
	var isTopDomain *bool
	for qrows.Next() {
		domain := &models.DomainDataOutput{}
		err = qrows.Scan(&domain.OrganizationID, &domain.OrganizationName, &domain.ID, &domain.Name, &isTopDomain)
		if err != nil {
			return
		}
		if isTopDomain != nil {
			domain.IsTopDomain = *isTopDomain
		}
		domains = append(domains, domain)
	}
	err = qrows.Err()
	if err != nil {
		return
	}
	err = qrows.Close()
	if err != nil {
		return
	}
	qrows, err = s.Query(sdb, tx, "select count(*)"+sel, args...)
	if err != nil {
		return
	}
	for qrows.Next() {
		err = qrows.Scan(&nRows)
		if err != nil {
			return
		}
	}
	err = qrows.Err()
	if err != nil {
		return
	}
	err = qrows.Close()
	if err != nil {
		return
	}
	return
}

func (s *service) GetAllAffiliations() (all *models.AllArrayOutput, err error) {
	all = &models.AllArrayOutput{}
	s.mtx.RLock()
	log.Info("GetAllAffiliations")
	defer func() {
		s.mtx.RUnlock()
		log.Info(fmt.Sprintf("GetAllAffiliations(exit): all:%d err:%v", len(all.Profiles), err))
	}()
	sel := "select distinct s.uuid, s.name, s.email, s.gender, s.is_bot, s.country_code, "
	sel += "i.id, i.name, i.email, i.username, i.source, s.id, s.start, s.end, s.project_slug, s.role, s.oname "
	sel += "from (select distinct u.uuid, p.name, p.email, p.gender, p.is_bot, p.country_code, "
	sel += "e.id, e.start, e.end, e.project_slug, e.role, o.name as oname from uidentities u, profiles p "
	sel += "left join enrollments e on e.uuid = p.uuid left join organizations o on o.id = e.organization_id "
	sel += "where u.uuid = p.uuid) s left join identities i on s.uuid = i.uuid"
	var rows *sql.Rows
	rows, err = s.Query(s.rodb, nil, sel)
	if err != nil {
		return
	}
	var (
		iID             *string
		iName           *string
		iEmail          *string
		iUsername       *string
		iSource         *string
		rolID           *int64
		rolStart        *strfmt.DateTime
		rolEnd          *strfmt.DateTime
		rolOrganization *string
		rolProjectSlug  *string
		rolRole         *string
	)
	uidsMap := make(map[string]*models.AllOutput)
	idsMap := make(map[string]*models.IdentityShortOutput)
	rolsMap := make(map[int64]*models.EnrollmentShortOutput)
	uuid := ""
	for rows.Next() {
		prof := &models.AllOutput{}
		id := &models.IdentityShortOutput{}
		rol := &models.EnrollmentShortOutput{}
		err = rows.Scan(
			&uuid, &prof.Name, &prof.Email, &prof.Gender, &prof.IsBot, &prof.CountryCode,
			&iID, &iName, &iEmail, &iUsername, &iSource,
			&rolID, &rolStart, &rolEnd, &rolProjectSlug, &rolRole, &rolOrganization,
		)
		if err != nil {
			return
		}
		if prof.Name != nil && strings.TrimSpace(*prof.Name) == "" {
			prof.Name = nil
		}
		if prof.Email != nil && strings.TrimSpace(*prof.Email) == "" {
			prof.Email = nil
		}
		if iName != nil && strings.TrimSpace(*iName) == "" {
			iName = nil
		}
		if iEmail != nil && strings.TrimSpace(*iEmail) == "" {
			iEmail = nil
		}
		if iUsername != nil && strings.TrimSpace(*iUsername) == "" {
			iUsername = nil
		}
		if prof.Name != nil {
			tmp := strings.TrimSpace(*prof.Name)
			prof.Name = &tmp
		}
		if iName != nil {
			tmp := strings.TrimSpace(*iName)
			iName = &tmp
		}
		if iUsername != nil {
			tmp := strings.TrimSpace(*iUsername)
			iUsername = &tmp
		}
		if iID != nil && iSource != nil {
			id = &models.IdentityShortOutput{
				Name:     iName,
				Email:    iEmail,
				Username: iUsername,
				Source:   *iSource,
			}
		}
		if rolID != nil && rolOrganization != nil {
			rol = &models.EnrollmentShortOutput{
				Start:        time.Time(*rolStart).Format(shared.DateFormat),
				End:          time.Time(*rolEnd).Format(shared.DateFormat),
				Organization: *rolOrganization,
				ProjectSlug:  rolProjectSlug,
				Role:         *rolRole,
			}
		}
		existingProf, ok := uidsMap[uuid]
		if !ok {
			uidsMap[uuid] = prof
		} else {
			prof = existingProf
		}
		if iID != nil {
			_, ok = idsMap[*iID]
			if !ok {
				prof.Identities = append(prof.Identities, id)
				idsMap[*iID] = id
			}
		}
		if rolID != nil {
			_, ok = rolsMap[*rolID]
			if !ok {
				prof.Enrollments = append(prof.Enrollments, rol)
				rolsMap[*rolID] = rol
			}
		}
		// @ -> ! + trim spaces
		s.SanitizeShortProfile(prof, true)
		uidsMap[uuid] = prof
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	for _, prof := range uidsMap {
		if len(prof.Enrollments) > 0 && len(prof.Identities) > 0 {
			all.Profiles = append(all.Profiles, prof)
		}
	}
	for k := range all.Profiles {
		if len(all.Profiles[k].Enrollments) > 1 {
			sort.SliceStable(all.Profiles[k].Enrollments, func(i, j int) bool {
				rols := all.Profiles[k].Enrollments
				a := &shared.LocalEnrollmentShortOutput{EnrollmentShortOutput: rols[i]}
				b := &shared.LocalEnrollmentShortOutput{EnrollmentShortOutput: rols[j]}
				return a.SortKey() < b.SortKey()
			})
		}
		if len(all.Profiles[k].Identities) > 1 {
			sort.SliceStable(all.Profiles[k].Identities, func(i, j int) bool {
				ids := all.Profiles[k].Identities
				a := &shared.LocalIdentityShortOutput{IdentityShortOutput: ids[i]}
				b := &shared.LocalIdentityShortOutput{IdentityShortOutput: ids[j]}
				return a.SortKey() < b.SortKey()
			})
		}
	}
	sort.SliceStable(all.Profiles, func(i, j int) bool {
		a := &shared.LocalAllOutput{AllOutput: all.Profiles[i]}
		b := &shared.LocalAllOutput{AllOutput: all.Profiles[j]}
		return a.SortKey(true) < b.SortKey(true)
	})
	return
}

func (s *service) QueryUniqueIdentitiesNested(q string, rows, page int64, identityRequired bool, projectSlugs []string, tx *sql.Tx) (uids []*models.UniqueIdentityNestedDataOutput, nRows int64, err error) {
	log.Info(fmt.Sprintf("QueryUniqueIdentitiesNested: q:%s rows:%d page:%d identityRequired:%v projectSlugs:%+v tx:%v", q, rows, page, identityRequired, projectSlugs, tx != nil))
	defer func() {
		list := ""
		nProfs := len(uids)
		if nProfs > shared.LogListMax {
			list = fmt.Sprintf("%d", nProfs)
		} else {
			list = fmt.Sprintf("%+v", s.ToLocalNestedUniqueIdentities(uids))
		}
		log.Info(
			fmt.Sprintf(
				"QueryUniqueIdentitiesNested(exit): q:%s rows:%d page:%d identityRequired:%v projectSlugs:%+v tx:%v uids:%s n_rows:%d err:%v",
				q,
				rows,
				page,
				identityRequired,
				projectSlugs,
				tx != nil,
				list,
				nRows,
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	args := []interface{}{}
	sel := ""
	where := ""
	qWhere := ""
	if q != "" {
		q = strings.TrimSpace(q)
		if strings.HasPrefix(q, "uuid=") {
			qWhere += "and u.uuid = ?"
			args = []interface{}{q[5:]}
		} else {
			qLike := "%" + q + "%"
			qWhere += "and (i.name like ? or i.email like ? or i.username like ? or i.source like ? or p.name like ? or p.email like ?)"
			args = []interface{}{qLike, qLike, qLike, qLike, qLike, qLike}
		}
	}
	if identityRequired {
		sel = "select distinct u.uuid from uidentities u, identities i, profiles p"
		where = "where u.uuid = i.uuid and u.uuid = p.uuid and i.uuid = p.uuid"
	} else {
		sel = "select distinct u.uuid from uidentities u, profiles p"
		where = "where u.uuid = p.uuid"
	}
	where += " " + qWhere + " order by 1"
	paging := fmt.Sprintf("limit %d offset %d", rows, (page-1)*rows)
	var qrows *sql.Rows
	query := sel + " " + where + " " + paging
	qrows, err = s.Query(sdb, tx, query, args...)
	if err != nil {
		return
	}
	uuids := []interface{}{}
	uuid := ""
	if identityRequired {
		sel = "select distinct u.uuid, u.last_modified, p.name, p.email, p.gender, p.gender_acc, p.is_bot, p.country_code, "
		sel += "i.id, i.name, i.email, i.username, i.source, i.last_modified, e.id, e.start, e.end, e.organization_id, e.project_slug, e.role, o.name "
		sel += "from uidentities u, identities i, profiles p "
		sel += "left join enrollments e on e.uuid = p.uuid left join organizations o on o.id = e.organization_id "
		sel += "where u.uuid = i.uuid and u.uuid = p.uuid and i.uuid = p.uuid and u.uuid in ("
	} else {
		sel = "select distinct s.uuid, s.last_modified, s.name, s.email, s.gender, s.gender_acc, s.is_bot, s.country_code, "
		sel += "i.id, i.name, i.email, i.username, i.source, i.last_modified, s.id, s.start, s.end, s.organization_id, s.project_slug, s.role, s.oname "
		sel += "from (select distinct u.uuid, u.last_modified, p.name, p.email, p.gender, p.gender_acc, p.is_bot, p.country_code, "
		sel += "e.id, e.start, e.end, e.organization_id, e.project_slug, e.role, o.name as oname from uidentities u, profiles p "
		sel += "left join enrollments e on e.uuid = p.uuid left join organizations o on o.id = e.organization_id "
		sel += "where u.uuid = p.uuid and u.uuid in ("
	}
	for qrows.Next() {
		err = qrows.Scan(&uuid)
		if err != nil {
			return
		}
		uuids = append(uuids, uuid)
		sel += "?,"
	}
	if len(uuids) < 1 {
		return
	}
	if identityRequired {
		sel = sel[0:len(sel)-1] + ") order by u.uuid, i.source, e.start"
	} else {
		sel = sel[0:len(sel)-1] + ")) s left join identities i on s.uuid = i.uuid order by s.uuid, i.source, s.start"
	}
	err = qrows.Err()
	if err != nil {
		return
	}
	err = qrows.Close()
	if err != nil {
		return
	}
	qrows, err = s.Query(sdb, tx, sel, uuids...)
	if err != nil {
		return
	}
	var (
		rolID             *int64
		rolStart          *strfmt.DateTime
		rolEnd            *strfmt.DateTime
		rolOrganizationID *int64
		rolOrganization   *string
		rolProjectSlug    *string
		rolRole           *string
		iID               *string
		iName             *string
		iEmail            *string
		iUsername         *string
		iSource           *string
		iLastModified     *strfmt.DateTime
	)
	uidsMap := make(map[string]*models.UniqueIdentityNestedDataOutput)
	idsMap := make(map[string]*models.IdentityDataOutput)
	rolsMap := make(map[int64]*models.EnrollmentNestedDataOutput)
	slugMap := make(map[string]struct{})
	for _, projectSlug := range projectSlugs {
		slugMap[projectSlug] = struct{}{}
	}
	for qrows.Next() {
		uid := &models.UniqueIdentityNestedDataOutput{}
		prof := &models.ProfileDataOutput{}
		id := &models.IdentityDataOutput{}
		rol := &models.EnrollmentNestedDataOutput{}
		err = qrows.Scan(
			&uid.UUID, &uid.LastModified,
			&prof.Name, &prof.Email, &prof.Gender, &prof.GenderAcc, &prof.IsBot, &prof.CountryCode,
			&iID, &iName, &iEmail, &iUsername, &iSource, &iLastModified,
			&rolID, &rolStart, &rolEnd, &rolOrganizationID, &rolProjectSlug, &rolRole, &rolOrganization,
		)
		if err != nil {
			return
		}
		uuid := uid.UUID
		prof.UUID = uuid
		addRol := false
		if rolID != nil && rolOrganization != nil {
			hit := false
			if rolProjectSlug != nil {
				_, hit = slugMap[*rolProjectSlug]
			}
			if rolProjectSlug == nil || hit {
				addRol = true
				rol = &models.EnrollmentNestedDataOutput{
					ID:             *rolID,
					UUID:           uuid,
					Start:          *rolStart,
					End:            *rolEnd,
					ProjectSlug:    rolProjectSlug,
					Role:           *rolRole,
					OrganizationID: *rolOrganizationID,
					Organization: &models.OrganizationDataOutput{
						ID:   *rolOrganizationID,
						Name: *rolOrganization,
					},
				}
			}
		}
		if iID != nil && iSource != nil {
			id = &models.IdentityDataOutput{
				ID:           *iID,
				UUID:         &uuid,
				Name:         iName,
				Email:        iEmail,
				Username:     iUsername,
				Source:       *iSource,
				LastModified: iLastModified,
			}
		}
		uidentity, ok := uidsMap[uuid]
		if !ok {
			uid.Profile = prof
			uidsMap[uuid] = uid
		}
		uidentity = uidsMap[uuid]
		if identityRequired || (!identityRequired && iID != nil && iSource != nil) {
			_, ok = idsMap[id.ID]
			if !ok {
				uidentity.Identities = append(uidentity.Identities, id)
				idsMap[id.ID] = id
			}
		}
		if addRol {
			_, ok = rolsMap[rol.ID]
			if !ok {
				uidentity.Enrollments = append(uidentity.Enrollments, rol)
				rolsMap[rol.ID] = rol
			}
		}
		uidsMap[uuid] = uidentity
	}
	err = qrows.Err()
	if err != nil {
		return
	}
	err = qrows.Close()
	if err != nil {
		return
	}
	suuids := []string{}
	for uuid := range uidsMap {
		suuids = append(suuids, uuid)
	}
	sort.Strings(suuids)
	for _, uuid := range suuids {
		uids = append(uids, uidsMap[uuid])
	}
	sel = "select count(distinct u.uuid) from uidentities u, identities i, profiles p"
	query = sel + " " + where
	qrows, err = s.Query(sdb, tx, query, args...)
	if err != nil {
		return
	}
	for qrows.Next() {
		err = qrows.Scan(&nRows)
		if err != nil {
			return
		}
	}
	err = qrows.Err()
	if err != nil {
		return
	}
	err = qrows.Close()
	if err != nil {
		return
	}
	return
}

func (s *service) QueryOrganizationsNested(q string, rows, page int64, tx *sql.Tx) (orgs []*models.OrganizationNestedDataOutput, nRows int64, err error) {
	log.Info(fmt.Sprintf("QueryOrganizationsNested: q:%s rows:%d page:%d tx:%v", q, rows, page, tx != nil))
	defer func() {
		list := ""
		nOrgs := len(orgs)
		if nOrgs > shared.LogListMax {
			list = fmt.Sprintf("%d", nOrgs)
		} else {
			list = fmt.Sprintf("%+v", s.ToLocalNestedOrganizations(orgs))
		}
		log.Info(
			fmt.Sprintf(
				"QueryOrganizationsNested(exit): q:%s rows:%d page:%d tx:%v orgs:%s n_rows:%d err:%v",
				q,
				rows,
				page,
				tx != nil,
				list,
				nRows,
				err,
			),
		)
	}()
	var (
		sel  string
		args []interface{}
	)
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	if q != "" {
		q = strings.TrimSpace(q)
		args = []interface{}{
			q,
			q + " %",
			q + "%",
			"% " + q,
			"%" + q,
			"%" + q + "%",
		}
		sel = `
    select distinct sub.id from (
    select 1 as rank, id, name from organizations where name = ?
    union all select 2 as rank, id, name from organizations where name like ?
    union all select 3 as rank, id, name from organizations where name like ?
    union all select 4 as rank, id, name from organizations where name like ?
    union all select 5 as rank, id, name from organizations where name like ?
    union all select 6 as rank, id, name from organizations where name like ?) sub
    order by sub.rank, sub.name
    `
	} else {
		sel = "select id from organizations o order by name"
	}
	sel += fmt.Sprintf(" limit %d offset %d", rows, (page-1)*rows)
	var qrows *sql.Rows
	if q == "" {
		qrows, err = s.Query(sdb, tx, sel)
	} else {
		qrows, err = s.Query(sdb, tx, sel, args...)
	}
	if err != nil {
		return
	}
	oids := []interface{}{}
	oid := int64(0)
	sel = "select distinct o.id, o.name, do.id, do.domain, do.is_top_domain from "
	sel += "organizations o left join domains_organizations do on o.id = do.organization_id"
	sel += " where o.id in ("
	for qrows.Next() {
		err = qrows.Scan(&oid)
		if err != nil {
			return
		}
		oids = append(oids, oid)
		sel += "?,"
	}
	if len(oids) < 1 {
		return
	}
	sel = sel[0:len(sel)-1] + ") order by o.name, do.domain"
	err = qrows.Err()
	if err != nil {
		return
	}
	err = qrows.Close()
	if err != nil {
		return
	}
	var (
		doid        *int64
		domainName  *string
		isTopDomain *bool
		oName       string
	)
	qrows, err = s.Query(sdb, tx, sel, oids...)
	if err != nil {
		return
	}
	orgsMap := make(map[int64]*models.OrganizationNestedDataOutput)
	for qrows.Next() {
		err = qrows.Scan(&oid, &oName, &doid, &domainName, &isTopDomain)
		if err != nil {
			return
		}
		org, ok := orgsMap[oid]
		if !ok {
			orgsMap[oid] = &models.OrganizationNestedDataOutput{ID: oid, Name: oName, Domains: []*models.DomainDataOutput{}}
		}
		if doid != nil {
			org = orgsMap[oid]
			org.Domains = append(org.Domains, &models.DomainDataOutput{ID: *doid, Name: *domainName, IsTopDomain: *isTopDomain, OrganizationID: oid, OrganizationName: oName})
			orgsMap[oid] = org
		}
	}
	for _, oid := range oids {
		orgs = append(orgs, orgsMap[oid.(int64)])
	}
	err = qrows.Err()
	if err != nil {
		return
	}
	err = qrows.Close()
	if err != nil {
		return
	}
	if q != "" {
		sel = `select count(distinct sub.id) from (
    select 1 as rank, id, name from organizations where name = ?
    union all select 2 as rank, id, name from organizations where name like ?
    union all select 3 as rank, id, name from organizations where name like ?
    union all select 4 as rank, id, name from organizations where name like ?
    union all select 5 as rank, id, name from organizations where name like ?
    union all select 6 as rank, id, name from organizations where name like ?) sub
    `
		qrows, err = s.Query(sdb, tx, sel, args...)
	} else {
		sel = "select count(*) from organizations"
		qrows, err = s.Query(sdb, tx, sel)
	}
	if err != nil {
		return
	}
	for qrows.Next() {
		err = qrows.Scan(&nRows)
		if err != nil {
			return
		}
	}
	err = qrows.Err()
	if err != nil {
		return
	}
	err = qrows.Close()
	if err != nil {
		return
	}
	return
}

func (s *service) QueryMatchingBlacklist(tx *sql.Tx, q string, rows, page int64) (matchingBlacklistOutput []*models.MatchingBlacklistOutput, nRows int64, err error) {
	log.Info(fmt.Sprintf("QueryMatchingBlacklist: q:%s rows:%d page:%d tx:%v", q, rows, page, tx != nil))
	defer func() {
		list := ""
		nEmails := len(matchingBlacklistOutput)
		if nEmails > shared.LogListMax {
			list = fmt.Sprintf("%d", nEmails)
		} else {
			list = fmt.Sprintf("%+v", s.ToLocalMatchingBlacklist(matchingBlacklistOutput))
		}
		log.Info(
			fmt.Sprintf(
				"QueryMatchingBlacklist(exit): q:%s rows:%d page:%d tx:%v matchingBlacklistOutput:%s n_rows:%d err:%v",
				q,
				rows,
				page,
				tx != nil,
				list,
				nRows,
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	qLike := ""
	sel := "select excluded from matching_blacklist"
	if q != "" {
		q = strings.TrimSpace(q)
		qLike = "%" + q + "%"
		sel += " where excluded like ?"
	}
	sel += " order by 1"
	sel += fmt.Sprintf(" limit %d offset %d", rows, (page-1)*rows)
	var qrows *sql.Rows
	if q == "" {
		qrows, err = s.Query(sdb, tx, sel)
	} else {
		qrows, err = s.Query(sdb, tx, sel, qLike)
	}
	if err != nil {
		return
	}
	for qrows.Next() {
		matchingBlacklistData := &models.MatchingBlacklistOutput{}
		err = qrows.Scan(&matchingBlacklistData.Excluded)
		if err != nil {
			return
		}
		matchingBlacklistOutput = append(matchingBlacklistOutput, matchingBlacklistData)
	}
	err = qrows.Err()
	if err != nil {
		return
	}
	err = qrows.Close()
	if err != nil {
		return
	}
	sel = "select count(*) from matching_blacklist"
	if q != "" {
		q = strings.TrimSpace(q)
		sel += " where excluded like ?"
	}
	if q == "" {
		qrows, err = s.Query(sdb, tx, sel)
	} else {
		qrows, err = s.Query(sdb, tx, sel, qLike)
	}
	if err != nil {
		return
	}
	for qrows.Next() {
		err = qrows.Scan(&nRows)
		if err != nil {
			return
		}
	}
	err = qrows.Err()
	if err != nil {
		return
	}
	err = qrows.Close()
	if err != nil {
		return
	}
	return
}

func (s *service) PostMatchingBlacklist(email string) (matchingBlacklistOutput *models.MatchingBlacklistOutput, err error) {
	log.Info(fmt.Sprintf("PostMatchingBlacklist: email:%s", email))
	// s.SetOrigin()
	defer func() {
		log.Info(
			fmt.Sprintf(
				"PostMatchingBlacklist(exit): email:%s matchingBlacklistOutput:%+v err:%v",
				email,
				matchingBlacklistOutput,
				err,
			),
		)
	}()
	matchingBlacklistOutput, err = s.AddMatchingBlacklist(&models.MatchingBlacklistOutput{Excluded: email}, false, nil)
	return
}

func (s *service) DeleteOrgDomain(organization, domain string) (status *models.TextStatusOutput, err error) {
	status = &models.TextStatusOutput{}
	log.Info(fmt.Sprintf("DeleteOrgDomain: organization:%s domain:%s", organization, domain))
	// s.SetOrigin()
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteOrgDomain(exit): organization:%s domain:%s status:%+v err:%v",
				organization,
				domain,
				status,
				err,
			),
		)
	}()
	err = s.DropOrgDomain(organization, domain, true, nil)
	if err == nil {
		status.Text = fmt.Sprintf("Deleted organization '%s' domain '%s': ", organization, domain)
	}
	return
}

func (s *service) DeleteMatchingBlacklist(email string) (status *models.TextStatusOutput, err error) {
	status = &models.TextStatusOutput{}
	log.Info(fmt.Sprintf("DeleteMatchingBlacklist: email:%s", email))
	// s.SetOrigin()
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteMatchingBlacklist(exit): email:%s status:%+v err:%v",
				email,
				status,
				err,
			),
		)
	}()
	err = s.DropMatchingBlacklist(email, true, nil)
	if err == nil {
		status.Text = "Deleted blacklist email: " + email
	}
	return
}

func (s *service) UnarchiveProfileNested(uuid string, projects []string) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("UnarchiveProfileNested: uuid:%s projects:%+v", uuid, projects))
	// s.SetOrigin()
	defer func() {
		log.Info(
			fmt.Sprintf(
				"UnarchiveProfileNested(exit): uuid:%s projects:%+v uid:%+v err:%v",
				uuid,
				projects,
				s.ToLocalNestedUniqueIdentity(uid),
				err,
			),
		)
	}()
	var rows *sql.Rows
	// Unarchive uses RW connection
	rows, err = s.Query(s.db, nil, "select max(archived_at) from uidentities_archive where uuid = ?", uuid)
	if err != nil {
		return
	}
	var archivedAt *time.Time
	fetched := false
	for rows.Next() {
		err = rows.Scan(&archivedAt)
		if err != nil {
			return
		}
		if archivedAt != nil {
			fetched = true
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if !fetched {
		err = fmt.Errorf("uuid '%s' no archive(s) found", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "UnarchiveProfileNested")
		return
	}
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	err = s.UnarchiveUUID(uuid, *archivedAt, tx)
	if err != nil {
		return
	}
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, _, err = s.QueryUniqueIdentitiesNested("uuid="+uuid, 1, 1, false, projects, tx)
	if err != nil {
		return
	}
	if len(ary) == 0 {
		err = fmt.Errorf("Unarchived profile with UUID '%s' not found", uuid)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "UnarchiveProfileNested")
		return
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	tx = nil
	uid = ary[0]
	return
}

func (s *service) DeleteProfileNested(uuid string, archive bool) (status *models.TextStatusOutput, err error) {
	status = &models.TextStatusOutput{}
	log.Info(fmt.Sprintf("DeleteProfileNested: uuid:%s archive:%v", uuid, archive))
	// s.SetOrigin()
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteProfileNested(exit): uuid:%s archive:%v status:%+v err:%v",
				uuid,
				archive,
				status,
				err,
			),
		)
	}()
	var tx *sql.Tx
	if archive {
		tx, err = s.db.Begin()
		if err != nil {
			return
		}
		defer func() {
			if tx != nil {
				tx.Rollback()
			}
		}()
		archivedDate := time.Now()
		_, err = s.ArchiveUUID(uuid, &archivedDate, tx)
		if err != nil {
			return
		}
	}
	err = s.DeleteUniqueIdentity(uuid, false, true, nil, tx)
	if err == nil {
		status.Text = fmt.Sprintf("Deleted profile uuid: '%s' (and all dependent objects), archive: %v", uuid, archive)
	}
	if tx != nil {
		err = tx.Commit()
		if err != nil {
			return
		}
		tx = nil
	}
	return
}

func (s *service) DeleteOrganization(id int64) (status *models.TextStatusOutput, err error) {
	status = &models.TextStatusOutput{}
	log.Info(fmt.Sprintf("DeleteOrganization: id:%d", id))
	// s.SetOrigin()
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteOrganization(exit): id:%d status:%+v err:%v",
				id,
				status,
				err,
			),
		)
	}()
	err = s.DropOrganization(id, true, nil)
	if err == nil {
		status.Text = fmt.Sprintf("Deleted organization id: %d", id)
	}
	return
}

func (s *service) GetListOrganizationsDomains(orgID int64, q string, rows, page int64) (getListOrganizationsDomains *models.GetListOrganizationsDomainsOutput, err error) {
	log.Info(fmt.Sprintf("GetListOrganizationsDomains: orgID:%d q:%s rows:%d page:%d", orgID, q, rows, page))
	// s.SetOrigin()
	getListOrganizationsDomains = &models.GetListOrganizationsDomainsOutput{}
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
				"GetListOrganizationsDomains(exit): orgID:%d q:%s rows:%d page:%d getListOrganizations:%s err:%v",
				orgID,
				q,
				rows,
				page,
				list,
				err,
			),
		)
	}()
	nRows := int64(0)
	var ary []*models.DomainDataOutput
	ary, nRows, err = s.QueryOrganizationsDomains(orgID, q, rows, page, nil)
	if err != nil {
		return
	}
	getListOrganizationsDomains.Domains = ary
	getListOrganizationsDomains.NRecords = nRows
	getListOrganizationsDomains.Rows = int64(len(ary))
	if rows == 0 {
		getListOrganizationsDomains.NPages = 1
	} else {
		pages := nRows / rows
		if nRows%rows != 0 {
			pages++
		}
		getListOrganizationsDomains.NPages = pages
	}
	getListOrganizationsDomains.Page = page
	if q != "" {
		getListOrganizationsDomains.Search = "q=" + q
	}
	return
}

func (s *service) GetListOrganizations(q string, rows, page int64) (getListOrganizations *models.GetListOrganizationsOutput, err error) {
	log.Info(fmt.Sprintf("GetListOrganizations: q:%s rows:%d page:%d", q, rows, page))
	// s.SetOrigin()
	getListOrganizations = &models.GetListOrganizationsOutput{}
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
				"GetListOrganizations(exit): q:%s rows:%d page:%d getListOrganizations:%s err:%v",
				q,
				rows,
				page,
				list,
				err,
			),
		)
	}()
	nRows := int64(0)
	var ary []*models.OrganizationNestedDataOutput
	ary, nRows, err = s.QueryOrganizationsNested(q, rows, page, nil)
	if err != nil {
		return
	}
	getListOrganizations.Organizations = ary
	getListOrganizations.NRecords = nRows
	getListOrganizations.Rows = int64(len(ary))
	if rows == 0 {
		getListOrganizations.NPages = 1
	} else {
		pages := nRows / rows
		if nRows%rows != 0 {
			pages++
		}
		getListOrganizations.NPages = pages
	}
	getListOrganizations.Page = page
	if q != "" {
		getListOrganizations.Search = "q=" + q
	}
	return
}

func (s *service) GetListProfiles(q string, rows, page int64, projectSlugs []string) (getListProfiles *models.GetListProfilesOutput, err error) {
	log.Info(fmt.Sprintf("GetListProfiles: q:%s rows:%d page:%d projectSlugs:%+v", q, rows, page, projectSlugs))
	// s.SetOrigin()
	getListProfiles = &models.GetListProfilesOutput{}
	defer func() {
		list := ""
		nprofs := len(getListProfiles.Uids)
		if nprofs > shared.LogListMax {
			list = fmt.Sprintf("%d", nprofs)
		} else {
			list = fmt.Sprintf("%+v", s.ToLocalNestedUniqueIdentities(getListProfiles.Uids))
		}
		log.Info(
			fmt.Sprintf(
				"GetListProfiles(exit): q:%s rows:%d page:%d projectSlugs:%+v getListProfiles:%s err:%v",
				q,
				rows,
				page,
				projectSlugs,
				list,
				err,
			),
		)
	}()
	nRows := int64(0)
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, nRows, err = s.QueryUniqueIdentitiesNested(q, rows, page, true, projectSlugs, nil)
	if err != nil {
		return
	}
	getListProfiles.Uids = ary
	getListProfiles.Rows = nRows
	if rows == 0 {
		getListProfiles.NPages = 1
	} else {
		pages := nRows / rows
		if nRows%rows != 0 {
			pages++
		}
		getListProfiles.NPages = pages
	}
	getListProfiles.Page = page
	if q != "" {
		getListProfiles.Search = "q=" + q
	}
	return
}

func (s *service) GetMatchingBlacklist(q string, rows, page int64) (getMatchingBlacklist *models.GetMatchingBlacklistOutput, err error) {
	log.Info(fmt.Sprintf("GetMatchingBlacklist: q:%s rows:%d page:%d", q, rows, page))
	// s.SetOrigin()
	getMatchingBlacklist = &models.GetMatchingBlacklistOutput{}
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
				"GetMatchingBlacklist(exit): q:%s rows:%d page:%d getMatchingBlacklist:%s err:%v",
				q,
				rows,
				page,
				list,
				err,
			),
		)
	}()
	var ary []*models.MatchingBlacklistOutput
	nRows := int64(0)
	ary, nRows, err = s.QueryMatchingBlacklist(nil, q, rows, page)
	if err != nil {
		return
	}
	getMatchingBlacklist.Emails = ary
	if rows == 0 {
		getMatchingBlacklist.NPages = 1
	} else {
		pages := nRows / rows
		if nRows%rows != 0 {
			pages++
		}
		getMatchingBlacklist.NPages = pages
	}
	getMatchingBlacklist.Page = page
	if q != "" {
		getMatchingBlacklist.Search = "q=" + q
	}
	getMatchingBlacklist.Rows = nRows
	return
}

// PutOrgDomain - add domain to organization
func (s *service) PutOrgDomain(org, dom string, overwrite, isTopDomain, skipEnrollments bool) (putOrgDomain *models.PutOrgDomainOutput, err error) {
	log.Info(fmt.Sprintf("PutOrgDomain: org:%s dom:%s overwrite:%v isTopDomain:%v skipEnrollments:%v", org, dom, overwrite, isTopDomain, skipEnrollments))
	// s.SetOrigin()
	putOrgDomain = &models.PutOrgDomainOutput{}
	org = strings.TrimSpace(org)
	dom = strings.TrimSpace(dom)
	defer func() {
		log.Info(fmt.Sprintf("PutOrgDomain(exit): org:%s dom:%s overwrite:%v isTopDomain:%v skipEnrollments:%v putOrgDomain:%+v err:%v", org, dom, overwrite, isTopDomain, skipEnrollments, putOrgDomain, err))
	}()
	// Uses RW connection only
	rows, err := s.Query(s.db, nil, "select id from organizations where name = ? limit 1", org)
	if err != nil {
		return
	}
	var orgID int
	fetched := false
	for rows.Next() {
		err = rows.Scan(&orgID)
		if err != nil {
			return
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if !fetched {
		err = fmt.Errorf("cannot find organization '%s'", org)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "PutOrgDomain")
		return
	}
	rows, err = s.Query(s.db, nil, "select 1 from domains_organizations where organization_id = ? and domain = ?", orgID, dom)
	if err != nil {
		return
	}
	dummy := 0
	for rows.Next() {
		err = rows.Scan(&dummy)
		if err != nil {
			return
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if dummy == 1 {
		err = fmt.Errorf("domain '%s' is already assigned to organization '%s'", dom, org)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "PutOrgDomain")
		return
	}
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	// Rollback unless tx was set to nil after successful commit
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	dom = strings.TrimSpace(dom)
	// s.SetOrigin()
	_, err = s.Exec(
		s.db,
		tx,
		"insert into domains_organizations(organization_id, domain, is_top_domain) select ?, ?, ?",
		orgID,
		dom,
		isTopDomain,
	)
	if err != nil {
		return
	}
	if !skipEnrollments {
		var res sql.Result
		affected := int64(0)
		if overwrite {
			res, err = s.Exec(
				s.db,
				tx,
				"delete from enrollments where uuid in (select distinct sub.uuid from ("+
					"select distinct uuid from profiles where email like ? "+
					"union select distinct uuid from identities where email like ?) sub)",
				"%"+dom,
				"%"+dom,
			)
			if err != nil {
				return
			}
			affected, err = res.RowsAffected()
			if err != nil {
				return
			}
			if affected > 0 {
				putOrgDomain.Deleted = fmt.Sprintf("%d", affected)
				putOrgDomain.Info = "deleted: " + putOrgDomain.Deleted
			}
			res, err = s.Exec(
				s.db,
				tx,
				"insert into enrollments(start, end, uuid, organization_id) "+
					"select distinct sub.start, sub.end, sub.uuid, sub.org_id from ("+
					"select '1900-01-01 00:00:00' as start, '2100-01-01 00:00:00' as end, uuid, ? as org_id from profiles where email like ? "+
					"union select '1900-01-01 00:00:00', '2100-01-01 00:00:00', uuid, ? from identities where email like ?) sub",
				orgID,
				"%"+dom,
				orgID,
				"%"+dom,
			)
			if err != nil {
				return
			}
			affected, err = res.RowsAffected()
			if err != nil {
				return
			}
			if affected > 0 {
				putOrgDomain.Added = fmt.Sprintf("%d", affected)
				if putOrgDomain.Info == "" {
					putOrgDomain.Info = "added: " + putOrgDomain.Added
				} else {
					putOrgDomain.Info += ", added: " + putOrgDomain.Added
				}
			}
		} else {
			res, err = s.Exec(
				s.db,
				tx,
				"insert into enrollments(start, end, uuid, organization_id) "+
					"select distinct sub.start, sub.end, sub.uuid, sub.org_id from ("+
					"select '1900-01-01 00:00:00' as start, '2100-01-01 00:00:00' as end, uuid, ? as org_id from profiles where email like ? "+
					"union select '1900-01-01 00:00:00', '2100-01-01 00:00:00', uuid, ? from identities where email like ?) sub "+
					"where sub.uuid not in (select distinct uuid from enrollments)",
				orgID,
				"%"+dom,
				orgID,
				"%"+dom,
			)
			if err != nil {
				return
			}
			affected, err = res.RowsAffected()
			if err != nil {
				return
			}
			if affected > 0 {
				putOrgDomain.Added = fmt.Sprintf("%d", affected)
				putOrgDomain.Info = "added: " + putOrgDomain.Added
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	// Set tx to nil, so deferred rollback will not happen
	tx = nil
	top := ""
	if isTopDomain {
		top = "top "
	}
	info := fmt.Sprintf("inserted '%s' %sdomain into '%s' organization", dom, top, org)
	if putOrgDomain.Info == "" {
		putOrgDomain.Info = info
	} else {
		putOrgDomain.Info += ", " + info
	}
	return
}

func (s *service) GetDetAffRangeSubjects() (subjects []*models.EnrollmentProjectRange, err error) {
	log.Info(fmt.Sprintf("GetDetAffRangeSubjects"))
	defer func() {
		log.Info(fmt.Sprintf("GetDetAffRangeSubjects(exit): subjects:%d err:%+v", len(subjects), err))
	}()
	rows, err := s.Query(
		s.rodb,
		nil,
		"select distinct sub.uuid, sub.project_slug, e.start, e.end from ("+
			"select uuid, project_slug, count(distinct organization_id) as cnt from enrollments "+ // was count(distinct id)
			"group by uuid, project_slug having cnt = 1) sub, enrollments e "+
			"where e.uuid = sub.uuid and (e.project_slug = sub.project_slug or "+
			"(e.project_slug is null and sub.project_slug is null)) and "+
			// "e.uuid = '20caea59977c497bad6bd1f33d6e8c9e54275bc2' and "+
			"(cast(e.start as time) != '00:00:07' or cast(e.end as time) != '00:00:07')", // skip special marked dates
	)
	if err != nil {
		return
	}
	mp := make(map[string][]*models.EnrollmentProjectRange)
	for rows.Next() {
		subject := &models.EnrollmentProjectRange{}
		err = rows.Scan(
			&subject.UUID,
			&subject.ProjectSlug,
			&subject.Start,
			&subject.End,
		)
		if err != nil {
			return
		}
		k := subject.UUID
		if subject.ProjectSlug != nil {
			k += *subject.ProjectSlug
		}
		_, ok := mp[k]
		if !ok {
			mp[k] = []*models.EnrollmentProjectRange{subject}
		} else {
			mp[k] = append(mp[k], subject)
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	for _, subs := range mp {
		subject := subs[0]
		for _, sub := range subs[1:] {
			if time.Time(sub.Start).Before(time.Time(subject.Start)) {
				subject.Start = sub.Start
			}
			if time.Time(sub.End).After(time.Time(subject.End)) {
				subject.End = sub.End
			}
		}
		subjects = append(subjects, subject)
	}
	return
}

func (s *service) UpdateProjectSlugs(uuidsProjs map[string][]string) (status string, err error) {
	log.Info(fmt.Sprintf("UpdateProjectSlugs: uuids:%d", len(uuidsProjs)))
	defer func() {
		log.Info(fmt.Sprintf("UpdateProjectSlugs(exit): uuids:%d status:%s err:%+v", len(uuidsProjs), status, err))
	}()
	type updateResult struct {
		err     error
		uuid    string
		added   int
		deleted int
	}
	// Updateprojects slugs uses RW connection only
	updateProjectSlug := func(ch chan updateResult, uuid string, projects []string) (res updateResult) {
		var err error
		defer func() {
			res.err = err
			if ch != nil {
				ch <- res
			}
		}()
		res.uuid = uuid
		var rows *sql.Rows
		rows, err = s.Query(
			s.db,
			nil,
			"select id, organization_id, role, start, end from enrollments where uuid = ? and project_slug is null",
			uuid,
		)
		if err != nil {
			return
		}
		type rolData struct {
			id    int64
			orgID int64
			role  string
			start strfmt.DateTime
			end   strfmt.DateTime
		}
		var (
			rol  rolData
			rols []rolData
		)
		uni := make(map[string]struct{})
		for rows.Next() {
			err = rows.Scan(&rol.id, &rol.orgID, &rol.role, &rol.start, &rol.end)
			if err != nil {
				return
			}
			key := fmt.Sprintf("%d-%s-%v-%v", rol.orgID, rol.role, rol.start, rol.end)
			_, ok := uni[key]
			if !ok {
				rols = append(rols, rol)
				uni[key] = struct{}{}
			}
		}
		err = rows.Err()
		if err != nil {
			return
		}
		err = rows.Close()
		if err != nil {
			return
		}
		if len(rols) == 0 {
			return
		}
		slugs := []string{}
		for _, project := range projects {
			var rows2 *sql.Rows
			rows2, err = s.Query(s.db, nil, "select id from enrollments where uuid = ? and project_slug = ? limit 1", uuid, project)
			if err != nil {
				return
			}
			dummy := 0
			fetched := false
			for rows2.Next() {
				err = rows2.Scan(&dummy)
				if err != nil {
					return
				}
				fetched = true
				break
			}
			err = rows2.Err()
			if err != nil {
				return
			}
			err = rows2.Close()
			if err != nil {
				return
			}
			if !fetched {
				slugs = append(slugs, project)
			}
		}
		if len(slugs) == 0 {
			return
		}
		args := []interface{}{}
		nRols := 0
		query := "insert into enrollments(uuid, organization_id, role, start, end, project_slug) values"
		for _, rol := range rols {
			start := time.Time(rol.start)
			end := time.Time(rol.end)
			for _, slug := range slugs {
				query += "(?,?,?,?,?,?),"
				args = append(args, uuid, rol.orgID, rol.role, start, end, slug)
				nRols++
			}
		}
		tx, err := s.db.Begin()
		if err != nil {
			return
		}
		defer func() {
			if tx != nil {
				tx.Rollback()
			}
		}()
		query = query[0 : len(query)-1]
		_, err = s.Exec(s.db, nil, query, args...)
		if err != nil {
			return
		}
		args = []interface{}{}
		query = "delete from enrollments where id in ("
		nDels := 0
		for _, rol := range rols {
			query += "?,"
			args = append(args, rol.id)
			nDels++
		}
		query = query[0:len(query)-1] + ")"
		_, err = s.Exec(s.db, nil, query, args...)
		if err != nil {
			return
		}
		err = tx.Commit()
		if err != nil {
			return
		}
		tx = nil
		res.added = nRols
		res.deleted = nDels
		return
	}
	processed := 0
	all := len(uuidsProjs)
	progressInfo := func() {
		processed++
		if processed%1000 == 0 {
			log.Info(fmt.Sprintf("Updated %d/%d\n", processed, all))
		}
	}
	thrN := s.GetThreadsNum()
	ern := 0
	oks := 0
	nuuids := 0
	rols := 0
	dels := 0
	if thrN > 1 {
		log.Info(fmt.Sprintf("Using %d parallel SH queries\n", thrN))
		ch := make(chan updateResult)
		nThreads := 0
		for uuid, projects := range uuidsProjs {
			go updateProjectSlug(ch, uuid, projects)
			nThreads++
			if nThreads == thrN {
				res := <-ch
				nThreads--
				if res.err != nil {
					log.Warn(res.err.Error())
					ern++
				} else {
					oks++
					if res.added > 0 {
						nuuids++
						rols += res.added
						dels += res.deleted
					}
				}
				progressInfo()
			}
		}
		for nThreads > 0 {
			res := <-ch
			nThreads--
			if res.err != nil {
				log.Warn(res.err.Error())
				ern++
			} else {
				oks++
				if res.added > 0 {
					nuuids++
					rols += res.added
					dels += res.deleted
				}
			}
			progressInfo()
		}
	} else {
		for uuid, projects := range uuidsProjs {
			res := updateProjectSlug(nil, uuid, projects)
			if res.err != nil {
				log.Warn(res.err.Error())
				ern++
			} else {
				oks++
				if res.added > 0 {
					nuuids++
					rols += res.added
					dels += res.deleted
				}
			}
			progressInfo()
		}
	}
	status = fmt.Sprintf(
		"All UUIDs: %d, Updated: %d (added %d rols, deleted %d rols), processed: %d, errors: %d",
		len(uuidsProjs),
		nuuids,
		rols,
		dels,
		oks,
		ern,
	)
	log.Info(status)
	return
}

func (s *service) UpdateAffRange(updates []*models.EnrollmentProjectRange) (status string, err error) {
	log.Info(fmt.Sprintf("UpdateAffRange: updates:%d", len(updates)))
	defer func() {
		log.Info(fmt.Sprintf("UpdateAffRange(exit): updates:%d status:%s err:%+v", len(updates), status, err))
	}()
	thrN := runtime.NumCPU()
	var mtx *sync.Mutex
	var umtx map[string]*sync.Mutex
	if thrN > 1 {
		mtx = &sync.Mutex{}
		umtx = make(map[string]*sync.Mutex)
	}
	updateFunc := func(ch chan error, update models.EnrollmentProjectRange) (err error) {
		defer func() {
			if ch != nil {
				ch <- err
			}
		}()
		queries := []string{}
		argss := [][]interface{}{}
		uuid := update.UUID
		ts := time.Time(update.Start)
		if ts.After(shared.MinPeriodDate) && ts.Before(shared.MaxPeriodDate) {
			query := "update enrollments set start = ? where uuid = ? "
			args := []interface{}{ts, uuid}
			if update.ProjectSlug == nil {
				query += "and project_slug is null "
			} else {
				query += "and project_slug = ? "
				args = append(args, *update.ProjectSlug)
			}
			query += "and start < ?"
			args = append(args, ts)
			queries = append(queries, query)
			argss = append(argss, args)
		}
		te := time.Time(update.End)
		if te.After(shared.MinPeriodDate) && te.Before(shared.MaxPeriodDate) && !te.Before(ts) {
			query := "update enrollments set end = ? where uuid = ? "
			args := []interface{}{te, uuid}
			if update.ProjectSlug == nil {
				query += "and project_slug is null "
			} else {
				query += "and project_slug = ? "
				args = append(args, *update.ProjectSlug)
			}
			query += "and end > ?"
			args = append(args, te)
			queries = append(queries, query)
			argss = append(argss, args)
		}
		query := "delete from enrollments where uuid = ? "
		args := []interface{}{uuid}
		if update.ProjectSlug == nil {
			query += "and project_slug is null "
		} else {
			query += "and project_slug = ? "
			args = append(args, *update.ProjectSlug)
		}
		query += "and end <= start"
		queries = append(queries, query)
		argss = append(argss, args)
		if mtx != nil {
			mtx.Lock()
			m, ok := umtx[uuid]
			if !ok {
				umtx[uuid] = &sync.Mutex{}
			}
			m = umtx[uuid]
			mtx.Unlock()
			m.Lock()
			defer func() {
				m.Unlock()
			}()
		}
		tx, err := s.db.Begin()
		if err != nil {
			return
		}
		defer func() {
			if tx != nil {
				tx.Rollback()
			}
		}()
		// fmt.Printf("%s: %+v\n", query, args)
		for idx, query := range queries {
			args := argss[idx]
			_, err = s.Exec(s.db, tx, query, args...)
			if err != nil {
				return
			}
		}
		err = tx.Commit()
		if err != nil {
			return
		}
		tx = nil
		return
	}
	processed := 0
	all := len(updates)
	progressInfo := func() {
		processed++
		if processed%500 == 0 {
			log.Info(fmt.Sprintf("Updated %d/%d\n", processed, all))
		}
	}
	e := 0
	ch := make(chan error)
	nThreads := 0
	if thrN > 0 {
		for _, update := range updates {
			go updateFunc(ch, *update)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				nThreads--
				if err != nil {
					log.Warn(err.Error())
					e++
					continue
				}
				progressInfo()
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				log.Warn(err.Error())
				e++
				continue
			}
			progressInfo()
		}
	} else {
		for _, update := range updates {
			err = updateFunc(nil, *update)
			if err != nil {
				log.Warn(err.Error())
				e++
				continue
			}
		}
		progressInfo()
	}
	status = fmt.Sprintf("Updated: %d, errors: %d", processed, e)
	log.Warn(status)
	return
}

/*
func (s *service) NotifySSAW() {
	go func() {
		e := ssawsync.Sync(s.origin)
		if e != nil {
			log.Warn(fmt.Sprintf("ssaw sync error for %s origin: %v\n", s.origin, e))
		}
	}()
}
*/

func (s *service) SetOrigin() {
	_, e := s.ExecDB(s.db, "set @origin = ?", s.origin)
	if e != nil {
		log.Warn(fmt.Sprintf("Unable to set origin to: %s: %v", s.origin, e))
	}
}

func (s *service) BulkUpdate(add, del []*models.AllOutput) (nAdded, nDeleted, nUpdated int, err error) {
	s.mtx.Lock()
	log.Info(fmt.Sprintf("BulkUpdate: add:%d del:%d", len(add), len(del)))
	// s.SetOrigin()
	/*
		defer func() {
			s.mtx.Unlock()
			log.Info(fmt.Sprintf("BulkUpdate(exit): add:%d del:%d err:%+v", len(add), len(del), err))
			// Trigger sync event
			s.NotifySSAW()
		}()
	*/
	mAdd := make(map[string]*models.AllOutput)
	mDel := make(map[string]*models.AllOutput)
	for _, obj := range add {
		// ! -> @ + trim spaces
		s.SanitizeShortProfile(obj, false)
		lobj := &shared.LocalAllOutput{AllOutput: obj}
		key := lobj.SortKey(true)
		mAdd[key] = obj
	}
	for _, obj := range del {
		// ! -> @ + trim spaces
		s.SanitizeShortProfile(obj, false)
		lobj := &shared.LocalAllOutput{AllOutput: obj}
		// key := strings.ToLower(lobj.SortKey(true))
		key := lobj.SortKey(true)
		mDel[key] = obj
	}
	sharedM := make(map[string]struct{})
	for k := range mAdd {
		_, ok := mDel[k]
		if ok {
			sharedM[k] = struct{}{}
		}
	}
	for k := range mDel {
		_, ok := mAdd[k]
		if ok {
			sharedM[k] = struct{}{}
		}
	}
	for k := range sharedM {
		log.Info(fmt.Sprintf("BulkUpdate: trying to add and delete the same object, skipping: '%s'", k))
		delete(mAdd, k)
		delete(mDel, k)
	}
	mAddProf := make(map[string]*models.AllOutput)
	mDelProf := make(map[string]*models.AllOutput)
	for _, obj := range mAdd {
		lobj := &shared.LocalAllOutput{AllOutput: obj}
		key := lobj.SortKey(false)
		p, ok := mAddProf[key]
		if ok {
			pobj := &shared.LocalAllOutput{AllOutput: p}
			err = fmt.Errorf(
				"attempt to add two profiles with the same profile data but different identities and/or enrollments: '%s' and '%s' both map into '%s'",
				pobj.SortKey(true),
				lobj.SortKey(true),
				key,
			)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "BulkUpdate")
			return
		}
		mAddProf[key] = obj
	}
	for _, obj := range mDel {
		lobj := &shared.LocalAllOutput{AllOutput: obj}
		key := lobj.SortKey(false)
		p, ok := mDelProf[key]
		if ok {
			pobj := &shared.LocalAllOutput{AllOutput: p}
			err = fmt.Errorf(
				"attempt to delete two profiles with the same profile data but different identities and/or enrollments: '%s' and '%s' both map into '%s'",
				pobj.SortKey(true),
				lobj.SortKey(true),
				key,
			)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "BulkUpdate")
			return
		}
		mDelProf[key] = obj
	}
	sharedProf := make(map[string]struct{})
	for k := range mAddProf {
		_, ok := mDelProf[k]
		if ok {
			sharedProf[k] = struct{}{}
		}
	}
	for k := range mDelProf {
		_, ok := mAddProf[k]
		if ok {
			sharedProf[k] = struct{}{}
		}
	}
	mUpdProf := make(map[string][2]*models.AllOutput)
	for k := range sharedProf {
		log.Info(fmt.Sprintf("BulkUpdate: detected update on profile '%s'", k))
		a := mAddProf[k]
		d := mDelProf[k]
		delete(mAddProf, k)
		delete(mDelProf, k)
		mUpdProf[k] = [2]*models.AllOutput{a, d}
	}
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	mOrgID := make(map[int64]*models.OrganizationDataOutput)
	mOrgName := make(map[string]*models.OrganizationDataOutput)
	archiveDate := time.Now()
	for _, prof := range mDelProf {
		foundProfs := []*models.ProfileDataOutput{}
		columns := []string{}
		values := []interface{}{}
		if prof.Name != nil {
			columns = append(columns, "name")
			values = append(values, *prof.Name)
		}
		if prof.Email != nil {
			columns = append(columns, "email")
			values = append(values, *prof.Email)
		}
		if len(columns) == 0 {
			obj := &shared.LocalAllOutput{AllOutput: prof}
			err = fmt.Errorf("profile to delete must have at least one profile data property set: (name, email), profile: '%s'", obj.SortKey(true))
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "BulkUpdate")
			return
		}
		if prof.Gender != nil {
			columns = append(columns, "gender")
			values = append(values, *prof.Gender)
		}
		if prof.IsBot != nil {
			columns = append(columns, "is_bot")
			values = append(values, *prof.IsBot)
		}
		if prof.CountryCode != nil {
			columns = append(columns, "country_code")
			values = append(values, *prof.CountryCode)
		}
		foundProfs, err = s.FindProfiles(columns, values, false, tx)
		if err != nil {
			return
		}
		nFoundProfs := len(foundProfs)
		obj := &shared.LocalAllOutput{AllOutput: prof}
		switch nFoundProfs {
		case 0:
			log.Info(fmt.Sprintf("BulkUpdate: delete profile '%s' - didn't found matching profiles, continuying", obj.SortKey(true)))
		case 1:
			uuid := foundProfs[0].UUID
			_, err = s.ArchiveUUID(uuid, &archiveDate, tx)
			if err != nil {
				return
			}
			err = s.DeleteUniqueIdentity(uuid, false, true, nil, tx)
			if err != nil {
				return
			}
			log.Info(fmt.Sprintf("BulkUpdate: delete profile '%s' - archived and deleted UUID '%s'", obj.SortKey(true), uuid))
		default:
			log.Info(fmt.Sprintf("BulkUpdate: delete profile '%s' found %d matching profiles to delete", obj.SortKey(true), nFoundProfs))
			if len(prof.Enrollments) == 0 && len(prof.Identities) == 0 {
				err = fmt.Errorf(
					"profile to delete '%s' has no enrollments and no identities (in delete record) and searching for this profile generates multiple results '%+v', cannot delete multiple objects",
					obj.SortKey(true),
					s.ToLocalProfiles(foundProfs),
				)
				err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "BulkUpdate")
				return
			}
			uuids := make(map[string]struct{})
			for _, foundProf := range foundProfs {
				for _, iden := range prof.Identities {
					columns := []string{"uuid", "source"}
					values := []interface{}{foundProf.UUID, iden.Source}
					isDates := []bool{false, false}
					if iden.Name != nil {
						columns = append(columns, "name")
						values = append(values, *iden.Name)
						isDates = append(isDates, false)
					}
					if iden.Email != nil {
						columns = append(columns, "email")
						values = append(values, *iden.Email)
						isDates = append(isDates, false)
					}
					if iden.Username != nil {
						columns = append(columns, "username")
						values = append(values, *iden.Username)
						isDates = append(isDates, false)
					}
					var identities []*models.IdentityDataOutput
					identities, err = s.FindIdentities(columns, values, isDates, false, tx)
					if err != nil {
						return
					}
					for _, identity := range identities {
						if identity.UUID == nil {
							continue
						}
						uuids[*identity.UUID] = struct{}{}
					}
				}
				for _, rol := range prof.Enrollments {
					var (
						start        time.Time
						end          time.Time
						organization *models.OrganizationDataOutput
						enrollments  []*models.EnrollmentDataOutput
						ok           bool
					)
					start, err = s.TimeParseAny(rol.Start)
					if err != nil {
						return
					}
					end, err = s.TimeParseAny(rol.End)
					if err != nil {
						return
					}
					organization, ok = mOrgName[rol.Organization]
					if !ok {
						organization, err = s.GetOrganizationByName(rol.Organization, true, tx)
						if err != nil {
							return
						}
						mOrgName[rol.Organization] = organization
						mOrgID[organization.ID] = organization
					}
					enrollments, err = s.FindEnrollments(
						[]string{"uuid", "start", "end", "organization_id", "project_slug", "role"},
						[]interface{}{
							foundProf.UUID,
							strfmt.DateTime(start),
							strfmt.DateTime(end),
							organization.ID,
							rol.ProjectSlug,
							rol.Role,
						},
						[]bool{false, true, true, false, false, false},
						false,
						tx,
					)
					if err != nil {
						return
					}
					for _, enrollment := range enrollments {
						uuids[enrollment.UUID] = struct{}{}
					}
				}
			}
			nUUIDs := len(uuids)
			switch nUUIDs {
			case 0:
				log.Info(fmt.Sprintf("BulkUpdate: delete profile '%s' - detailed search didn't found matching profiles, continuying", obj.SortKey(true)))
			case 1:
				uuid := ""
				for k := range uuids {
					uuid = k
					break
				}
				_, err = s.ArchiveUUID(uuid, &archiveDate, tx)
				if err != nil {
					return
				}
				err = s.DeleteUniqueIdentity(uuid, false, true, nil, tx)
				if err != nil {
					return
				}
				log.Info(fmt.Sprintf("BulkUpdate: delete profile '%s' - archived and deleted UUID '%s' after detailed search", obj.SortKey(true), uuid))
			default:
				log.Info(fmt.Sprintf("BulkUpdate: delete profile '%s' detailed search returned multiple UUIDs: %+v, skipping delete", obj.SortKey(true), uuids))
			}
		}
	}
	for _, prof := range mAddProf {
		foundProfs := []*models.ProfileDataOutput{}
		columns := []string{}
		values := []interface{}{}
		if prof.Name != nil {
			columns = append(columns, "name")
			values = append(values, *prof.Name)
		}
		if prof.Email != nil {
			columns = append(columns, "email")
			values = append(values, *prof.Email)
		}
		if len(columns) == 0 {
			obj := &shared.LocalAllOutput{AllOutput: prof}
			err = fmt.Errorf("profile to add must have at least one profile data property set: (name, email), profile: '%s'", obj.SortKey(true))
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "BulkUpdate")
			return
		}
		if prof.Gender != nil {
			columns = append(columns, "gender")
			values = append(values, *prof.Gender)
		}
		if prof.IsBot != nil {
			columns = append(columns, "is_bot")
			values = append(values, *prof.IsBot)
		}
		if prof.CountryCode != nil {
			columns = append(columns, "country_code")
			values = append(values, *prof.CountryCode)
		}
		foundProfs, err = s.FindProfiles(columns, values, false, tx)
		if err != nil {
			return
		}
		obj := &shared.LocalAllOutput{AllOutput: prof}
		if len(foundProfs) > 0 {
			log.Info(fmt.Sprintf("BulkUpdate: add profile '%s' - found %d matching profiles", obj.SortKey(true), len(foundProfs)))
			dels := []*models.AllOutput{}
			uuids := []string{}
			for _, foundProf := range foundProfs {
				var identities []*models.IdentityDataOutput
				var enrollments []*models.EnrollmentDataOutput
				identities, err = s.FindIdentities([]string{"uuid"}, []interface{}{foundProf.UUID}, []bool{false}, false, tx)
				if err != nil {
					return
				}
				enrollments, err = s.FindEnrollments([]string{"uuid"}, []interface{}{foundProf.UUID}, []bool{false}, false, tx)
				if err != nil {
					return
				}
				if len(identities) > 0 && len(enrollments) > 0 {
					obj := &shared.LocalAllOutput{AllOutput: prof}
					err = fmt.Errorf(
						"adding profile '%s' - such profile '%+v' '%+v' '%+v' already exists in database and has both identities and enrollments, you should find that profile and edit it instead",
						obj.SortKey(true),
						s.ToLocalProfile(foundProf),
						s.ToLocalIdentities(identities),
						s.ToLocalEnrollments(enrollments),
					)
					err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "BulkUpdate")
					return
				}
				log.Info(fmt.Sprintf(
					"BulkUpdate: add profile '%s' - found profile with missing identities or enrollments '%+v' '%+v' '%+v'",
					obj.SortKey(true),
					s.ToLocalProfile(foundProf),
					s.ToLocalIdentities(identities),
					s.ToLocalEnrollments(enrollments),
				))
				del := &models.AllOutput{
					Name:        foundProf.Name,
					Email:       foundProf.Email,
					Gender:      foundProf.Gender,
					IsBot:       foundProf.IsBot,
					CountryCode: foundProf.CountryCode,
				}
				for _, identity := range identities {
					iden := &models.IdentityShortOutput{
						Source:   identity.Source,
						Name:     identity.Name,
						Email:    identity.Email,
						Username: identity.Username,
					}
					del.Identities = append(del.Identities, iden)
				}
				sort.SliceStable(del.Identities, func(i, j int) bool {
					identities := del.Identities
					a := &shared.LocalIdentityShortOutput{IdentityShortOutput: identities[i]}
					b := &shared.LocalIdentityShortOutput{IdentityShortOutput: identities[j]}
					return a.SortKey() < b.SortKey()
				})
				for _, enrollment := range enrollments {
					var (
						organization *models.OrganizationDataOutput
						ok           bool
					)
					organization, ok = mOrgID[enrollment.OrganizationID]
					if !ok {
						organization, err = s.GetOrganization(enrollment.OrganizationID, true, tx)
						if err != nil {
							return
						}
						mOrgID[enrollment.OrganizationID] = organization
						mOrgName[organization.Name] = organization
					}
					rol := &models.EnrollmentShortOutput{
						Start:        time.Time(enrollment.Start).Format(shared.DateFormat),
						End:          time.Time(enrollment.End).Format(shared.DateFormat),
						Organization: organization.Name,
						ProjectSlug:  enrollment.ProjectSlug,
						Role:         enrollment.Role,
					}
					del.Enrollments = append(del.Enrollments, rol)
				}
				sort.SliceStable(del.Enrollments, func(i, j int) bool {
					rols := del.Enrollments
					a := &shared.LocalEnrollmentShortOutput{EnrollmentShortOutput: rols[i]}
					b := &shared.LocalEnrollmentShortOutput{EnrollmentShortOutput: rols[j]}
					return a.SortKey() < b.SortKey()
				})
				dels = append(dels, del)
				uuids = append(uuids, foundProf.UUID)
			}
			k := obj.SortKey(false)
			for index, del := range dels {
				dobj := &shared.LocalAllOutput{AllOutput: del}
				uuid := uuids[index]
				key := uuid + "==[uuid]==" + k
				log.Info(fmt.Sprintf("BulkUpdate: add profile '%s' - generated update record '%s' '%s'", obj.SortKey(true), key, dobj.SortKey(true)))
				mUpdProf[key] = [2]*models.AllOutput{prof, del}
			}
			_, ok := mAddProf[k]
			if !ok {
				err = fmt.Errorf("add profile map '%+v' doesn't have key '%s'", mAddProf, k)
				err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "BulkUpdate")
			}
			delete(mAddProf, k)
			continue
		}
		var genderAcc *int64
		if prof.Gender != nil {
			i100 := int64(100)
			genderAcc = &i100
		}
		profile := &models.ProfileDataOutput{
			Name:        prof.Name,
			Email:       prof.Email,
			Gender:      prof.Gender,
			GenderAcc:   genderAcc,
			IsBot:       prof.IsBot,
			CountryCode: prof.CountryCode,
		}
		uuid := ""
		uuid, err = s.ProfileUUIDHash(profile)
		if err != nil {
			return
		}
		profile.UUID = uuid
		log.Info(fmt.Sprintf("BulkUpdate: add profile '%s' - generated profile UUID '%s'", obj.SortKey(true), uuid))
		_, err = s.AddUniqueIdentity(&models.UniqueIdentityDataOutput{UUID: uuid}, false, tx)
		if err != nil {
			return
		}
		_, err = s.AddProfile(profile, false, tx)
		if err != nil {
			return
		}
		iid := ""
		for _, iden := range prof.Identities {
			identity := &models.IdentityDataOutput{
				UUID:     &uuid,
				Source:   iden.Source,
				Email:    iden.Email,
				Name:     iden.Name,
				Username: iden.Username,
			}
			iid, err = s.IdentityIDHash(identity)
			if err != nil {
				return
			}
			identity.ID = iid
			iobj := &shared.LocalIdentityShortOutput{IdentityShortOutput: iden}
			log.Info(fmt.Sprintf("BulkUpdate: add profile identity '%s' - generated identity ID '%s'", iobj.SortKey(), iid))
			_, err = s.AddIdentity(identity, true, false, tx)
			if err != nil {
				return
			}
		}
		var (
			start time.Time
			end   time.Time
		)
		for _, rol := range prof.Enrollments {
			start, err = s.TimeParseAny(rol.Start)
			if err != nil {
				return
			}
			end, err = s.TimeParseAny(rol.End)
			if err != nil {
				return
			}
			var (
				organization *models.OrganizationDataOutput
				ok           bool
			)
			organization, ok = mOrgName[rol.Organization]
			if !ok {
				organization, err = s.GetOrganizationByName(rol.Organization, true, tx)
				if err != nil {
					return
				}
				mOrgName[rol.Organization] = organization
				mOrgID[organization.ID] = organization
			}
			enrollment := &models.EnrollmentDataOutput{
				UUID:           uuid,
				Start:          strfmt.DateTime(start),
				End:            strfmt.DateTime(end),
				OrganizationID: organization.ID,
				ProjectSlug:    rol.ProjectSlug,
				Role:           rol.Role,
			}
			_, err = s.AddEnrollment(enrollment, false, false, tx)
			if err != nil {
				return
			}
		}
	}
	for k, data := range mUpdProf {
		prof := data[0]
		delProf := data[1]
		foundProfs := []*models.ProfileDataOutput{}
		columns := []string{}
		values := []interface{}{}
		ary := strings.Split(k, "==[uuid]==")
		if len(ary) > 1 {
			columns = append(columns, "uuid")
			values = append(values, ary[0])
		} else {
			if prof.Name != nil {
				columns = append(columns, "name")
				values = append(values, *prof.Name)
			}
			if prof.Email != nil {
				columns = append(columns, "email")
				values = append(values, *prof.Email)
			}
			if len(columns) == 0 {
				obj := &shared.LocalAllOutput{AllOutput: prof}
				err = fmt.Errorf("profile to add must have at least one profile data property set: (name, email), profile: '%s'", obj.SortKey(true))
				err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "BulkUpdate")
				return
			}
			if prof.Gender != nil {
				columns = append(columns, "gender")
				values = append(values, *prof.Gender)
			}
			if prof.IsBot != nil {
				columns = append(columns, "is_bot")
				values = append(values, *prof.IsBot)
			}
			if prof.CountryCode != nil {
				columns = append(columns, "country_code")
				values = append(values, *prof.CountryCode)
			}
		}
		foundProfs, err = s.FindProfiles(columns, values, false, tx)
		if err != nil {
			return
		}
		obj := &shared.LocalAllOutput{AllOutput: prof}
		delObj := &shared.LocalAllOutput{AllOutput: delProf}
		nFoundProfs := len(foundProfs)
		if nFoundProfs > 0 {
			log.Info(fmt.Sprintf("BulkUpdate: update profile '%s'/'%s' - found %d profiles '%+v'", obj.SortKey(true), delObj.SortKey(true), len(foundProfs), s.ToLocalProfiles(foundProfs)))
		}
		uuids := make(map[string]struct{})
		switch nFoundProfs {
		case 0:
			log.Info(fmt.Sprintf("BulkUpdate: update profile '%s'/'%s' - didn't found matching profiles, continuying", obj.SortKey(true), delObj.SortKey(true)))
		case 1:
			uuid := foundProfs[0].UUID
			_, err = s.ArchiveUUID(uuid, &archiveDate, tx)
			if err != nil {
				return
			}
			log.Info(fmt.Sprintf("BulkUpdate: update profile '%s'/'%s' - archived UUID '%s'", obj.SortKey(true), delObj.SortKey(true), uuid))
			uuids[uuid] = struct{}{}
		default:
			log.Info(fmt.Sprintf("BulkUpdate: update profile '%s'/'%s' found %d matching profiles to update", obj.SortKey(true), delObj.SortKey(true), nFoundProfs))
			for _, foundProf := range foundProfs {
				for _, iden := range prof.Identities {
					columns := []string{"uuid", "source"}
					values := []interface{}{foundProf.UUID, iden.Source}
					isDates := []bool{false, false}
					if iden.Name != nil {
						columns = append(columns, "name")
						values = append(values, *iden.Name)
						isDates = append(isDates, false)
					}
					if iden.Email != nil {
						columns = append(columns, "email")
						values = append(values, *iden.Email)
						isDates = append(isDates, false)
					}
					if iden.Username != nil {
						columns = append(columns, "username")
						values = append(values, *iden.Username)
						isDates = append(isDates, false)
					}
					var identities []*models.IdentityDataOutput
					identities, err = s.FindIdentities(columns, values, isDates, false, tx)
					if err != nil {
						return
					}
					for _, identity := range identities {
						if identity.UUID == nil {
							continue
						}
						uuids[*identity.UUID] = struct{}{}
					}
				}
				for _, rol := range prof.Enrollments {
					var (
						start        time.Time
						end          time.Time
						organization *models.OrganizationDataOutput
						enrollments  []*models.EnrollmentDataOutput
						ok           bool
					)
					start, err = s.TimeParseAny(rol.Start)
					if err != nil {
						return
					}
					end, err = s.TimeParseAny(rol.End)
					if err != nil {
						return
					}
					organization, ok = mOrgName[rol.Organization]
					if !ok {
						organization, err = s.GetOrganizationByName(rol.Organization, true, tx)
						if err != nil {
							return
						}
						mOrgName[rol.Organization] = organization
						mOrgID[organization.ID] = organization
					}
					enrollments, err = s.FindEnrollments(
						[]string{"uuid", "start", "end", "organization_id", "project_slug", "role"},
						[]interface{}{
							foundProf.UUID,
							strfmt.DateTime(start),
							strfmt.DateTime(end),
							organization.ID,
							rol.ProjectSlug,
							rol.Role,
						},
						[]bool{false, true, true, false, false, false},
						false,
						tx,
					)
					if err != nil {
						return
					}
					for _, enrollment := range enrollments {
						uuids[enrollment.UUID] = struct{}{}
					}
				}
			}
			nUUIDs := len(uuids)
			if nUUIDs == 0 {
				for _, foundProf := range foundProfs {
					uuids[foundProf.UUID] = struct{}{}
				}
			}
			nUUIDs = len(uuids)
			switch nUUIDs {
			case 0:
				log.Info(fmt.Sprintf("BulkUpdate: update profile '%s'/'%s' - detailed search didn't found matching profiles, continuying", obj.SortKey(true), delObj.SortKey(true)))
			default:
				for uuid := range uuids {
					_, err = s.ArchiveUUID(uuid, &archiveDate, tx)
					if err != nil {
						return
					}
				}
				log.Info(fmt.Sprintf("BulkUpdate: update profile '%s'/'%s' - archived UUIDs '%+v' after detailed search", obj.SortKey(true), delObj.SortKey(true), uuids))
			}
		}
		for uuid := range uuids {
			var genderAcc *int64
			if prof.Gender != nil {
				i100 := int64(100)
				genderAcc = &i100
			}
			profile := &models.ProfileDataOutput{
				UUID:        uuid,
				Name:        prof.Name,
				Email:       prof.Email,
				Gender:      prof.Gender,
				GenderAcc:   genderAcc,
				IsBot:       prof.IsBot,
				CountryCode: prof.CountryCode,
			}
			_, err = s.EditProfile(profile, false, tx)
			if err != nil {
				return
			}
			for _, iden := range delProf.Identities {
				columns := []string{"uuid", "source"}
				values := []interface{}{uuid, iden.Source}
				isDates := []bool{false, false}
				if iden.Name != nil {
					columns = append(columns, "name")
					values = append(values, *iden.Name)
					isDates = append(isDates, false)
				}
				if iden.Email != nil {
					columns = append(columns, "email")
					values = append(values, *iden.Email)
					isDates = append(isDates, false)
				}
				if iden.Username != nil {
					columns = append(columns, "username")
					values = append(values, *iden.Username)
					isDates = append(isDates, false)
				}
				var identities []*models.IdentityDataOutput
				identities, err = s.FindIdentities(columns, values, isDates, false, tx)
				if err != nil {
					return
				}
				for _, identity := range identities {
					err = s.DeleteIdentity(identity.ID, false, false, nil, tx)
					if err != nil {
						return
					}
				}
			}
			for _, rol := range delProf.Enrollments {
				var (
					start        time.Time
					end          time.Time
					organization *models.OrganizationDataOutput
					enrollments  []*models.EnrollmentDataOutput
					ok           bool
				)
				start, err = s.TimeParseAny(rol.Start)
				if err != nil {
					return
				}
				end, err = s.TimeParseAny(rol.End)
				if err != nil {
					return
				}
				organization, ok = mOrgName[rol.Organization]
				if !ok {
					organization, err = s.GetOrganizationByName(rol.Organization, true, tx)
					if err != nil {
						return
					}
					mOrgName[rol.Organization] = organization
					mOrgID[organization.ID] = organization
				}
				enrollments, err = s.FindEnrollments(
					[]string{"uuid", "start", "end", "organization_id", "project_slug", "role"},
					[]interface{}{
						uuid,
						strfmt.DateTime(start),
						strfmt.DateTime(end),
						organization.ID,
						rol.ProjectSlug,
						rol.Role,
					},
					[]bool{false, true, true, false, false, false},
					false,
					tx,
				)
				if err != nil {
					return
				}
				for _, enrollment := range enrollments {
					err = s.DeleteEnrollment(enrollment.ID, false, false, nil, tx)
					if err != nil {
						return
					}
				}
			}
			for _, iden := range prof.Identities {
				identity := &models.IdentityDataOutput{
					UUID:     &uuid,
					Source:   iden.Source,
					Email:    iden.Email,
					Name:     iden.Name,
					Username: iden.Username,
				}
				iid := ""
				iid, err = s.IdentityIDHash(identity)
				if err != nil {
					return
				}
				identity.ID = iid
				iobj := &shared.LocalIdentityShortOutput{IdentityShortOutput: iden}
				log.Info(fmt.Sprintf("BulkUpdate: update profile identity '%s' - generated identity ID '%s'", iobj.SortKey(), iid))
				_, err = s.AddIdentity(identity, true, false, tx)
				if err != nil {
					return
				}
			}
			var (
				start time.Time
				end   time.Time
			)
			for _, rol := range prof.Enrollments {
				start, err = s.TimeParseAny(rol.Start)
				if err != nil {
					return
				}
				end, err = s.TimeParseAny(rol.End)
				if err != nil {
					return
				}
				var (
					organization *models.OrganizationDataOutput
					ok           bool
				)
				organization, ok = mOrgName[rol.Organization]
				if !ok {
					organization, err = s.GetOrganizationByName(rol.Organization, true, tx)
					if err != nil {
						return
					}
					mOrgName[rol.Organization] = organization
					mOrgID[organization.ID] = organization
				}
				enrollment := &models.EnrollmentDataOutput{
					UUID:           uuid,
					Start:          strfmt.DateTime(start),
					End:            strfmt.DateTime(end),
					OrganizationID: organization.ID,
					ProjectSlug:    rol.ProjectSlug,
					Role:           rol.Role,
				}
				_, err = s.AddEnrollment(enrollment, true, false, tx)
				if err != nil {
					return
				}
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	tx = nil
	nAdded = len(mAddProf)
	nDeleted = len(mDelProf)
	nUpdated = len(mUpdProf)
	return
}

func (s *service) GetSlugMappings() error {
	rows, err := s.Query(s.rodb, nil, "select da_name, sf_name from slug_mapping")
	if err != nil {
		return err
	}
	shared.GDA2SF = make(map[string]string)
	shared.GSF2DA = make(map[string]string)
	var (
		da string
		sf string
	)
	for rows.Next() {
		err = rows.Scan(&da, &sf)
		if err != nil {
			return err
		}
		shared.GDA2SF[da] = sf
		shared.GSF2DA[sf] = da
	}
	err = rows.Err()
	if err != nil {
		return err
	}
	err = rows.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *service) FindSlugMappings(columns []string, values []interface{}, missingFatal bool, tx *sql.Tx) (mappings []*models.SlugMapping, err error) {
	log.Info(fmt.Sprintf("FindSlugMappings: columns:%+v values:%+v missingFatal:%v tx:%v", columns, values, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"FindSlugMappings(exit): columns:%+v values:%+v missingFatal:%v tx:%v mappings:%+v err:%v",
				columns,
				values,
				missingFatal,
				tx != nil,
				mappings,
				err,
			),
		)
	}()
	sdb := s.rodb
	if tx != nil {
		sdb = s.db
	}
	sel := "select da_name, sf_name, sf_id from slug_mapping"
	nColumns := len(columns)
	lastIndex := nColumns - 1
	if nColumns > 0 {
		sel += " where"
	}
	for index := range columns {
		column := columns[index]
		sel += " " + column + " = ?"
		if index < lastIndex {
			sel += " and"
		}
	}
	sel += " order by da_name asc"
	rows, err := s.Query(sdb, tx, sel, values...)
	if err != nil {
		return
	}
	for rows.Next() {
		mapping := &models.SlugMapping{}
		err = rows.Scan(
			&mapping.DaName,
			&mapping.SfName,
			&mapping.SfID,
		)
		if err != nil {
			return
		}
		mappings = append(mappings, mapping)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if missingFatal && len(mappings) == 0 {
		err = fmt.Errorf("cannot find slug mappings for %+v/%+v", columns, values)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "FindSlugMappings")
		return
	}
	return
}

func (s *service) GetListSlugMappings() (res *models.ListSlugMappings, err error) {
	log.Info("GetListSlugMappings")
	res = &models.ListSlugMappings{}
	defer func() {
		log.Info(fmt.Sprintf("GetListSlugMappings(exit): res:%+v err:%v", res, err))
	}()
	var ary []*models.SlugMapping
	ary, err = s.FindSlugMappings([]string{}, []interface{}{}, true, nil)
	if err != nil {
		return
	}
	res.Mappings = ary
	return
}

func (s *service) AddSlugMapping(inMapping *models.SlugMapping, tx *sql.Tx) (mapping *models.SlugMapping, err error) {
	log.Info(fmt.Sprintf("AddSlugMapping: inMapping:%+v tx:%v", inMapping, tx != nil))
	mapping = inMapping
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddSlugMapping(exit): inMapping:%+v tx:%v mapping:%+v err:%v",
				inMapping,
				tx != nil,
				mapping,
				err,
			),
		)
	}()
	_, err = s.Exec(
		s.db,
		tx,
		"insert into slug_mapping(da_name, sf_name, sf_id) select ?, ?, ?",
		mapping.DaName,
		mapping.SfName,
		mapping.SfID,
	)
	if err != nil {
		mapping = nil
		return
	}
	return
}

func (s *service) DeleteSlugMapping(sfID string) (status *models.TextStatusOutput, err error) {
	status = &models.TextStatusOutput{}
	log.Info(fmt.Sprintf("DeleteSlugMapping: sfID:%s", sfID))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"DeleteSlugMapping(exit): sfID:%s status:%+v err:%v",
				sfID,
				status,
				err,
			),
		)
	}()
	err = s.DropSlugMapping(sfID, true, nil)
	if err == nil {
		status.Text = fmt.Sprintf("Deleted slug mapping sfID: '%s'", sfID)
	}
	return
}

func (s *service) DropSlugMapping(sfID string, missingFatal bool, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DropSlugMapping: sfID:%s missingFatal:%v tx:%v", sfID, missingFatal, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DropSlugMapping(exit): sfID:%s missingFatal:%v tx:%v err:%v", sfID, missingFatal, tx != nil, err))
	}()
	del := "delete from slug_mapping where sf_id = ?"
	res, err := s.Exec(s.db, tx, del, sfID)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting slug mapping sfID %s had no effect", sfID)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "DropSlugMapping")
		return
	}
	return
}

func (s *service) EditSlugMapping(key, inMapping *models.SlugMapping, tx *sql.Tx) (mapping *models.SlugMapping, err error) {
	log.Info(fmt.Sprintf("EditSLugMapping: key:%+v inMapping:%+v tx:%v", key, inMapping, tx != nil))
	mapping = inMapping
	defer func() {
		log.Info(
			fmt.Sprintf(
				"EditSlugMapping(exit): inMapping:%+v tx:%v mapping:%+v err:%v",
				inMapping,
				tx != nil,
				mapping,
				err,
			),
		)
	}()
	if mapping.DaName == "" || mapping.SfName == "" || mapping.SfID == "" {
		err = fmt.Errorf("slug mapping '%+v' all columns must be set to non-empty value", mapping)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "EditSLugMapping")
		mapping = nil
		return
	}
	update := "update slug_mapping set da_name = ?, sf_name = ?, sf_id = ? where da_name = ? and sf_name = ? and sf_id = ?"
	// s.SetOrigin()
	var res sql.Result
	res, err = s.Exec(
		s.db,
		tx,
		update,
		mapping.DaName,
		mapping.SfName,
		mapping.SfID,
		key.DaName,
		key.SfName,
		key.SfID,
	)
	if err != nil {
		mapping = nil
		return
	}
	affected := int64(0)
	affected, err = res.RowsAffected()
	if err != nil {
		mapping = nil
		return
	}
	if affected == 0 {
		log.Info(fmt.Sprintf("EditSlugMapping: slug mapping '%+v' -> '%+v' update didn't affected any rows", key, mapping))
	}
	return
}
