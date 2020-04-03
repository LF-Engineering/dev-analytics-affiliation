package shdb

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"crypto/sha1"
	"database/sql"
	"encoding/hex"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"

	"github.com/go-openapi/strfmt"
	"github.com/jmoiron/sqlx"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/LF-Engineering/dev-analytics-affiliation/shared"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"

	// SortingHat database is MariaDB/MySQL format
	_ "github.com/go-sql-driver/mysql"
)

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
	AddIdentity(*models.IdentityDataOutput, bool, *sql.Tx) (*models.IdentityDataOutput, error)
	// UniqueIdentity
	TouchUniqueIdentity(string, *sql.Tx) (int64, error)
	AddUniqueIdentity(*models.UniqueIdentityDataOutput, bool, *sql.Tx) (*models.UniqueIdentityDataOutput, error)
	GetUniqueIdentity(string, bool, *sql.Tx) (*models.UniqueIdentityDataOutput, error)
	DeleteUniqueIdentity(string, bool, bool, *time.Time, *sql.Tx) error
	ArchiveUniqueIdentity(string, *time.Time, *sql.Tx) error
	UnarchiveUniqueIdentity(string, bool, *time.Time, *sql.Tx) error
	DeleteUniqueIdentityArchive(string, bool, bool, *time.Time, *sql.Tx) error
	QueryUniqueIdentitiesNested(string, int64, int64, bool, *sql.Tx) ([]*models.UniqueIdentityNestedDataOutput, int64, error)
	// Enrollment
	GetEnrollment(int64, bool, *sql.Tx) (*models.EnrollmentDataOutput, error)
	FindEnrollments([]string, []interface{}, []bool, bool, *sql.Tx) ([]*models.EnrollmentDataOutput, error)
	EditEnrollment(*models.EnrollmentDataOutput, bool, *sql.Tx) (*models.EnrollmentDataOutput, error)
	AddEnrollment(*models.EnrollmentDataOutput, bool, *sql.Tx) (*models.EnrollmentDataOutput, error)
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
	// Other
	MoveIdentityToUniqueIdentity(*models.IdentityDataOutput, *models.UniqueIdentityDataOutput, bool, *sql.Tx) error
	GetArchiveUniqueIdentityEnrollments(string, time.Time, bool, *sql.Tx) ([]*models.EnrollmentDataOutput, error)
	GetArchiveUniqueIdentityIdentities(string, time.Time, bool, *sql.Tx) ([]*models.IdentityDataOutput, error)
	GetUniqueIdentityEnrollments(string, bool, *sql.Tx) ([]*models.EnrollmentDataOutput, error)
	GetUniqueIdentityIdentities(string, bool, *sql.Tx) ([]*models.IdentityDataOutput, error)
	MoveEnrollmentToUniqueIdentity(*models.EnrollmentDataOutput, *models.UniqueIdentityDataOutput, *sql.Tx) error
	MergeEnrollments(*models.UniqueIdentityDataOutput, *models.OrganizationDataOutput, *sql.Tx) error
	MergeDateRanges([][]strfmt.DateTime) ([][]strfmt.DateTime, error)
	FindUniqueIdentityOrganizations(string, bool, *sql.Tx) ([]*models.OrganizationDataOutput, error)
	ArchiveUUID(string, *time.Time, *sql.Tx) (*time.Time, error)
	UnarchiveUUID(string, time.Time, *sql.Tx) error
	Unarchive(string, string) (bool, error)
	CheckUnaffiliated([]*models.UnaffiliatedDataOutput, *sql.Tx) ([]*models.UnaffiliatedDataOutput, error)
	EnrichContributors([]*models.ContributorStats, int64, *sql.Tx) error

	// API endpoints
	GetMatchingBlacklist(string, int64, int64) (*models.GetMatchingBlacklistOutput, error)
	PostMatchingBlacklist(string) (*models.MatchingBlacklistOutput, error)
	DeleteMatchingBlacklist(string) (*models.TextStatusOutput, error)
	DeleteOrganization(int64) (*models.TextStatusOutput, error)
	DeleteOrgDomain(string, string) (*models.TextStatusOutput, error)
	DeleteProfileNested(string, bool) (*models.TextStatusOutput, error)
	UnarchiveProfileNested(string) (*models.UniqueIdentityNestedDataOutput, error)
	GetListOrganizations(string, int64, int64) (*models.GetListOrganizationsOutput, error)
	GetListOrganizationsDomains(int64, string, int64, int64) (*models.GetListOrganizationsDomainsOutput, error)
	GetListProfiles(string, int64, int64) (*models.GetListProfilesOutput, error)
	AddNestedUniqueIdentity(string) (*models.UniqueIdentityNestedDataOutput, error)
	AddNestedIdentity(*models.IdentityDataOutput) (*models.UniqueIdentityNestedDataOutput, error)
	FindEnrollmentsNested([]string, []interface{}, []bool, bool, *sql.Tx) ([]*models.EnrollmentNestedDataOutput, error)
	WithdrawEnrollment(*models.EnrollmentDataOutput, bool, *sql.Tx) error
	PutOrgDomain(string, string, bool, bool, bool) (*models.PutOrgDomainOutput, error)
	MergeUniqueIdentities(string, string, bool) error
	MoveIdentity(string, string, bool) error
	GetAllAffiliations() (*models.AllArrayOutput, error)
}

type service struct {
	shared.ServiceStruct
	db *sqlx.DB
}

// New creates new db service instance with given db
func New(db *sqlx.DB) Service {
	return &service{
		db: db,
	}
}

// DateTimeFormat - this is how we format datetime for MariaDB
const (
	DateTimeFormat = "%Y-%m-%dT%H:%i:%s.%fZ"
)

var (
	// MinPeriodDate - default start data for enrollments
	MinPeriodDate = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	// MaxPeriodDate - default end date for enrollments
	MaxPeriodDate = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	// DateFormat - format date as YYYY-MM-DD
	DateFormat = "2006-01-02"
)

func (s *service) GetCountry(countryCode string, tx *sql.Tx) (countryData *models.CountryDataOutput, err error) {
	log.Info(fmt.Sprintf("GetCountry: countryCode:%s tx:%v", countryCode, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("GetCountry(exit): countryCode:%s tx:%v countryData:%+v err:%v", countryCode, tx != nil, countryData, err))
	}()
	countryData = &models.CountryDataOutput{}
	rows, err := s.Query(
		s.db,
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
		if time.Time(st).Before(MinPeriodDate) || time.Time(st).After(MaxPeriodDate) {
			err = fmt.Errorf("start date %v must be between %v and %v", st, MinPeriodDate, MaxPeriodDate)
			return
		}
		if time.Time(en).Before(MinPeriodDate) || time.Time(en).After(MaxPeriodDate) {
			err = fmt.Errorf("end date %v must be between %v and %v", en, MinPeriodDate, MaxPeriodDate)
			return
		}
		// st <= saved[1]
		if !time.Time(st).After(time.Time(saved[1])) {
			// saved[0] == MIN_PERIOD_DATE
			if !time.Time(saved[0]).After(MinPeriodDate) {
				saved[0] = st
				minRange = true
			}
			// if MAX_PERIOD_DATE in (en, saved[1]):
			if !time.Time(saved[1]).Before(MaxPeriodDate) || !time.Time(en).Before(MaxPeriodDate) {
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
		mergedDates[0][0] = strfmt.DateTime(MinPeriodDate)
	}
	if maxRange {
		mergedDates[len(mergedDates)-1][1] = strfmt.DateTime(MaxPeriodDate)
	}
	return
}

func (s *service) MergeEnrollments(uniqueIdentity *models.UniqueIdentityDataOutput, organization *models.OrganizationDataOutput, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("MergeEnrollments: uniqueIdentity:%+v organization:%+v tx:%v", s.ToLocalUniqueIdentity(uniqueIdentity), organization, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("MergeEnrollments(exit): uniqueIdentity:%+v organization:%+v tx:%v err:%v", s.ToLocalUniqueIdentity(uniqueIdentity), organization, tx != nil, err))
	}()
	disjoint, err := s.FindEnrollments([]string{"uuid", "organization_id"}, []interface{}{uniqueIdentity.UUID, organization.ID}, []bool{false, false}, false, tx)
	if err != nil {
		return
	}
	if len(disjoint) == 0 {
		err = fmt.Errorf("merge enrollments unique identity '%+v' organization '%+v' found no enrollments", s.ToLocalUniqueIdentity(uniqueIdentity), organization)
		return
	}
	dates := [][]strfmt.DateTime{}
	for _, rol := range disjoint {
		dates = append(dates, []strfmt.DateTime{rol.Start, rol.End})
	}
	mergedDates, err := s.MergeDateRanges(dates)
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
		newEnrollment := &models.EnrollmentDataOutput{UUID: uniqueIdentity.UUID, OrganizationID: organization.ID, Start: st, End: en}
		_, err = s.AddEnrollment(newEnrollment, false, tx)
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
		return
	}
	affected, err = s.TouchUniqueIdentity(uniqueIdentity.UUID, tx)
	if err != nil {
		return
	}
	if affected != 1 {
		err = fmt.Errorf("'%+v' unique identity update affected %d rows", s.ToLocalUniqueIdentity(uniqueIdentity), affected)
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
			return
		}
		affected, err = s.TouchUniqueIdentity(uniqueIdentity.UUID, tx)
		if err != nil {
			return
		}
		if affected != 1 {
			err = fmt.Errorf("'%+v' unique identity update affected %d rows", s.ToLocalUniqueIdentity(uniqueIdentity), affected)
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

func (s *service) EnrichContributors(contributors []*models.ContributorStats, millisSinceEpoch int64, tx *sql.Tx) (err error) {
	inf := ""
	n := len(contributors)
	if n > shared.LogListMax {
		inf = fmt.Sprintf("%d", n)
	} else {
		inf = fmt.Sprintf("%+v", s.ToLocalTopContributors(contributors))
	}
	found := 0
	orgFound := 0
	log.Info(fmt.Sprintf("EnrichContributors: contributors:%s millisSinceEpoch:%d tx:%v", inf, millisSinceEpoch, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"EnrichContributors(exit): contributors:%s millisSinceEpoch:%d tx:%v found:%d/%d/%d err:%v",
				inf,
				millisSinceEpoch,
				tx != nil,
				orgFound,
				found,
				n,
				err,
			),
		)
	}()
	secsSinceEpoch := float64(millisSinceEpoch) / 1000.0
	sel := "select p.uuid, coalesce(p.name, ''), coalesce(p.email, ''), coalesce(o.name, '') from profiles p left join enrollments e"
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
	sel = sel[0:len(sel)-1] + ")"
	var rows *sql.Rows
	rows, err = s.Query(s.db, tx, sel, uuids...)
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
		data[uuid] = [3]string{name, email, org}
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
	return
}

func (s *service) CheckUnaffiliated(inUnaffiliated []*models.UnaffiliatedDataOutput, tx *sql.Tx) (unaffiliated []*models.UnaffiliatedDataOutput, err error) {
	inunaff := ""
	nUnaffiliated := len(inUnaffiliated)
	if nUnaffiliated > shared.LogListMax {
		inunaff = fmt.Sprintf("%d", nUnaffiliated)
	} else {
		inunaff = fmt.Sprintf("%+v", s.ToLocalUnaffiliated(inUnaffiliated))
	}
	log.Info(fmt.Sprintf("CheckUnaffiliated: inUnaffiliated:%s tx:%v", inunaff, tx != nil))
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
				"CheckUnaffiliated(exit): inUnaffiliated:%+v tx:%v unaffiliated:%+v err:%v",
				inunaff,
				tx != nil,
				unaff,
				err,
			),
		)
	}()
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
	sel = sel[0:len(sel)-1] + ")"
	var rows *sql.Rows
	rows, err = s.Query(s.db, tx, sel, uuids...)
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
	if uniqueIdentity.LastModified == nil {
		uniqueIdentity.LastModified = s.Now()
	}
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
	sel := "select distinct o.id, o.name from organizations o, enrollments e where e.organization_id = o.id and e.uuid = ? order by o.name asc"
	rows, err := s.Query(s.db, tx, sel, uuid)
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
	rows, err := s.Query(s.db, tx, sel, values...)
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
		return
	}
	return
}
func (s *service) FindEnrollmentsNested(columns []string, values []interface{}, isDate []bool, missingFatal bool, tx *sql.Tx) (enrollments []*models.EnrollmentNestedDataOutput, err error) {
	log.Info(fmt.Sprintf("FindEnrollmentsNested: columns:%+v values:%+v isDate:%+v missingFatal:%v tx:%v", columns, values, isDate, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"FindEnrollmentsNested(exit): columns:%+v values:%+v isDate:%+v missingFatal:%v tx:%v enrollments:%+v err:%v",
				columns,
				values,
				isDate,
				missingFatal,
				tx != nil,
				s.ToLocalNestedEnrollments(enrollments),
				err,
			),
		)
	}()
	sel := "select e.id, e.uuid, e.start, e.end, o.id, o.name from enrollments e, organizations o where e.organization_id = o.id"
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
		if date {
			sel += " " + column + " = str_to_date(?, ?)"
			vals = append(vals, value)
			vals = append(vals, DateTimeFormat)
		} else {
			sel += " " + column + " = ?"
			vals = append(vals, value)
		}
		if index < lastIndex {
			sel += " and"
		}
	}
	sel += " order by e.uuid asc, e.start asc, e.end asc"
	rows, err := s.Query(s.db, tx, sel, vals...)
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
	sel := "select id, uuid, organization_id, start, end from enrollments"
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
		if date {
			sel += " " + column + " = str_to_date(?, ?)"
			vals = append(vals, value)
			vals = append(vals, DateTimeFormat)
		} else {
			sel += " " + column + " = ?"
			vals = append(vals, value)
		}
		if index < lastIndex {
			sel += " and"
		}
	}
	sel += " order by uuid asc, start asc, end asc"
	rows, err := s.Query(s.db, tx, sel, vals...)
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
	rows, err := s.Query(
		s.db,
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
	rows, err := s.Query(
		s.db,
		tx,
		"select id, uuid, organization_id, start, end from enrollments_archive where uuid = ? and archived_at = ? order by start asc, end asc",
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
	rows, err := s.Query(
		s.db,
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
	rows, err := s.Query(
		s.db,
		tx,
		"select id, uuid, organization_id, start, end from enrollments where uuid = ? order by start asc, end asc",
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
	enrollmentData = &models.EnrollmentDataOutput{}
	rows, err := s.Query(
		s.db,
		tx,
		"select id, uuid, organization_id, start, end from enrollments where id = ? limit 1",
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
	rows, err := s.Query(
		s.db,
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
	organizationData = &models.OrganizationDataOutput{}
	rows, err := s.Query(
		s.db,
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
	organizationData = &models.OrganizationDataOutput{}
	rows, err := s.Query(
		s.db,
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
	uniqueIdentityData = &models.UniqueIdentityDataOutput{}
	rows, err := s.Query(
		s.db,
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
	identityData = &models.IdentityDataOutput{}
	rows, err := s.Query(
		s.db,
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
	profileData = &models.ProfileDataOutput{}
	rows, err := s.Query(
		s.db,
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
		return
	}
	return
}

func (s *service) UnarchiveUniqueIdentity(uuid string, replace bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("UnarchiveUniqueIdentity: uuid:%s replace:%v tm:%v tx:%v", uuid, replace, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("UnarchiveUniqueIdentity(exit): uuid:%s replace:%v tm:%v tx:%v err:%v", uuid, replace, tm, tx != nil, err))
	}()
	if replace {
		err = s.DeleteUniqueIdentity(uuid, false, false, nil, tx)
		if err != nil {
			return
		}
	}
	var res sql.Result
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
	if tm != nil {
		insert := "insert into enrollments(id, uuid, organization_id, start, end) " +
			"select id, uuid, organization_id, start, end from enrollments_archive " +
			"where id = ? and archived_at = ?"
		res, err = s.Exec(s.db, tx, insert, id, tm)
	} else {
		insert := "insert into enrollments(id, uuid, organization_id, start, end) " +
			"select id, uuid, organization_id, start, end from enrollments_archive " +
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
	insert := "insert into enrollments_archive(id, uuid, organization_id, start, end, archived_at) " +
		"select id, uuid, organization_id, start, end, ? from enrollments where id = ? limit 1"
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
		return
	}
	return
}

func (s *service) WithdrawEnrollment(enrollment *models.EnrollmentDataOutput, missingFatal bool, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("WithdrawEnrollment: enrollment:%+v missingFatal:%v tx:%v", enrollment, missingFatal, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("WithdrawEnrollment(exit): enrollment:%+v missingFatal:%v tx:%v err:%v", enrollment, missingFatal, tx != nil, err))
	}()
	del := "delete from enrollments where uuid = ? and organization_id = ? and start >= str_to_date(?, ?) and end <= str_to_date(?, ?)"
	res, err := s.Exec(
		s.db,
		tx,
		del,
		enrollment.UUID,
		enrollment.OrganizationID,
		enrollment.Start,
		DateTimeFormat,
		enrollment.End,
		DateTimeFormat,
	)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting enrollment id '%+v' had no effect", enrollment)
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
		return
	}
	return
}

func (s *service) UnarchiveProfile(uuid string, replace bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("UnarchiveProfile: uuid:%s replace:%v tm:%v tx:%v", uuid, replace, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("UnarchiveProfile(exit): uuid:%s replace:%v tm:%v tx:%v err:%v", uuid, replace, tm, tx != nil, err))
	}()
	if replace {
		err = s.DeleteProfile(uuid, false, false, nil, tx)
		if err != nil {
			return
		}
	}
	var res sql.Result
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
		profileData = nil
		return
	}
	if profileData.IsBot != nil && (*profileData.IsBot != 0 && *profileData.IsBot != 1) {
		err = fmt.Errorf("profile '%+v' is_bot should be '0' or '1'", s.ToLocalProfile(profileData))
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
			return
		}
		if profileData.GenderAcc != nil && (*profileData.GenderAcc < 1 || *profileData.GenderAcc > 100) {
			err = fmt.Errorf("profile '%+v' gender_acc should be within [1, 100]", s.ToLocalProfile(profileData))
			return
		}
	}
	if profileData.Gender == nil && profileData.GenderAcc != nil {
		err = fmt.Errorf("profile '%+v' gender_acc can only be set when gender is given", s.ToLocalProfile(profileData))
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
		return
	}
	if organizationData.Name == "" {
		err = fmt.Errorf("organization '%+v' missing name", organizationData)
		return
	}
	return
}

func (s *service) ValidateEnrollment(enrollmentData *models.EnrollmentDataOutput, forUpdate bool) (err error) {
	log.Info(fmt.Sprintf("ValidateEnrollment: enrollmentData:%+v forUpdate:%v", enrollmentData, forUpdate))
	defer func() {
		log.Info(fmt.Sprintf("ValidateEnrollment(exit): enrollmentData:%+v forUpdate:%v err:%v", enrollmentData, forUpdate, err))
	}()
	if forUpdate && enrollmentData.ID < 1 {
		err = fmt.Errorf("enrollment '%+v' missing id", enrollmentData)
		return
	}
	if enrollmentData.UUID == "" || enrollmentData.OrganizationID < 1 {
		err = fmt.Errorf("enrollment '%+v' missing uuid or organization_id", enrollmentData)
		return
	}
	if time.Time(enrollmentData.Start).Before(MinPeriodDate) || time.Time(enrollmentData.Start).After(MaxPeriodDate) {
		err = fmt.Errorf("enrollment '%+v' start date must be between %v and %v", enrollmentData, MinPeriodDate, MaxPeriodDate)
		return
	}
	if time.Time(enrollmentData.End).Before(MinPeriodDate) || time.Time(enrollmentData.End).After(MaxPeriodDate) {
		err = fmt.Errorf("enrollment '%+v' end date must be between %v and %v", enrollmentData, MinPeriodDate, MaxPeriodDate)
		return
	}
	if time.Time(enrollmentData.End).Before(time.Time(enrollmentData.Start)) {
		err = fmt.Errorf("enrollment '%+v' end date must be after start date", enrollmentData)
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
		return
	}
	if !forUpdate {
		if identityData.Source == "" {
			err = fmt.Errorf("identity '%+v' missing source", s.ToLocalIdentity(identityData))
			return
		}
		if (identityData.Name == nil || (identityData.Name != nil && *(identityData.Name) == "")) &&
			(identityData.Email == nil || (identityData.Email != nil && *(identityData.Email) == "")) &&
			(identityData.Username == nil || (identityData.Username != nil && *(identityData.Username) == "")) {
			err = fmt.Errorf("identity '%+v' you need to set at leats one of (name, email, username)", s.ToLocalIdentity(identityData))
			return
		}
		return
	}
	return
}

func (s *service) AddNestedIdentity(identity *models.IdentityDataOutput) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	log.Info(fmt.Sprintf("AddNestedIdentity: identity:%+v", s.ToLocalIdentity(identity)))
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
	arg := stripF(identity.Source) + ":" + stripF(email) + ":" + stripF(name) + ":" + stripF(username)
	hash := sha1.New()
	_, err = hash.Write([]byte(arg))
	if err != nil {
		uid = nil
		return
	}
	identity.ID = hex.EncodeToString(hash.Sum(nil))
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
	_, err = s.AddIdentity(identity, false, tx)
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

func (s *service) AddIdentity(inIdentityData *models.IdentityDataOutput, refresh bool, tx *sql.Tx) (identityData *models.IdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("AddIdentity: inIdentityData:%+v refresh:%v tx:%v", s.ToLocalIdentity(inIdentityData), refresh, tx != nil))
	identityData = inIdentityData
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddIdentity(exit): inIdentityData:%+v refresh:%v tx:%v identityData:%+v err:%v",
				s.ToLocalIdentity(inIdentityData),
				refresh,
				tx != nil,
				s.ToLocalIdentity(identityData),
				err,
			),
		)
	}()
	if identityData.LastModified == nil {
		identityData.LastModified = s.Now()
	}
	err = s.ValidateIdentity(identityData, false)
	if err != nil {
		identityData = nil
		return
	}
	insert := "insert into identities(id, uuid, source, name, email, username, last_modified) select ?, ?, ?, ?, ?, ?, str_to_date(?, ?)"
	var res sql.Result
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
				identityData = nil
				return
			}
		}
	} else {
		err = fmt.Errorf("adding identity '%+v' didn't affected any rows", s.ToLocalIdentity(identityData))
		identityData = nil
		return
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
	rows, err := s.Query(s.db, tx, sel, vals...)
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
		return
	}
	return
}

func (s *service) AddNestedUniqueIdentity(uuid string) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	log.Info(fmt.Sprintf("AddNestedUniqueIdentity: uuid:%s", uuid))
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
	err = s.ValidateProfile(profileData, tx)
	if err != nil {
		profileData = nil
		return
	}
	insert := "insert into profiles(uuid, name, email, gender, gender_acc, is_bot, country_code) select ?, ?, ?, ?, ?, ?, ?"
	var res sql.Result
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
			profileData = nil
			return
		}
	} else {
		err = fmt.Errorf("adding profile '%+v' didn't affected any rows", s.ToLocalProfile(profileData))
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

func (s *service) AddEnrollment(inEnrollmentData *models.EnrollmentDataOutput, refresh bool, tx *sql.Tx) (enrollmentData *models.EnrollmentDataOutput, err error) {
	log.Info(fmt.Sprintf("AddEnrollment: inEnrollmentData:%+v refresh:%v tx:%v", inEnrollmentData, refresh, tx != nil))
	enrollmentData = inEnrollmentData
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddEnrollment(exit): inEnrollmentData:%+v refresh:%v tx:%v enrollmentData:%+v err:%v",
				inEnrollmentData,
				refresh,
				tx != nil,
				enrollmentData,
				err,
			),
		)
	}()
	err = s.ValidateEnrollment(enrollmentData, false)
	if err != nil {
		enrollmentData = nil
		return
	}
	insert := "insert into enrollments(uuid, organization_id, start, end) select ?, ?, str_to_date(?, ?), str_to_date(?, ?)"
	var res sql.Result
	res, err = s.Exec(
		s.db,
		tx,
		insert,
		enrollmentData.UUID,
		enrollmentData.OrganizationID,
		enrollmentData.Start,
		DateTimeFormat,
		enrollmentData.End,
		DateTimeFormat,
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
			enrollmentData = nil
			return
		}
	} else {
		err = fmt.Errorf("adding enrollment '%+v' didn't affected any rows", enrollmentData)
		enrollmentData = nil
		return
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
	err = s.ValidateOrganization(organizationData, true)
	if err != nil {
		err = fmt.Errorf("organization '%+v' didn't pass update validation", organizationData)
		organizationData = nil
		return
	}
	update := "update organizations set name = ? where id = ?"
	var res sql.Result
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
	err = s.ValidateEnrollment(enrollmentData, true)
	if err != nil {
		err = fmt.Errorf("enrollment '%+v' didn't pass update validation", enrollmentData)
		enrollmentData = nil
		return
	}
	update := "update enrollments set uuid = ?, organization_id = ?, start = str_to-date(?, ?), end = str_to_date(?, ?) where id = ?"
	var res sql.Result
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
	if identityData.ID == "" || identityData.Source == "" {
		err = fmt.Errorf("identity '%+v' missing id or source", s.ToLocalIdentity(identityData))
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
			return
		}
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
	err = s.ArchiveProfile(uuid, tm, tx)
	if err != nil {
		return
	}
	identities, err := s.GetUniqueIdentityIdentities(uuid, false, tx)
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

func (s *service) MergeUniqueIdentities(fromUUID, toUUID string, archive bool) (err error) {
	log.Info(fmt.Sprintf("MergeUniqueIdentities: fromUUID:%s toUUID:%s archive:%v", fromUUID, toUUID, archive))
	defer func() {
		log.Info(fmt.Sprintf("MergeUniqueIdentities(exit): fromUUID:%s toUUID:%s archive:%v err:%v", fromUUID, toUUID, archive, err))
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
			[]string{"uuid", "organization_id", "start", "end"},
			[]interface{}{toUUID, rol.OrganizationID, rol.Start, rol.End},
			[]bool{false, false, true, true},
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
		err = s.MergeEnrollments(toUU, org, tx)
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
				UUID: toUUID,
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
	qrows, err = s.Query(s.db, tx, selRoot+sel, args...)
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
	qrows, err = s.Query(s.db, tx, "select count(*)"+sel, args...)
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
	log.Info("GetAllAffiliations")
	defer func() {
		log.Info(fmt.Sprintf("GetAllAffiliations(exit): all:%d err:%v", len(all.Profiles), err))
	}()
	sel := "select distinct s.uuid, s.name, s.email, s.gender, s.is_bot, s.country_code, "
	sel += "i.id, i.name, i.email, i.username, i.source, s.id, s.start, s.end, s.oname "
	sel += "from (select distinct u.uuid, p.name, p.email, p.gender, p.is_bot, p.country_code, "
	sel += "e.id, e.start, e.end, o.name as oname from uidentities u, profiles p "
	sel += "left join enrollments e on e.uuid = p.uuid left join organizations o on o.id = e.organization_id "
	sel += "where u.uuid = p.uuid) s left join identities i on s.uuid = i.uuid"
	var rows *sql.Rows
	rows, err = s.Query(s.db, nil, sel)
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
			&rolID, &rolStart, &rolEnd, &rolOrganization,
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
		if prof.Email != nil {
			tmp := strings.Replace(strings.TrimSpace(*prof.Email), "@", "!", -1)
			prof.Email = &tmp
		}
		if iName != nil {
			tmp := strings.TrimSpace(*iName)
			iName = &tmp
		}
		if iEmail != nil {
			tmp := strings.Replace(strings.TrimSpace(*iEmail), "@", "!", -1)
			iEmail = &tmp
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
				Start:        time.Time(*rolStart).Format(DateFormat),
				End:          time.Time(*rolEnd).Format(DateFormat),
				Organization: *rolOrganization,
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
		return a.SortKey() < b.SortKey()
	})
	return
}

func (s *service) QueryUniqueIdentitiesNested(q string, rows, page int64, identityRequired bool, tx *sql.Tx) (uids []*models.UniqueIdentityNestedDataOutput, nRows int64, err error) {
	log.Info(fmt.Sprintf("QueryUniqueIdentitiesNested: q:%s rows:%d page:%d identityRequired:%v tx:%v", q, rows, page, identityRequired, tx != nil))
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
				"QueryUniqueIdentitiesNested(exit): q:%s rows:%d page:%d identityRequired:%v tx:%v uids:%s n_rows:%d err:%v",
				q,
				rows,
				page,
				identityRequired,
				tx != nil,
				list,
				nRows,
				err,
			),
		)
	}()
	args := []interface{}{}
	sel := ""
	where := ""
	qWhere := ""
	if q != "" {
		q = strings.TrimSpace(q)
		if strings.HasPrefix(q, "uuid=") {
			qLike := "%" + q[5:] + "%"
			qWhere += "and u.uuid like ?"
			args = []interface{}{qLike}
		} else {
			qLike := "%" + q + "%"
			qWhere += "and (i.name like ? or i.email like ? or i.username like ? or i.source like ?)"
			args = []interface{}{qLike, qLike, qLike, qLike}
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
	qrows, err = s.Query(s.db, tx, query, args...)
	if err != nil {
		return
	}
	uuids := []interface{}{}
	uuid := ""
	if identityRequired {
		sel = "select distinct u.uuid, u.last_modified, p.name, p.email, p.gender, p.gender_acc, p.is_bot, p.country_code, "
		sel += "i.id, i.name, i.email, i.username, i.source, i.last_modified, e.id, e.start, e.end, e.organization_id, o.name "
		sel += "from uidentities u, identities i, profiles p "
		sel += "left join enrollments e on e.uuid = p.uuid left join organizations o on o.id = e.organization_id "
		sel += "where u.uuid = i.uuid and u.uuid = p.uuid and i.uuid = p.uuid and u.uuid in ("
	} else {
		sel = "select distinct s.uuid, s.last_modified, s.name, s.email, s.gender, s.gender_acc, s.is_bot, s.country_code, "
		sel += "i.id, i.name, i.email, i.username, i.source, i.last_modified, s.id, s.start, s.end, s.organization_id, s.oname "
		sel += "from (select distinct u.uuid, u.last_modified, p.name, p.email, p.gender, p.gender_acc, p.is_bot, p.country_code, "
		sel += "e.id, e.start, e.end, e.organization_id, o.name as oname from uidentities u, profiles p "
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
	qrows, err = s.Query(s.db, tx, sel, uuids...)
	if err != nil {
		return
	}
	var (
		rolID             *int64
		rolStart          *strfmt.DateTime
		rolEnd            *strfmt.DateTime
		rolOrganizationID *int64
		rolOrganization   *string
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
	for qrows.Next() {
		uid := &models.UniqueIdentityNestedDataOutput{}
		prof := &models.ProfileDataOutput{}
		id := &models.IdentityDataOutput{}
		rol := &models.EnrollmentNestedDataOutput{}
		err = qrows.Scan(
			&uid.UUID, &uid.LastModified,
			&prof.Name, &prof.Email, &prof.Gender, &prof.GenderAcc, &prof.IsBot, &prof.CountryCode,
			&iID, &iName, &iEmail, &iUsername, &iSource, &iLastModified,
			&rolID, &rolStart, &rolEnd, &rolOrganizationID, &rolOrganization,
		)
		if err != nil {
			return
		}
		uuid := uid.UUID
		prof.UUID = uuid
		if rolID != nil && rolOrganization != nil {
			rol = &models.EnrollmentNestedDataOutput{
				ID:             *rolID,
				UUID:           uuid,
				Start:          *rolStart,
				End:            *rolEnd,
				OrganizationID: *rolOrganizationID,
				Organization: &models.OrganizationDataOutput{
					ID:   *rolOrganizationID,
					Name: *rolOrganization,
				},
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
		if rolID != nil {
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
	qrows, err = s.Query(s.db, tx, query, args...)
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
	qLike := ""
	sel := "select id from organizations"
	if q != "" {
		q = strings.TrimSpace(q)
		qLike = "%" + q + "%"
		sel += " where name like ?"
	}
	sel += " order by name"
	sel += fmt.Sprintf(" limit %d offset %d", rows, (page-1)*rows)
	var qrows *sql.Rows
	if q == "" {
		qrows, err = s.Query(s.db, tx, sel)
	} else {
		qrows, err = s.Query(s.db, tx, sel, qLike)
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
	qrows, err = s.Query(s.db, tx, sel, oids...)
	if err != nil {
		return
	}
	orgsMap := make(map[string]*models.OrganizationNestedDataOutput)
	for qrows.Next() {
		err = qrows.Scan(&oid, &oName, &doid, &domainName, &isTopDomain)
		if err != nil {
			return
		}
		org, ok := orgsMap[oName]
		if !ok {
			orgsMap[oName] = &models.OrganizationNestedDataOutput{ID: oid, Name: oName, Domains: []*models.DomainDataOutput{}}
		}
		if doid != nil {
			org = orgsMap[oName]
			org.Domains = append(org.Domains, &models.DomainDataOutput{ID: *doid, Name: *domainName, IsTopDomain: *isTopDomain, OrganizationID: oid, OrganizationName: oName})
			orgsMap[oName] = org
		}
	}
	oNames := []string{}
	for oName := range orgsMap {
		oNames = append(oNames, oName)
	}
	sort.Strings(oNames)
	for _, oName := range oNames {
		orgs = append(orgs, orgsMap[oName])
	}
	err = qrows.Err()
	if err != nil {
		return
	}
	err = qrows.Close()
	if err != nil {
		return
	}
	sel = "select count(*) from organizations"
	if q != "" {
		q = strings.TrimSpace(q)
		sel += " where name like ?"
	}
	if q == "" {
		qrows, err = s.Query(s.db, tx, sel)
	} else {
		qrows, err = s.Query(s.db, tx, sel, qLike)
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
		qrows, err = s.Query(s.db, tx, sel)
	} else {
		qrows, err = s.Query(s.db, tx, sel, qLike)
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
		qrows, err = s.Query(s.db, tx, sel)
	} else {
		qrows, err = s.Query(s.db, tx, sel, qLike)
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

func (s *service) UnarchiveProfileNested(uuid string) (uid *models.UniqueIdentityNestedDataOutput, err error) {
	uid = &models.UniqueIdentityNestedDataOutput{}
	log.Info(fmt.Sprintf("UnarchiveProfileNested: uuid:%s", uuid))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"UnarchiveProfileNested(exit): uuid:%s uid:%+v err:%v",
				uuid,
				s.ToLocalNestedUniqueIdentity(uid),
				err,
			),
		)
	}()
	var rows *sql.Rows
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
	ary, _, err = s.QueryUniqueIdentitiesNested("uuid="+uuid, 1, 1, false, tx)
	if err != nil {
		return
	}
	if len(ary) == 0 {
		err = fmt.Errorf("Unarchived profile with UUID '%s' not found", uuid)
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

func (s *service) GetListProfiles(q string, rows, page int64) (getListProfiles *models.GetListProfilesOutput, err error) {
	log.Info(fmt.Sprintf("GetListProfiles: q:%s rows:%d page:%d", q, rows, page))
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
				"GetListProfiles(exit): q:%s rows:%d page:%d getListProfiles:%s err:%v",
				q,
				rows,
				page,
				list,
				err,
			),
		)
	}()
	nRows := int64(0)
	var ary []*models.UniqueIdentityNestedDataOutput
	ary, nRows, err = s.QueryUniqueIdentitiesNested(q, rows, page, true, nil)
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
	putOrgDomain = &models.PutOrgDomainOutput{}
	org = strings.TrimSpace(org)
	dom = strings.TrimSpace(dom)
	defer func() {
		log.Info(fmt.Sprintf("PutOrgDomain(exit): org:%s dom:%s overwrite:%v isTopDomain:%v skipEnrollments:%v putOrgDomain:%+v err:%v", org, dom, overwrite, isTopDomain, skipEnrollments, putOrgDomain, err))
	}()
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
