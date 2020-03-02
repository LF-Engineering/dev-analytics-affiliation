package shdb

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	"database/sql"

	"github.com/go-openapi/strfmt"
	"github.com/jmoiron/sqlx"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"

	// SortingHat database is MariaDB/MySQL format
	_ "github.com/go-sql-driver/mysql"
)

// Service - access affiliations MariaDB interface
type Service interface {
	// External CRUD methods
	// Country
	GetCountry(string, *sql.Tx) (*models.CountryDataOutput, error)
	// Profile
	GetProfile(string, bool, *sql.Tx) (*models.ProfileDataOutput, error)
	EditProfile(*models.ProfileDataOutput, bool, *sql.Tx) (*models.ProfileDataOutput, error)
	DeleteProfile(string, bool, bool, *time.Time, *sql.Tx) error
	ArchiveProfile(string, *time.Time, *sql.Tx) error
	UnarchiveProfile(string, bool, *time.Time, *sql.Tx) error
	DeleteProfileArchive(string, bool, bool, *time.Time, *sql.Tx) error
	// Identity
	TouchIdentity(string, *sql.Tx) (int64, error)
	GetIdentity(string, bool, *sql.Tx) (*models.IdentityDataOutput, error)
	EditIdentity(*models.IdentityDataOutput, bool, *sql.Tx) (*models.IdentityDataOutput, error)
	DeleteIdentity(int64, bool, bool, *time.Time, *sql.Tx) error
	ArchiveIdentity(int64, *time.Time, *sql.Tx) error
	UnarchiveIdentity(int64, bool, *time.Time, *sql.Tx) error
	DeleteIdentityArchive(int64, bool, bool, *time.Time, *sql.Tx) error
	// UniqueIdentity
	TouchUniqueIdentity(string, *sql.Tx) (int64, error)
	AddUniqueIdentity(*models.UniqueIdentityDataOutput, bool, *sql.Tx) (*models.UniqueIdentityDataOutput, error)
	GetUniqueIdentity(string, bool, *sql.Tx) (*models.UniqueIdentityDataOutput, error)
	DeleteUniqueIdentity(string, bool, bool, *time.Time, *sql.Tx) error
	ArchiveUniqueIdentity(string, *time.Time, *sql.Tx) error
	UnarchiveUniqueIdentity(string, bool, *time.Time, *sql.Tx) error
	DeleteUniqueIdentityArchive(string, bool, bool, *time.Time, *sql.Tx) error
	// Enrollment
	GetEnrollment(int64, bool, *sql.Tx) (*models.EnrollmentDataOutput, error)
	FindEnrollments([]string, []interface{}, []bool, bool, *sql.Tx) ([]*models.EnrollmentDataOutput, error)
	EditEnrollment(*models.EnrollmentDataOutput, bool, *sql.Tx) (*models.EnrollmentDataOutput, error)
	AddEnrollment(*models.EnrollmentDataOutput, bool, *sql.Tx) (*models.EnrollmentDataOutput, error)
	DeleteEnrollment(int64, bool, bool, *time.Time, *sql.Tx) error
	ArchiveEnrollment(int64, *time.Time, *sql.Tx) error
	UnarchiveEnrollment(int64, bool, *time.Time, *sql.Tx) error
	DeleteEnrollmentArchive(int64, bool, bool, *time.Time, *sql.Tx) error
	ValidateEnrollment(*models.EnrollmentDataOutput, bool, *sql.Tx) error
	// Organization
	FindOrganizations([]string, []interface{}, bool, *sql.Tx) ([]*models.OrganizationDataOutput, error)
	// Other
	MoveIdentityToUniqueIdentity(*models.IdentityDataOutput, *models.UniqueIdentityDataOutput, *sql.Tx) error
	GetIdentityUniqueIdentities(*models.UniqueIdentityDataOutput, bool, *sql.Tx) ([]*models.IdentityDataOutput, error)
	GetUniqueIdentityEnrollments(string, bool, *sql.Tx) ([]*models.EnrollmentDataOutput, error)
	MoveEnrollmentToUniqueIdentity(*models.EnrollmentDataOutput, *models.UniqueIdentityDataOutput, *sql.Tx) error
	MergeEnrollments(string, *models.OrganizationDataOutput, *sql.Tx) error
	MergeDateRanges([][]strfmt.DateTime) ([][]strfmt.DateTime, error)
	FindUniqueIdentityOrganizations(string, bool, *sql.Tx) ([]*models.OrganizationDataOutput, error)
	ArchiveUUID(string, *sql.Tx) (time.Time, error)

	// API endpoints
	MergeUniqueIdentities(string, string) error
	MoveIdentity(string, string) error
	PutOrgDomain(string, string, bool, bool) (*models.PutOrgDomainOutput, error)

	// Internal methods
	now() *strfmt.DateTime
	toLocalProfile(*models.ProfileDataOutput) *localProfile
	toLocalIdentity(*models.IdentityDataOutput) *localIdentity
	toLocalUniqueIdentity(*models.UniqueIdentityDataOutput) *localUniqueIdentity
	toLocalOrganizations([]*models.OrganizationDataOutput) []interface{}
	toLocalEnrollments([]*models.EnrollmentDataOutput) []interface{}
	queryOut(string, ...interface{})
	queryDB(*sqlx.DB, string, ...interface{}) (*sql.Rows, error)
	queryTX(*sql.Tx, string, ...interface{}) (*sql.Rows, error)
	query(*sqlx.DB, *sql.Tx, string, ...interface{}) (*sql.Rows, error)
	execDB(*sqlx.DB, string, ...interface{}) (sql.Result, error)
	execTX(*sql.Tx, string, ...interface{}) (sql.Result, error)
	exec(*sqlx.DB, *sql.Tx, string, ...interface{}) (sql.Result, error)
}

type service struct {
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
)

type localProfile struct {
	*models.ProfileDataOutput
}

type localIdentity struct {
	*models.IdentityDataOutput
}

type localUniqueIdentity struct {
	*models.UniqueIdentityDataOutput
}

func (s *service) GetCountry(countryCode string, tx *sql.Tx) (countryData *models.CountryDataOutput, err error) {
	log.Info(fmt.Sprintf("GetCountry: countryCode:%s tx:%v", countryCode, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("GetCountry(exit): countryCode:%s tx:%v countryData:%+v err:%v", countryCode, tx != nil, countryData, err))
	}()
	countryData = &models.CountryDataOutput{}
	rows, err := s.query(
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
	sort.Slice(sortedDates, func(i, j int) bool {
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

func (s *service) MergeEnrollments(uuid string, organization *models.OrganizationDataOutput, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("MergeEnrollments: uuid:%s organization:%+v tx:%v", uuid, organization, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("MergeEnrollments(exit): uuid:%s organization:%+v tx:%v err:%v", uuid, organization, tx != nil, err))
	}()
	uniqueIdentity, err := s.GetUniqueIdentity(uuid, true, tx)
	if err != nil {
		return
	}
	disjoint, err := s.FindEnrollments([]string{"uuid", "organization_id"}, []interface{}{uuid, organization.ID}, []bool{false, false}, false, tx)
	if err != nil {
		return
	}
	if len(disjoint) == 0 {
		err = fmt.Errorf("merge enrollments unique identity '%+v' organization '%+v' found no enrollments", s.toLocalUniqueIdentity(uniqueIdentity), organization)
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
		newEnrollment := &models.EnrollmentDataOutput{UUID: uuid, OrganizationID: organization.ID, Start: st, End: en}
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
	log.Info(fmt.Sprintf("MoveEnrollmentToUniqueIdentity: enrollment:%+v uniqueIdentity:%+v tx:%v", enrollment, s.toLocalUniqueIdentity(uniqueIdentity), tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("MoveEnrollmentToUniqueIdentity(exit): enrollment:%+v uniqueIdentity:%+v tx:%v err:%v", enrollment, s.toLocalUniqueIdentity(uniqueIdentity), tx != nil, err))
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
		err = fmt.Errorf("'%+v' unique identity update affected %d rows", s.toLocalUniqueIdentity(oldUniqueIdentity), affected)
		return
	}
	affected, err = s.TouchUniqueIdentity(uniqueIdentity.UUID, tx)
	if err != nil {
		return
	}
	if affected != 1 {
		err = fmt.Errorf("'%+v' unique identity update affected %d rows", s.toLocalUniqueIdentity(uniqueIdentity), affected)
		return
	}
	return
}

func (s *service) MoveIdentityToUniqueIdentity(identity *models.IdentityDataOutput, uniqueIdentity *models.UniqueIdentityDataOutput, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("MoveIdentityToUniqueIdentity: identity:%+v uniqueIdentity:%+v tx:%v", s.toLocalIdentity(identity), s.toLocalUniqueIdentity(uniqueIdentity), tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("MoveIdentityToUniqueIdentity(exit): identity:%+v uniqueIdentity:%+v tx:%v err:%v", s.toLocalIdentity(identity), s.toLocalUniqueIdentity(uniqueIdentity), tx != nil, err))
	}()
	if identity.UUID == uniqueIdentity.UUID {
		return
	}
	oldUniqueIdentity, err := s.GetUniqueIdentity(identity.UUID, true, tx)
	if err != nil {
		return
	}
	identity.UUID = uniqueIdentity.UUID
	identity.LastModified = s.now()
	identity, err = s.EditIdentity(identity, true, tx)
	if err != nil {
		return
	}
	affected, err := s.TouchUniqueIdentity(oldUniqueIdentity.UUID, tx)
	if err != nil {
		return
	}
	if affected != 1 {
		err = fmt.Errorf("'%+v' unique identity update affected %d rows", s.toLocalUniqueIdentity(oldUniqueIdentity), affected)
		return
	}
	affected, err = s.TouchUniqueIdentity(uniqueIdentity.UUID, tx)
	if err != nil {
		return
	}
	if affected != 1 {
		err = fmt.Errorf("'%+v' unique identity update affected %d rows", s.toLocalUniqueIdentity(uniqueIdentity), affected)
		return
	}
	return
}

func (s *service) AddUniqueIdentity(inUniqueIdentity *models.UniqueIdentityDataOutput, refresh bool, tx *sql.Tx) (uniqueIdentity *models.UniqueIdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("AddUniqueIdentity: inUniqueIdentity:%+v refresh:%v tx:%v", s.toLocalUniqueIdentity(inUniqueIdentity), refresh, tx != nil))
	uniqueIdentity = inUniqueIdentity
	defer func() {
		log.Info(
			fmt.Sprintf(
				"AddUniqueIdentity(exit): inUniqueIdentity:%+v refresh:%v tx:%v uniqueIdentity:%+v err:%v",
				s.toLocalUniqueIdentity(inUniqueIdentity),
				refresh,
				tx != nil,
				s.toLocalUniqueIdentity(uniqueIdentity),
				err,
			),
		)
	}()
	if uniqueIdentity.LastModified == nil {
		uniqueIdentity.LastModified = s.now()
	}
	_, err = s.exec(
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
				s.toLocalOrganizations(organizations),
				err,
			),
		)
	}()
	sel := "select distinct o.id, o.name from organizations o, enrollments e where e.organization_id = o.id and e.uuid = ? order by o.name asc"
	rows, err := s.query(s.db, tx, sel, uuid)
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
				s.toLocalOrganizations(organizations),
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
	rows, err := s.query(s.db, tx, sel, values...)
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
				s.toLocalEnrollments(enrollments),
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
	rows, err := s.query(s.db, tx, sel, vals...)
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

func (s *service) GetUniqueIdentityEnrollments(uuid string, missingFatal bool, tx *sql.Tx) (enrollments []*models.EnrollmentDataOutput, err error) {
	log.Info(fmt.Sprintf("GetUniqueIdentityEnrollments: uuid:%s missingFatal:%v tx:%v", uuid, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetUniqueIdentityEnrollments(exit): uuid:%s missingFatal:%v tx:%v enrollments:%+v err:%v",
				uuid,
				missingFatal,
				tx != nil,
				s.toLocalEnrollments(enrollments),
				err,
			),
		)
	}()
	rows, err := s.query(
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
	rows, err := s.query(
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

func (s *service) GetUniqueIdentity(uuid string, missingFatal bool, tx *sql.Tx) (uniqueIdentityData *models.UniqueIdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("GetUniqueIdentity: uuid:%s missingFatal:%v tx:%v", uuid, missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetUniqueIdentity(exit): uuid:%s missingFatal:%v tx:%v uniqueIdentityData:%+v err:%v",
				uuid,
				missingFatal,
				tx != nil,
				s.toLocalUniqueIdentity(uniqueIdentityData),
				err,
			),
		)
	}()
	uniqueIdentityData = &models.UniqueIdentityDataOutput{}
	rows, err := s.query(
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

func (s *service) GetIdentityUniqueIdentities(uniqueIdentity *models.UniqueIdentityDataOutput, missingFatal bool, tx *sql.Tx) (identities []*models.IdentityDataOutput, err error) {
	log.Info(fmt.Sprintf("GetIdentityUniqueIdentities: uniqueIdentity:%+v missingFatal:%v tx:%v", s.toLocalUniqueIdentity(uniqueIdentity), missingFatal, tx != nil))
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetIdentityUniqueIdentities(exit): uniqueIdentity:%s missingFatal:%v tx:%v identities:%+v err:%v",
				s.toLocalUniqueIdentity(uniqueIdentity),
				missingFatal,
				tx != nil,
				s.toLocalIdentities(identities),
				err,
			),
		)
	}()
	rows, err := s.query(
		s.db,
		tx,
		"select id, uuid, source, name, username, email, last_modified from identities where uuid = ?",
		uniqueIdentity.UUID,
	)
	if err != nil {
		return
	}
	fetched := false
	for rows.Next() {
		identityData := &models.IdentityDataOutput{}
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
		if !fetched {
			fetched = true
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
	if missingFatal && !fetched {
		err = fmt.Errorf("cannot find identities uuid '%s'", uniqueIdentity.UUID)
		return
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
				s.toLocalIdentity(identityData),
				err,
			),
		)
	}()
	identityData = &models.IdentityDataOutput{}
	rows, err := s.query(
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
				s.toLocalProfile(profileData),
				err,
			),
		)
	}()
	profileData = &models.ProfileDataOutput{}
	rows, err := s.query(
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
	res, err := s.exec(s.db, tx, "update identities set last_modified = ? where id = ?", time.Now(), id)
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
	res, err := s.exec(s.db, tx, "update uidentities set last_modified = ? where uuid = ?", time.Now(), uuid)
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
		del := "delete from uidentities_archive where archived_at = ?"
		res, err = s.exec(s.db, tx, del, tm)
	} else {
		if onlyLast {
			del := "delete from uidentities_archive where uuid = ? and archived_at = (" +
				"select max(archived_at) from uidentities_archive where uuid = ?)"
			res, err = s.exec(s.db, tx, del, uuid, uuid)
		} else {
			del := "delete from uidentities_archive where uuid = ?"
			res, err = s.exec(s.db, tx, del, uuid)
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
			"select uuid, now() from uidentites_archive " +
			"where uuid = ? and archived_at = ?"
		res, err = s.exec(s.db, tx, insert, uuid, tm)
	} else {
		insert := "insert into uidentities(uuid, last_modified) " +
			"select uuid, now() from uidentites_archive " +
			"where uuid = ? order by archived_at desc limit 1"
		res, err = s.exec(s.db, tx, insert, uuid)
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
	res, err := s.exec(s.db, tx, insert, tm, uuid)
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
	res, err := s.exec(s.db, tx, del, uuid)
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
		del := "delete from enrollments_archive where archived_at = ?"
		res, err = s.exec(s.db, tx, del, tm)
	} else {
		if onlyLast {
			del := "delete from enrollments_archive where id = ? and archived_at = (" +
				"select max(archived_at) from enrollments_archive where id = ?)"
			res, err = s.exec(s.db, tx, del, id, id)
		} else {
			del := "delete from enrollments_archive where id = ?"
			res, err = s.exec(s.db, tx, del, id)
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
		res, err = s.exec(s.db, tx, insert, id, tm)
	} else {
		insert := "insert into enrollments(id, uuid, organization_id, start, end) " +
			"select id, uuid, organization_id, start, end from enrollments_archive " +
			"where id = ? order by archived_at desc limit 1"
		res, err = s.exec(s.db, tx, insert, id)
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
	res, err := s.exec(s.db, tx, insert, tm, id)
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
	res, err := s.exec(s.db, tx, del, id)
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

func (s *service) DeleteIdentityArchive(id int64, missingFatal, onlyLast bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DeleteIdentityArchive: id:%d missingFatal:%v onlyLast:%v tm:%v tx:%v", id, missingFatal, onlyLast, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DeleteIdentityArchive(exit): id:%s missingFatal:%v onlyLast:%v tm:%v tx:%v err:%v", id, missingFatal, onlyLast, tm, tx != nil, err))
	}()
	var res sql.Result
	if tm != nil {
		del := "delete from identities_archive where archived_at = ?"
		res, err = s.exec(s.db, tx, del, tm)
	} else {
		if onlyLast {
			del := "delete from identities_archive where id = ? and archived_at = (" +
				"select max(archived_at) from identities_archive where id = ?)"
			res, err = s.exec(s.db, tx, del, id, id)
		} else {
			del := "delete from identities_archive where id = ?"
			res, err = s.exec(s.db, tx, del, id)
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
		err = fmt.Errorf("deleting archived identity id '%d' had no effect", id)
		return
	}
	return
}

func (s *service) UnarchiveIdentity(id int64, replace bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("UnarchiveIdentity: id:%d replace:%v tm:%v tx:%v", id, replace, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("UnarchiveIdentity(exit): id:%d replace:%v tm:%v tx:%v err:%v", id, replace, tm, tx != nil, err))
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
			"where id = ? order by archived_at desc limit 1"
		res, err = s.exec(s.db, tx, insert, id)
	} else {
		insert := "insert into identities(id, uuid, source, name, email, username, last_modified) " +
			"select id, uuid, source, name, email, username, now() from identities_archive " +
			"where id = ? and archived_at = ?"
		res, err = s.exec(s.db, tx, insert, id, tm)
	}
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if affected == 0 {
		err = fmt.Errorf("unachiving identity id '%d' created no data", id)
		return
	}
	err = s.DeleteIdentityArchive(id, true, tm == nil, tm, tx)
	if err != nil {
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
		del := "delete from profiles_archive where archived_at = ?"
		res, err = s.exec(s.db, tx, del, tm)
	} else {
		if onlyLast {
			del := "delete from profiles_archive where uuid = ? and archived_at = (" +
				"select max(archived_at) from profiles_archive where uuid = ?)"
			res, err = s.exec(s.db, tx, del, uuid, uuid)
		} else {
			del := "delete from profiles_archive where uuid = ?"
			res, err = s.exec(s.db, tx, del, uuid)
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
			"where uuid = ? order by archived_at desc limit 1"
		res, err = s.exec(s.db, tx, insert, uuid)
	} else {
		insert := "insert into profiles(uuid, name, email, gender, gender_acc, is_bot, country_code) " +
			"select uuid, name, email, gender, gender_acc, is_bot, country_code from profiles_archive " +
			"where uuid = ? and archived_at = ?"
		res, err = s.exec(s.db, tx, insert, uuid, tm)
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

func (s *service) ArchiveIdentity(id int64, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("ArchiveIdentity: id:%d tm:%v tx:%v", id, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("ArchiveIdentity(exit): id:%d tm:%v tx:%v err:%v", id, tm, tx != nil, err))
	}()
	if tm == nil {
		t := time.Now()
		tm = &t
	}
	insert := "insert into identities_archive(id, uuid, source, name, email, username, last_modified, archived_at) " +
		"select id, uuid, source, name, email, username, last_modified, ? from identities where id = ? limit 1"
	res, err := s.exec(s.db, tx, insert, tm, id)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if affected == 0 {
		err = fmt.Errorf("archiving identity id '%d' created no data", id)
		return
	}
	return
}

func (s *service) DeleteIdentity(id int64, archive, missingFatal bool, tm *time.Time, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("DeleteIdentity: id:%d archive:%v missingFatal:%v tm:%v tx:%v", id, archive, missingFatal, tm, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("DeleteIdentity(exit): id:%d archive:%v missingFatal:%v tm:%v tx:%v err:%v", id, archive, missingFatal, tm, tx != nil, err))
	}()
	if archive {
		err = s.ArchiveIdentity(id, tm, tx)
		if err != nil {
			return
		}
	}
	del := "delete from identities where id = ?"
	res, err := s.exec(s.db, tx, del, id)
	if err != nil {
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return
	}
	if missingFatal && affected == 0 {
		err = fmt.Errorf("deleting identity id '%d' had no effect", id)
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
	res, err := s.exec(s.db, tx, insert, tm, uuid)
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
	res, err := s.exec(s.db, tx, del, uuid)
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

func (s *service) ValidateEnrollment(enrollmentData *models.EnrollmentDataOutput, forUpdate bool, tx *sql.Tx) (err error) {
	log.Info(fmt.Sprintf("ValidateEnrollment: enrollmentData:%+v forUpdate:%v tx:%v", enrollmentData, forUpdate, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("ValidateEnrollment(exit): enrollmentData:%+v forUpdate:%v tx:%v err:%v", enrollmentData, forUpdate, tx != nil, err))
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
	err = s.ValidateEnrollment(enrollmentData, false, tx)
	if err != nil {
		enrollmentData = nil
		return
	}
	insert := "insert into enrollments(uuid, organization_id, start, end) select ?, ?, str_to_date(?, ?), str_to_date(?, ?)"
	var res sql.Result
	res, err = s.exec(
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
	err = s.ValidateEnrollment(enrollmentData, true, tx)
	if err != nil {
		err = fmt.Errorf("enrollment '%+v' didn't pass update validation", enrollmentData)
		enrollmentData = nil
		return
	}
	update := "update enrollments set uuid = ?, organization_id = ?, start = str_to-date(?, ?), end = str_to_date(?, ?) where id = ?"
	var res sql.Result
	res, err = s.exec(
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
	log.Info(fmt.Sprintf("EditIdentity: inIdentityData:%+v refresh:%v tx:%v", s.toLocalIdentity(inIdentityData), refresh, tx != nil))
	identityData = inIdentityData
	defer func() {
		log.Info(
			fmt.Sprintf(
				"EditIdentity(exit): inIdentityData:%+v refresh:%v tx:%v identityData:%+v err:%v",
				s.toLocalIdentity(inIdentityData),
				refresh,
				tx != nil,
				s.toLocalIdentity(identityData),
				err,
			),
		)
	}()
	if identityData.ID == "" || identityData.UUID == "" || identityData.Source == "" {
		err = fmt.Errorf("identity '%+v' missing id or uuid or source", s.toLocalIdentity(identityData))
		identityData = nil
		return
	}
	if identityData.LastModified == nil {
		identityData.LastModified = s.now()
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
	res, err = s.exec(s.db, tx, update, values...)
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
		err = fmt.Errorf("identity '%+v' update affected %d rows", s.toLocalIdentity(identityData), affected)
		identityData = nil
		return
	} else if affected == 1 {
		affected2 := int64(0)
		// Mark identity's matching unique identity as modified
		affected2, err = s.TouchUniqueIdentity(identityData.UUID, tx)
		if err != nil {
			identityData = nil
			return
		}
		if affected2 != 1 {
			err = fmt.Errorf("identity '%+v' unique identity update affected %d rows", s.toLocalIdentity(identityData), affected2)
			identityData = nil
			return
		}
	} else {
		log.Info(fmt.Sprintf("EditIdentity: identity '%+v' update didn't affected any rows", s.toLocalIdentity(identityData)))
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
	log.Info(fmt.Sprintf("EditProfile: inProfileData:%+v refresh:%v tx:%v", s.toLocalProfile(inProfileData), refresh, tx != nil))
	profileData = inProfileData
	defer func() {
		log.Info(
			fmt.Sprintf(
				"EditProfile(exit): inProfileData:%+v refresh:%v tx:%v profileData:%+v err:%v",
				s.toLocalProfile(inProfileData),
				refresh,
				tx != nil,
				s.toLocalProfile(profileData),
				err,
			),
		)
	}()
	if profileData.UUID == "" {
		err = fmt.Errorf("profile '%+v' missing uuid", s.toLocalProfile(profileData))
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
		if *profileData.IsBot != 0 && *profileData.IsBot != 1 {
			err = fmt.Errorf("profile '%+v' is_bot should be '0' or '1'", s.toLocalProfile(profileData))
			profileData = nil
			return
		}
		columns = append(columns, "is_bot")
		values = append(values, *profileData.IsBot)
	}
	if profileData.CountryCode != nil && *profileData.CountryCode != "" {
		_, err = s.GetCountry(*profileData.CountryCode, tx)
		if err != nil {
			profileData = nil
			return
		}
		columns = append(columns, "country_code")
		values = append(values, *profileData.CountryCode)
	}
	if profileData.Gender != nil {
		if *profileData.Gender != "male" && *profileData.Gender != "female" {
			err = fmt.Errorf("profile '%+v' gender should be 'male' or 'female'", s.toLocalProfile(profileData))
			profileData = nil
			return
		}
		columns = append(columns, "gender")
		values = append(values, *profileData.Gender)
		columns = append(columns, "gender_acc")
		if profileData.GenderAcc == nil {
			values = append(values, 100)
		} else {
			if *profileData.GenderAcc < 1 || *profileData.GenderAcc > 100 {
				err = fmt.Errorf("profile '%+v' gender_acc should be within [1, 100]", s.toLocalProfile(profileData))
				profileData = nil
				return
			}
			values = append(values, *profileData.GenderAcc)
		}
	}
	if profileData.Gender == nil && profileData.GenderAcc != nil {
		err = fmt.Errorf("profile '%+v' gender_acc can only be set when gender is given", s.toLocalProfile(profileData))
		profileData = nil
		return
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
		res, err = s.exec(s.db, tx, update, values...)
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
			err = fmt.Errorf("profile '%+v' update affected %d rows", s.toLocalProfile(profileData), affected)
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
				err = fmt.Errorf("profile '%+v' unique identity update affected %d rows", s.toLocalProfile(profileData), affected2)
				profileData = nil
				return
			}
		} else {
			log.Info(fmt.Sprintf("EditProfile: profile '%+v' update didn't affected any rows", s.toLocalProfile(profileData)))
		}
	} else {
		log.Info(fmt.Sprintf("EditProfile: profile '%+v' nothing to update", s.toLocalProfile(profileData)))
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

func (s *service) ArchiveUUID(uuid string, tx *sql.Tx) (tm time.Time, err error) {
	log.Info(fmt.Sprintf("ArchiveUUID: uuid:%s", uuid))
	defer func() {
		log.Info(fmt.Sprintf("ArchiveUUID(exit): uuid:%s tm:%v err:%v", uuid, tm, err))
	}()
	if uuid == "" {
		return
	}
	tm = time.Now()
	err = s.ArchiveUniqueIdentity(uuid, &tm, tx)
	if err != nil {
		return
	}
	return
}

func (s *service) MergeUniqueIdentities(fromUUID, toUUID string) (err error) {
	log.Info(fmt.Sprintf("MergeUniqueIdentities: fromUUID:%s toUUID:%s", fromUUID, toUUID))
	defer func() {
		log.Info(fmt.Sprintf("MergeUniqueIdentities(exit): fromUUID:%s toUUID:%s err:%v", fromUUID, toUUID, err))
	}()
	if fromUUID == toUUID {
		return
	}
	fromUU, err := s.GetUniqueIdentity(fromUUID, true, nil)
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
	_, err = s.ArchiveUUID(fromUUID, tx)
	if err != nil {
		return
	}
	_, err = s.ArchiveUUID(toUUID, tx)
	if err != nil {
		return
	}
	// Rollback unless tx was set to nil after successful commit
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
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
	identities, err := s.GetIdentityUniqueIdentities(fromUU, false, tx)
	if err != nil {
		return
	}
	for _, identity := range identities {
		err = s.MoveIdentityToUniqueIdentity(identity, toUU, tx)
		if err != nil {
			return
		}
		enrollments := []*models.EnrollmentDataOutput{}
		enrollments, err = s.GetUniqueIdentityEnrollments(fromUUID, false, tx)
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
		err = s.MergeEnrollments(toUUID, org, tx)
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

func (s *service) MoveIdentity(fromID, toUUID string) (err error) {
	log.Info(fmt.Sprintf("MoveIdentity: fromID:%s toUUID:%s", fromID, toUUID))
	defer func() {
		log.Info(fmt.Sprintf("MoveIdentity(exit): fromID:%s toUUID:%s err:%v", fromID, toUUID, err))
	}()
	from, err := s.GetIdentity(fromID, true, nil)
	if err != nil {
		return
	}
	to, err := s.GetUniqueIdentity(toUUID, false, nil)
	if err != nil {
		return
	}
	if to == nil && fromID != toUUID {
		err = fmt.Errorf("profile uuid '%s' is not found and identity id is different: '%s'", toUUID, s.toLocalIdentity(from))
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
	}
	err = s.MoveIdentityToUniqueIdentity(from, to, tx)
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

// PutOrgDomain - add domain to organization
func (s *service) PutOrgDomain(org, dom string, overwrite, isTopDomain bool) (putOrgDomain *models.PutOrgDomainOutput, err error) {
	log.Info(fmt.Sprintf("PutOrgDomain: org:%s dom:%s overwrite:%v isTopDomain:%v", org, dom, overwrite, isTopDomain))
	putOrgDomain = &models.PutOrgDomainOutput{}
	defer func() {
		log.Info(fmt.Sprintf("PutOrgDomain(exit): org:%s dom:%s overwrite:%v isTopDomain:%v putOrgDomain:%+v err:%v", org, dom, overwrite, isTopDomain, putOrgDomain, err))
	}()
	rows, err := s.query(s.db, nil, "select id from organizations where name = ? limit 1", org)
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
	rows, err = s.query(s.db, nil, "select 1 from domains_organizations where organization_id = ? and domain = ?", orgID, dom)
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
	_, err = s.exec(
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
	var res sql.Result
	affected := int64(0)
	if overwrite {
		res, err = s.exec(
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
		res, err = s.exec(
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
		res, err = s.exec(
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

func (p *localProfile) String() (s string) {
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

func (p *localIdentity) String() (s string) {
	s = "{ID:" + p.ID + ",UUID:" + p.UUID + ",Source:" + p.Source + ","
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

func (p *localUniqueIdentity) String() (s string) {
	s = "{UUID:" + p.UUID + ","
	if p.LastModified == nil {
		s += "LastModified:nil}"
	} else {
		s += fmt.Sprintf("LastModified:%+v}", *p.LastModified)
	}
	return
}

func (s *service) now() *strfmt.DateTime {
	n := strfmt.DateTime(time.Now())
	return &n
}

func (s *service) toLocalOrganizations(ia []*models.OrganizationDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

func (s *service) toLocalEnrollments(ia []*models.EnrollmentDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

func (s *service) toLocalProfile(i *models.ProfileDataOutput) (o *localProfile) {
	if i == nil {
		return
	}
	o = &localProfile{i}
	return
}

func (s *service) toLocalIdentity(i *models.IdentityDataOutput) (o *localIdentity) {
	if i == nil {
		return
	}
	o = &localIdentity{i}
	return
}

func (s *service) toLocalIdentities(ia []*models.IdentityDataOutput) (oa []*localIdentity) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, &localIdentity{i})
	}
	return
}

func (s *service) toLocalUniqueIdentity(i *models.UniqueIdentityDataOutput) (o *localUniqueIdentity) {
	if i == nil {
		return
	}
	o = &localUniqueIdentity{i}
	return
}

func (s *service) queryOut(query string, args ...interface{}) {
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

func (s *service) queryDB(db *sqlx.DB, query string, args ...interface{}) (rows *sql.Rows, err error) {
	rows, err = db.Query(query, args...)
	if err != nil {
		log.Info("queryDB failed")
		s.queryOut(query, args...)
	}
	return
}

func (s *service) queryTX(db *sql.Tx, query string, args ...interface{}) (rows *sql.Rows, err error) {
	rows, err = db.Query(query, args...)
	if err != nil {
		log.Info("queryTX failed")
		s.queryOut(query, args...)
	}
	return
}

func (s *service) query(db *sqlx.DB, tx *sql.Tx, query string, args ...interface{}) (*sql.Rows, error) {
	if tx == nil {
		return s.queryDB(db, query, args...)
	}
	return s.queryTX(tx, query, args...)
}

func (s *service) execDB(db *sqlx.DB, query string, args ...interface{}) (res sql.Result, err error) {
	res, err = db.Exec(query, args...)
	if err != nil {
		log.Info("execDB failed")
		s.queryOut(query, args...)
	}
	return
}

func (s *service) execTX(db *sql.Tx, query string, args ...interface{}) (res sql.Result, err error) {
	res, err = db.Exec(query, args...)
	if err != nil {
		log.Info("execTX failed")
		s.queryOut(query, args...)
	}
	return
}

func (s *service) exec(db *sqlx.DB, tx *sql.Tx, query string, args ...interface{}) (sql.Result, error) {
	if tx == nil {
		return s.execDB(db, query, args...)
	}
	return s.execTX(tx, query, args...)
}
