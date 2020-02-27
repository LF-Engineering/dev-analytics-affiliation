package shdb

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"database/sql"

	"github.com/jmoiron/sqlx"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"

	_ "github.com/go-sql-driver/mysql"
)

type Service interface {
	// External CRUD methods
	GetCountry(string, *sql.Tx) (*models.CountryDataOutput, error)
	GetProfile(string, *sql.Tx) (*models.ProfileDataOutput, error)
	EditProfile(string, *models.ProfileDataOutput, bool, *sql.Tx) (*models.ProfileDataOutput, error)
	TouchUIdentity(string, *sql.Tx) (int64, error)

	// API endpoints
	PutOrgDomain(string, string, bool, bool) (*models.PutOrgDomainOutput, error)
	MergeProfiles(string, string) error
	MoveProfile(string, string) error

	// Internal methods
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

func (s *service) GetCountry(countryCode string, tx *sql.Tx) (*models.CountryDataOutput, error) {
	log.Info(fmt.Sprintf("GetCountry: code:%s tx:%v", countryCode, tx != nil))
	countryData := &models.CountryDataOutput{}
	db := s.db
	rows, err := s.query(
		db,
		tx,
		"select code, name, alpha3 from countries where code = ? limit 1",
		countryCode,
	)
	if err != nil {
		return nil, err
	}
	fetched := false
	for rows.Next() {
		err = rows.Scan(
			&countryData.Code,
			&countryData.Name,
			&countryData.Alpha3,
		)
		if err != nil {
			return nil, err
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	if err != nil {
		return nil, err
	}
	if !fetched {
		err = fmt.Errorf("cannot find country by code '%s'", countryCode)
		return nil, err
	}
	return countryData, nil
}

func (s *service) GetProfile(uuid string, tx *sql.Tx) (*models.ProfileDataOutput, error) {
	log.Info(fmt.Sprintf("GetProfile: uuid:%s tx:%v", uuid, tx != nil))
	profileData := &models.ProfileDataOutput{}
	db := s.db
	rows, err := s.query(
		db,
		tx,
		"select uuid, name, email, gender, gender_acc, is_bot, country_code from profiles where uuid = ? limit 1",
		uuid,
	)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	if err != nil {
		return nil, err
	}
	if !fetched {
		err = fmt.Errorf("cannot find profile '%s'", uuid)
		return nil, err
	}
	return profileData, nil
}

func (s *service) TouchUIdentity(uuid string, tx *sql.Tx) (int64, error) {
	log.Info(fmt.Sprintf("TouchUIdentity: uuid:%s tx:%v", uuid, tx != nil))
	res, err := s.exec(s.db, tx, "update uidentities set last_modified = ? where uuid = ?", time.Now(), uuid)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *service) EditProfile(uuid string, profileData *models.ProfileDataOutput, refresh bool, tx *sql.Tx) (*models.ProfileDataOutput, error) {
	log.Info(fmt.Sprintf("EditProfile: uuid:%s data:%+v tx:%v", uuid, &localProfile{profileData}, tx != nil))
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
			return nil, fmt.Errorf("profile '%+v' is_bot should be '0' or '1'", &localProfile{profileData})
		}
		columns = append(columns, "is_bot")
		values = append(values, *profileData.IsBot)
	}
	if profileData.CountryCode != nil && *profileData.CountryCode != "" {
		_, err := s.GetCountry(*profileData.CountryCode, tx)
		if err != nil {
			return nil, err
		}
		columns = append(columns, "country_code")
		values = append(values, *profileData.CountryCode)
	}
	if profileData.Gender != nil {
		if *profileData.Gender != "male" && *profileData.Gender != "female" {
			return nil, fmt.Errorf("profile '%+v' gender should be 'male' or 'female'", &localProfile{profileData})
		}
		columns = append(columns, "gender")
		values = append(values, *profileData.Gender)
		columns = append(columns, "gender_acc")
		if profileData.GenderAcc == nil {
			values = append(values, 100)
		} else {
			if *profileData.GenderAcc < 1 || *profileData.GenderAcc > 100 {
				return nil, fmt.Errorf("profile '%+v' gender_acc should be within [1, 100]", &localProfile{profileData})
			}
			values = append(values, *profileData.GenderAcc)
		}
	}
	if profileData.Gender == nil && profileData.GenderAcc != nil {
		return nil, fmt.Errorf("profile '%+v' gender_acc can only be set when gender is given: %+v", &localProfile{profileData})
	}
	db := s.db
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
		res, err := s.exec(db, tx, update, values...)
		if err != nil {
			return nil, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected > 1 {
			return nil, fmt.Errorf("profile '%+v' update affected %d rows", &localProfile{profileData}, affected)
		} else if affected == 1 {
			affected2, err := s.TouchUIdentity(profileData.UUID, tx)
			if err != nil {
				return nil, err
			}
			if affected2 != 1 {
				return nil, fmt.Errorf("profile '%+v' uidentity update affected %d rows", &localProfile{profileData}, affected2)
			}
		} else {
			log.Info(fmt.Sprintf("EditProfile: profile '%+v' update didn't affected any rows", &localProfile{profileData}))
		}
	} else {
		log.Info(fmt.Sprintf("EditProfile: profile '%+v' nothing to update", &localProfile{profileData}))
	}
	if refresh {
		var err error
		profileData, err = s.GetProfile(profileData.UUID, tx)
		if err != nil {
			return nil, err
		}
	}
	return profileData, nil
}

// PutOrgDomain - add domain to organization
func (s *service) PutOrgDomain(org, dom string, overwrite, isTopDomain bool) (*models.PutOrgDomainOutput, error) {
	log.Info(fmt.Sprintf("PutOrgDomain: org:%s dom:%s overwrite:%v isTopDomain:%v", org, dom, overwrite, isTopDomain))
	putOrgDomain := &models.PutOrgDomainOutput{}
	db := s.db
	rows, err := s.query(db, nil, "select id from organizations where name = ? limit 1", org)
	if err != nil {
		return nil, err
	}
	var orgID int
	fetched := false
	for rows.Next() {
		err = rows.Scan(&orgID)
		if err != nil {
			return nil, err
		}
		fetched = true
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	if err != nil {
		return nil, err
	}
	if !fetched {
		err = fmt.Errorf("cannot find organization '%s'", org)
		return nil, err
	}
	rows, err = s.query(db, nil, "select 1 from domains_organizations where organization_id = ? and domain = ?", orgID, dom)
	if err != nil {
		return nil, err
	}
	dummy := 0
	for rows.Next() {
		err = rows.Scan(&dummy)
		if err != nil {
			return nil, err
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	if err != nil {
		return nil, err
	}
	if dummy == 1 {
		err = fmt.Errorf("domain '%s' is already assigned to organization '%s'", dom, org)
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	// Rollback unless tx was set to nil after successful commit
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	_, err = s.exec(
		db,
		tx,
		"insert into domains_organizations(organization_id, domain, is_top_domain) select ?, ?, ?",
		orgID,
		dom,
		isTopDomain,
	)
	if err != nil {
		return nil, err
	}
	if overwrite {
		res, err := s.exec(
			db,
			tx,
			"delete from enrollments where uuid in (select distinct sub.uuid from ("+
				"select distinct uuid from profiles where email like ? "+
				"union select distinct uuid from identities where email like ?) sub)",
			"%"+dom,
			"%"+dom,
		)
		if err != nil {
			return nil, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected > 0 {
			putOrgDomain.Deleted = fmt.Sprintf("%d", affected)
			putOrgDomain.Info = "deleted: " + putOrgDomain.Deleted
		}
		res, err = s.exec(
			db,
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
			return nil, err
		}
		affected, err = res.RowsAffected()
		if err != nil {
			return nil, err
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
		res, err := s.exec(
			db,
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
			return nil, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected > 0 {
			putOrgDomain.Added = fmt.Sprintf("%d", affected)
			putOrgDomain.Info = "added: " + putOrgDomain.Added
		}
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
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
	return putOrgDomain, nil
}

func (s *service) MergeProfiles(fromUUID, toUUID string) (err error) {
	if fromUUID == toUUID {
		return
	}
	from, err := s.GetProfile(fromUUID, nil)
	if err != nil {
		return
	}
	to, err := s.GetProfile(toUUID, nil)
	if err != nil {
		return
	}
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
	to, err = s.EditProfile(toUUID, to, true, tx)
	if err != nil {
		return
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	// Set tx to nil, so deferred rollback will not happen
	tx = nil
	fmt.Printf("from:%+v to:%+v\n", &localProfile{from}, &localProfile{to})
	return
}

func (s *service) MoveProfile(fromUUID, toUUID string) (err error) {
	if fromUUID == toUUID {
		return
	}
	from, err := s.GetProfile(fromUUID, nil)
	if err != nil {
		return
	}
	to, err := s.GetProfile(toUUID, nil)
	if err != nil {
		return
	}
	fmt.Printf("from:%+v to:%+v\n", from, to)
	return
}

type localProfile struct {
	*models.ProfileDataOutput
}

func (p *localProfile) String() string {
	s := "{UUID:" + p.UUID + ","
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
	return s
}

func (s *service) queryOut(query string, args ...interface{}) {
	log.Info(query)
	fmt.Printf("%+v\n", args)
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
				s += fmt.Sprintf("%d:%+v ", vi+1, reflect.ValueOf(vv).Elem())
			}
		}
		log.Info("[" + s + "]")
	}
}

func (s *service) queryDB(db *sqlx.DB, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		s.queryOut(query, args...)
	}
	return rows, err
}

func (s *service) queryTX(db *sql.Tx, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		s.queryOut(query, args...)
	}
	return rows, err
}

func (s *service) query(db *sqlx.DB, tx *sql.Tx, query string, args ...interface{}) (*sql.Rows, error) {
	if tx == nil {
		return s.queryDB(db, query, args...)
	}
	return s.queryTX(tx, query, args...)
}

func (s *service) execDB(db *sqlx.DB, query string, args ...interface{}) (sql.Result, error) {
	res, err := db.Exec(query, args...)
	if err != nil {
		s.queryOut(query, args...)
	}
	return res, err
}

func (s *service) execTX(db *sql.Tx, query string, args ...interface{}) (sql.Result, error) {
	res, err := db.Exec(query, args...)
	if err != nil {
		s.queryOut(query, args...)
	}
	return res, err
}

func (s *service) exec(db *sqlx.DB, tx *sql.Tx, query string, args ...interface{}) (sql.Result, error) {
	if tx == nil {
		return s.execDB(db, query, args...)
	}
	return s.execTX(tx, query, args...)
}
