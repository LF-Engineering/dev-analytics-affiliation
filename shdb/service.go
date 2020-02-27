package shdb

import (
	"fmt"
	"reflect"
	"time"

	"database/sql"

	"github.com/jmoiron/sqlx"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"

	_ "github.com/go-sql-driver/mysql"
)

type Service interface {
	PutOrgDomain(string, string, bool, bool) (*models.PutOrgDomainOutput, error)
	GetProfile(string) (*models.ProfileDataOutput, error)
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

func (s *service) GetProfile(uuid string) (*models.ProfileDataOutput, error) {
	log.Info(fmt.Sprintf("GetProfile: uuid:%s", uuid))
	profileData := &models.ProfileDataOutput{}
	db := s.db
	rows, err := s.query(
		db,
		"select uuid, name, email, gender, gender_acc, is_bot, country_code from profiles where uuid = ?",
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

// PutOrgDomain - add domain to organization
func (s *service) PutOrgDomain(org, dom string, overwrite, isTopDomain bool) (*models.PutOrgDomainOutput, error) {
	log.Info(fmt.Sprintf("PutOrgDomain: org:%s dom:%s overwrite:%v isTopDomain:%v", org, dom, overwrite, isTopDomain))
	putOrgDomain := &models.PutOrgDomainOutput{}
	db := s.db
	rows, err := s.query(db, "select id from organizations where name = ?", org)
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
	rows, err = s.query(db, "select 1 from domains_organizations where organization_id = ? and domain = ?", orgID, dom)
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
	con, err := db.Begin()
	if err != nil {
		return nil, err
	}
	_, err = s.exec(
		con,
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
			con,
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
			con,
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
			con,
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
	err = con.Commit()
	if err != nil {
		return nil, err
	}
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
				s += fmt.Sprintf("%d:%+v ", vi+1, reflect.ValueOf(vv).Elem())
			}
		}
		log.Info("[" + s + "]")
	}
}

func (s *service) query(db *sqlx.DB, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		s.queryOut(query, args...)
	}
	return rows, err
}

func (s *service) exec(db *sql.Tx, query string, args ...interface{}) (sql.Result, error) {
	res, err := db.Exec(query, args...)
	if err != nil {
		s.queryOut(query, args...)
	}
	return res, err
}
