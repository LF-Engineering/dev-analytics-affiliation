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

// PutOrgDomain - add domain to organization
func (s *service) PutOrgDomain(org, dom string, overwrite, isTopDomain bool) (*models.PutOrgDomainOutput, error) {
	log.Info(fmt.Sprintf("PutOrgDomain: org:%s dom:%s overwrite:%v isTopDomain:%v\n", org, dom, overwrite, isTopDomain))
	putOrgDomain := &models.PutOrgDomainOutput{}
	db := s.db
	rows, err := s.query(db, "aselect id from organizations where name = ?", org)
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
	//putOrgDomain := &models.PutOrgDomainOutput{Deleted: "1", Added: "2", Info: params.OrgName}
	/*
			rows, err = query(db, "select 1 from domains_organizations where organization_id = ? and domain = ?", orgID, dom)
			fatalOnError(err)
			dummy := 0
			for rows.Next() {
				fatalOnError(rows.Scan(&dummy))
			}
			fatalOnError(rows.Err())
			fatalOnError(rows.Close())
			if dummy == 1 {
				info = fmt.Sprintf("domain '%s' is already assigned to organization '%s'", dom, org)
				return
			}
			con, err := db.Begin()
			fatalOnError(err)
			_, err = s.exec(
				con,
				"insert into domains_organizations(organization_id, domain, is_top_domain) select ?, ?, ?",
				orgID,
				dom,
				isTopDomain,
			)
			fatalOnError(err)
			if overwrite {
				res, err := s.exec(
					con,
					"delete from enrollments where uuid in (select distinct sub.uuid from ("+
						"select distinct uuid from profiles where email like ? "+
						"union select distinct uuid from identities where email like ?) sub)",
					"%"+dom,
					"%"+dom,
				)
				fatalOnError(err)
				affected, err := res.RowsAffected()
				fatalOnError(err)
				if affected > 0 {
					info = fmt.Sprintf("deleted: %d", affected)
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
				fatalOnError(err)
				affected, err = res.RowsAffected()
				fatalOnError(err)
				if affected > 0 {
					if info == "" {
						info = fmt.Sprintf("inserted: %d", affected)
					} else {
						info += fmt.Sprintf("\ninserted: %d", affected)
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
				fatalOnError(err)
				affected, err := res.RowsAffected()
				fatalOnError(err)
				if affected > 0 {
					info = fmt.Sprintf("inserted: %d", affected)
				}
			}
			fatalOnError(con.Commit())
			return
		}
	*/
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

func (s *service) exec(db *sqlx.Tx, query string, args ...interface{}) (sql.Result, error) {
	res, err := db.Exec(query, args...)
	if err != nil {
		s.queryOut(query, args...)
	}
	return res, err
}
