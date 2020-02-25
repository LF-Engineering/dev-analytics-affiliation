package affiliation

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"database/sql"

	"github.com/jmoiron/sqlx"

	"github.com/LF-Engineering/dev-analytics-affiliation/apidb"
	"github.com/LF-Engineering/dev-analytics-affiliation/shdb"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/restapi/operations/affiliation"
	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
)

const (
	maxConcurrentRequests = 50
)

type Service interface {
	PutOrgDomain(ctx context.Context, in *affiliation.PutOrgDomainParams) (*models.PutOrgDomainOutput, error)
	SetServiceRequestID(requestID string)
	GetServiceRequestID() string
}

func (s *service) SetServiceRequestID(requestID string) {
	s.requestID = requestID
}

func (s *service) GetServiceRequestID() string {
	return s.requestID
}

type service struct {
	requestID string
	apiDB     apidb.Service
	shDB      shdb.Service
}

// New is a simple helper function to create a service instance
func New(apiDB apidb.Service, shDB shdb.Service) Service {
	return &service{
		apiDB: apiDB,
		shDB:  shDB,
	}
}

func queryOut(query string, args ...interface{}) {
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

func query(db *sqlx.DB, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		queryOut(query, args...)
	}
	return rows, err
}

func exec(db *sqlx.Tx, query string, args ...interface{}) (sql.Result, error) {
	res, err := db.Exec(query, args...)
	if err != nil {
		queryOut(query, args...)
	}
	return res, err
}

func (s *service) PutOrgDomain(ctx context.Context, params *affiliation.PutOrgDomainParams) (*models.PutOrgDomainOutput, error) {
	//affs, err := s.db.Baz()
	//err := fmt.Errorf("bo tak")
	org := params.OrgName
	dom := params.Domain
	overwrite := false
	isTopDomain := false
	if params.Overwrite != nil {
		overwrite = *params.Overwrite
	}
	if params.IsTopDomain != nil {
		isTopDomain = *params.IsTopDomain
	}
	log.Info(fmt.Sprintf("org:%s dom:%s overwrite:%v isTopDomain:%v\n", org, dom, overwrite, isTopDomain))
	putOrgDomain := &models.PutOrgDomainOutput{
		Deleted: "1",
		Added:   "2",
		Info:    params.OrgName,
	}
	return putOrgDomain, nil
	//Domain:microsoft.com IsTopDomain:0xc0004600c0 OrgName:microsoft Overwrite:0xc0004600c1
	/*
		// setOrgDomain: API params: 'organization_name' 'domain' [overwrite] [top]
		// if overwrite is set, all profiles found are force-updated/affiliated to 'organization_name'
		// if overwite is not set, API will not change any profiles which already have any affiliation(s)
		// if you specify "top" as 4th argument it will set 'is_top_domain' cvalue to true, else it will set false
		func setOrgDomain(db *sql.DB, args []string) (info string, err error) {
			if len(args) < 2 {
				fatalf("setOrgDomain: requires 2 args: organization name & domain")
			}
			rows, err := query(db, "select id from organizations where name = ?", org)
			fatalOnError(err)
			var orgID int
			fetched := false
			for rows.Next() {
				fatalOnError(rows.Scan(&orgID))
				fetched = true
			}
			fatalOnError(rows.Err())
			fatalOnError(rows.Close())
			if !fetched {
				info = fmt.Sprintf("cannot find organization '%s'", org)
				err = fmt.Errorf("%s", info)
				return
			}
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
			_, err = exec(
				con,
				"insert into domains_organizations(organization_id, domain, is_top_domain) select ?, ?, ?",
				orgID,
				dom,
				isTopDomain,
			)
			fatalOnError(err)
			if overwrite {
				res, err := exec(
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
				res, err = exec(
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
				res, err := exec(
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
}
