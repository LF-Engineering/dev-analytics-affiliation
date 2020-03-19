package apidb

import (
	"fmt"
	"reflect"
	"time"

	"database/sql"

	"github.com/jmoiron/sqlx"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"

	// We use Postgres as an API db
	_ "github.com/lib/pq"
)

// Service - accessing API db
type Service interface {
	CheckIdentityManagePermission(string, string) (bool, error)
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

func (s *service) CheckIdentityManagePermission(username, scope string) (allowed bool, err error) {
	log.Info(fmt.Sprintf("CheckIdentityManagePermission: username:%s scope:%s", username, scope))
	defer func() {
		log.Info(fmt.Sprintf("CheckIdentityManagePermission(exit): username:%s scope:%s allowed:%v err:%v", username, scope, allowed, err))
	}()
	db := s.db
	rows, err := s.query(
		db,
		"select 1 from access_control_entries where "+
			"scope in ($1, $2) and subject = $3 and resource = $4 and action = $5",
		scope,
		"/projects/"+scope,
		username,
		"identity",
		"manage",
	)
	if err != nil {
		return
	}
	var dummy int
	for rows.Next() {
		err = rows.Scan(&dummy)
		if err != nil {
			return
		}
		allowed = true
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
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

func (s *service) query(db *sqlx.DB, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		s.queryOut(query, args...)
	}
	return rows, err
}
