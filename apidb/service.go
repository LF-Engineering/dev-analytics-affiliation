package apidb

import (
	"fmt"

	"database/sql"

	"github.com/jmoiron/sqlx"

	"github.com/LF-Engineering/dev-analytics-affiliation/errs"
	"github.com/LF-Engineering/dev-analytics-affiliation/shared"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"

	// We use Postgres as an API db
	_ "github.com/lib/pq"
)

// Service - accessing API db
type Service interface {
	shared.ServiceInterface
	CheckIdentityManagePermission(string, string, *sql.Tx) (bool, error)
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

func (s *service) CheckIdentityManagePermission(username, scope string, tx *sql.Tx) (allowed bool, err error) {
	log.Info(fmt.Sprintf("CheckIdentityManagePermission: username:%s scope:%s tx:%v", username, scope, tx != nil))
	defer func() {
		log.Info(fmt.Sprintf("CheckIdentityManagePermission(exit): username:%s scope:%s tx:%v allowed:%v err:%v", username, scope, tx != nil, allowed, err))
	}()
	rows, err := s.Query(
		s.db,
		tx,
		"select 1 from access_control_entries where "+
			"scope in ($1, $2) and subject = $3 and resource = $4 and action = $5",
		scope,
		"/projects/"+scope,
		username,
		"identity",
		"manage",
	)
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "CheckIdentityManagePermission")
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
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "CheckIdentityManagePermission")
		return
	}
	err = rows.Close()
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "CheckIdentityManagePermission")
		return
	}
	return
}
