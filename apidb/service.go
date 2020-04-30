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
	GetDataSourceTypes(string) ([]string, error)
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

func (s *service) GetDataSourceTypes(projectSlug string) (dataSourceTypes []string, err error) {
	log.Info(fmt.Sprintf("GetDataSourceTypes: projectSlug:%s", projectSlug))
	defer func() {
		log.Info(fmt.Sprintf("GetDataSourceTypes(exit): projectSlug:%s dataSourceTypes:%+v", projectSlug, dataSourceTypes))
	}()
	rows, err := s.Query(
		s.db,
		nil,
		"select distinct coalesce(dsp.name || '/' || ds.name, ds.name) "+
			"from projects p, data_source_instances dsi, data_sources ds "+
			"left join data_sources dsp on dsp.id = ds.parent_id "+
			"where p.slug in ($1, $2) and p.id = dsi.project_id and dsi.data_source_id = ds.id",
		projectSlug,
		"/projects/"+projectSlug,
	)
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "GetDataSourceTypes")
		return
	}
	dataSourceType := ""
	for rows.Next() {
		err = rows.Scan(&dataSourceType)
		if err != nil {
			return
		}
		dataSourceTypes = append(dataSourceTypes, dataSourceType)
	}
	err = rows.Err()
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "GetDataSourceTypes")
		return
	}
	err = rows.Close()
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "GetDataSourceTypes")
		return
	}
	return
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
