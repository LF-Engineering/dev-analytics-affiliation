package apidb

import (
	"fmt"

	"database/sql"

	"github.com/jmoiron/sqlx"

	"github.com/LF-Engineering/dev-analytics-affiliation/errs"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
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
	GetListProjects(string) (*models.ListProjectsOutput, error)
	GetAllProjects() ([]string, error)
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

func (s *service) GetAllProjects() (projects []string, err error) {
	log.Info("GetAllProjects")
	defer func() {
		log.Info(fmt.Sprintf("GetAllProjects(exit): projects:%+v", projects))
	}()
	rows, err := s.Query(s.db, nil, "select distinct slug from projects where project_type = 0")
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "GetAllProjects")
		return
	}
	projectSlug := ""
	for rows.Next() {
		err = rows.Scan(&projectSlug)
		if err != nil {
			return
		}
		projects = append(projects, projectSlug)
	}
	err = rows.Err()
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "GetAllProjects")
		return
	}
	err = rows.Close()
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "GetAllProjects")
		return
	}
	return
}

func (s *service) GetListProjects(user string) (projects *models.ListProjectsOutput, err error) {
	log.Info(fmt.Sprintf("GetListProjects: user:%s", user))
	projects = &models.ListProjectsOutput{}
	defer func() {
		log.Info(fmt.Sprintf("GetListProjects(exit): user:%s projects:%+v", user, projects))
	}()
	// insert into access_control_entries(scope, subject, resource, action, effect) select '/projects/' || slug, 'internal-api-user', 'identity', 'manage', 0 from projects;
	// insert into access_control_entries(scope, subject, resource, action, effect) select slug, 'internal-api-user', 'identity', 'manage', 0 from projects;
	var rows *sql.Rows
	if user == "internal-api-user" {
		rows, err = s.Query(s.db, nil, "select distinct slug, name from projects where project_type = 0 order by slug")
	} else {
		rows, err = s.Query(
			s.db,
			nil,
			"select distinct ace.scope, p.name from access_control_entries ace, projects p "+
				"where ace.scope = p.slug and p.project_type = 0 and ace.subject = $1 and ace.resource = $2 and ace.action = $3 "+
				"and ace.scope not like $4 order by ace.scope",
			user,
			"identity",
			"manage",
			"/projects/%",
		)
	}
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "GetListProjects")
		return
	}
	projectSlug := ""
	projectName := ""
	for rows.Next() {
		err = rows.Scan(&projectSlug, &projectName)
		if err != nil {
			return
		}
		projects.Projects = append(projects.Projects, &models.ProjectDataOutput{ProjectSlug: projectSlug, ProjectName: projectName})
	}
	err = rows.Err()
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "GetListProjects")
		return
	}
	err = rows.Close()
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrServerError), "GetListProjects")
		return
	}
	return
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
