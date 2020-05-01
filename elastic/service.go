package elastic

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/LF-Engineering/dev-analytics-affiliation/errs"
	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/LF-Engineering/dev-analytics-affiliation/shared"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
)

// Service - interface to access ES data
type Service interface {
	shared.ServiceInterface
	// External methods
	GetUnaffiliated(string, int64) (*models.GetUnaffiliatedOutput, error)
	AggsUnaffiliated(string, int64) ([]*models.UnaffiliatedDataOutput, error)
	GetTopContributors(string, []string, int64, int64, int64, int64, string, string, string) (*models.TopContributorsFlatOutput, error)
	// Internal methods
	projectSlugToIndexPattern(string) string
	projectSlugToIndexPatterns(string, []string) []string
	contributorStatsMainQuery(string, string, string, int64, int64, int64, int64, string, string, string) (string, error)
	contributorStatsMergeQuery(string, string, string, string, string, string, int64, int64) (string, error)
	dataSourceTypeFields(string) (map[string]string, error)
	searchCondition(string, string) (string, error)
	getAllStringFields(string) ([]string, error)
	additionalWhere(string, string) (string, error)
	having(string, string) (string, error)
	orderBy(string, string, string) (string, error)
	dataSourceQuery(string) (map[string][]string, error)
	search(string, io.Reader) (*esapi.Response, error)
}

type service struct {
	shared.ServiceStruct
	client *elasticsearch.Client
	url    string
}

type aggsUnaffiliatedResult struct {
	Aggregations struct {
		Unaffiliated struct {
			Unaffiliated struct {
				Buckets []struct {
					Key      string `json:"key"`
					DocCount int64  `json:"doc_count"`
				} `json:"buckets"`
			} `json:"unaffiliated"`
		} `json:"unaffiliated"`
	} `json:"aggregations"`
}

// New return ES connection
func New(client *elasticsearch.Client, url string) Service {
	return &service{
		client: client,
		url:    url,
	}
}

func (s *service) projectSlugToIndexPattern(projectSlug string) (pattern string) {
	pattern = strings.TrimSpace(projectSlug)
	if strings.HasPrefix(pattern, "/projects/") {
		pattern = pattern[10:]
	}
	pattern = "sds-" + strings.Replace(pattern, "/", "-", -1)
	pattern = pattern + "-*,-*raw,-*for-merge"
	return
}

func (s *service) projectSlugToIndexPatterns(projectSlug string, dataSourceTypes []string) (patterns []string) {
	patternRoot := strings.TrimSpace(projectSlug)
	if strings.HasPrefix(patternRoot, "/projects/") {
		patternRoot = patternRoot[10:]
	}
	patternRoot = "sds-" + strings.Replace(patternRoot, "/", "-", -1) + "-"
	for _, dataSourceType := range dataSourceTypes {
		dataSourceType = strings.Replace(dataSourceType, "/", "-", -1)
		patterns = append(patterns, patternRoot+dataSourceType+"*,-*raw,-*for-merge")
	}
	return
}

func (s *service) GetUnaffiliated(projectSlug string, topN int64) (getUnaffiliated *models.GetUnaffiliatedOutput, err error) {
	log.Info(fmt.Sprintf("GetUnaffiliated: projectSlug:%s topN:%d", projectSlug, topN))
	pattern := ""
	getUnaffiliated = &models.GetUnaffiliatedOutput{}
	defer func() {
		unaff := ""
		nUnaffiliated := len(getUnaffiliated.Unaffiliated)
		if nUnaffiliated > shared.LogListMax {
			unaff = fmt.Sprintf("%d", nUnaffiliated)
		} else {
			unaff = fmt.Sprintf("%+v", s.ToLocalUnaffiliatedObj(getUnaffiliated))
		}
		log.Info(
			fmt.Sprintf(
				"GetUnaffiliated(exit): projectSlug:%s topN:%d pattern:%s getUnaffiliated:%+v err:%v",
				projectSlug,
				topN,
				pattern,
				unaff,
				err,
			),
		)
	}()
	pattern = s.projectSlugToIndexPattern(projectSlug)
	getUnaffiliated.Unaffiliated, err = s.AggsUnaffiliated(pattern, topN)
	return
}

func (s *service) AggsUnaffiliated(indexPattern string, topN int64) (unaffiliated []*models.UnaffiliatedDataOutput, err error) {
	log.Info(fmt.Sprintf("AggsUnaffiliated: index:%s topN:%d", indexPattern, topN))
	if topN <= 0 {
		topN = 2147483647
	}
	data := `{"size":0,"aggs":{"unaffiliated":{"filter":{"terms":{"author_org_name":["Unknown","NotFound","","-","?"]}},"aggs":{"unaffiliated":{"terms":{"field":"author_uuid","missing":"","size":`
	data += fmt.Sprintf("%d", topN)
	data += "}}}}}}"
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
				"AggsUnaffiliated(exit): index:%s topN:%d data:%s unaffiliated:%+v err:%v",
				indexPattern,
				topN,
				data,
				unaff,
				err,
			),
		)
	}()
	payloadBytes := []byte(data)
	payloadBody := bytes.NewReader(payloadBytes)
	var res *esapi.Response
	res, err = s.search(indexPattern, payloadBody)
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ES.search.request")
		return
	}
	defer res.Body.Close()
	if res.IsError() {
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ES.search.result.decode")
			return
		}
		err = fmt.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ES.search.result")
		return
	}
	var result aggsUnaffiliatedResult
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ES.search.aggs.decode")
		return
	}
	for _, bucket := range result.Aggregations.Unaffiliated.Unaffiliated.Buckets {
		// We don't have Name here yet (from the ES aggregation)
		unaffiliated = append(unaffiliated, &models.UnaffiliatedDataOutput{Contributions: bucket.DocCount, UUID: bucket.Key})
	}
	return
}

// Top contributor functions
func (s *service) getAllStringFields(indexPattern string) (fields []string, err error) {
	log.Info(fmt.Sprintf("getAllStringFields: indexPattern:%s", indexPattern))
	defer func() {
		log.Info(fmt.Sprintf("getAllStringFields(exit): indexPattern:%s fields:%+v err:%v", indexPattern, fields, err))
	}()
	data := fmt.Sprintf(`{"query":"show columns in \"%s\""}`, s.JSONEscape(indexPattern))
	payloadBytes := []byte(data)
	payloadBody := bytes.NewReader(payloadBytes)
	method := "POST"
	url := fmt.Sprintf("%s/_sql?format=csv", s.url)
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		err = fmt.Errorf("new request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		err = fmt.Errorf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != 200 {
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("ReadAll non-ok request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
			return
		}
		err = fmt.Errorf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, data, resp.StatusCode, body)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
		return
	}
	reader := csv.NewReader(resp.Body)
	row := []string{}
	n := 0
	for {
		row, err = reader.Read()
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			err = fmt.Errorf("Read CSV row #%d, error: %v/%T\n", n, err, err)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
			return
		}
		n++
		// hash_short,VARCHAR,keyword
		if row[1] == "VARCHAR" && row[2] == "keyword" {
			fields = append(fields, row[0])
		}
	}
	return
}

func (s *service) dataSourceQuery(query string) (result map[string][]string, err error) {
	payloadBytes := []byte(query)
	payloadBody := bytes.NewReader(payloadBytes)
	method := "POST"
	url := fmt.Sprintf("%s/_sql?format=csv", s.url)
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		err = fmt.Errorf("new request error: %+v for %s url: %s, query: %s\n", err, method, url, query)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		err = fmt.Errorf("do request error: %+v for %s url: %s query: %s\n", err, method, url, query)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != 200 {
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("ReadAll non-ok request error: %+v for %s url: %s query: %s\n", err, method, url, query)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
			return
		}
		err = fmt.Errorf("Method:%s url:%s status:%d\nquery:\n%s\nbody:\n%s\n", method, url, resp.StatusCode, query, body)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
		return
	}
	log.Debug(fmt.Sprintf("Query: %s", query))
	reader := csv.NewReader(resp.Body)
	row := []string{}
	n := 0
	i2n := make(map[int]string)
	for {
		row, err = reader.Read()
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			err = fmt.Errorf("Read CSV row #%d, error: %v/%T\n", n, err, err)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
			return
		}
		n++
		log.Debug(fmt.Sprintf("Row #%d: %+v", n, row))
		if n == 1 {
			result = make(map[string][]string)
			for i, col := range row {
				i2n[i] = col
				result[col] = []string{}
			}
			continue
		}
		for i, val := range row {
			col := i2n[i]
			ary := result[col]
			ary = append(ary, val)
			result[col] = ary

		}
	}
	return
}

func (s *service) searchCondition(indexPattern, search string) (condition string, err error) {
	log.Info(fmt.Sprintf("searchCondition: indexPattern:%s search:%s", indexPattern, search))
	defer func() {
		log.Info(fmt.Sprintf("searchCondition(exit): indexPattern:%s search:%s condition:%s err:%v", indexPattern, search, condition, err))
	}()
	if search == "" {
		return
	}
	ary := strings.Split(search, "=")
	if len(ary) > 1 {
		fields := ary[0]
		fieldsAry := strings.Split(fields, ",")
		if strings.TrimSpace(fieldsAry[0]) == "" {
			return
		}
		values := ary[1]
		valuesAry := strings.Split(values, ",")
		if strings.TrimSpace(valuesAry[0]) == "" {
			return
		}
		if len(fieldsAry) == 1 && fieldsAry[0] == "all" {
			fieldsAry, err = s.getAllStringFields(indexPattern)
			if err != nil {
				err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "searchCondition")
				return
			}
		}
		for _, value := range valuesAry {
			value := s.JSONEscape(s.ToCaseInsensitiveRegexp(value))
			for _, field := range fieldsAry {
				field = `\"` + s.JSONEscape(field) + `\"`
				if condition == "" {
					condition = "and (" + field + " rlike " + value
				} else {
					condition += " or " + field + " rlike " + value
				}
			}
		}
		if condition != "" {
			condition += ")"
		}
	} else {
		escaped := s.JSONEscape(s.ToCaseInsensitiveRegexp(search))
		condition = fmt.Sprintf(`
			and (\"author_name\" rlike %[1]s
			or \"author_org_name\" rlike %[1]s
			or \"author_uuid\" rlike %[1]s)
			`,
			escaped,
		)
	}
	return
}

func (s *service) dataSourceTypeFields(dataSourceType string) (fields map[string]string, err error) {
	log.Info(fmt.Sprintf("dataSourceTypeFields: dataSourceType:%s", dataSourceType))
	defer func() {
		log.Info(fmt.Sprintf("dataSourceTypeFields(exit): dataSourceType:%s fieldsAry:%+v err:%v", dataSourceType, fields, err))
	}()
	// FIXME: use correct fields
	switch dataSourceType {
	case "git":
		fields = map[string]string{
			"git_commits":       "count(distinct hash) as git_commits",
			"git_lines_added":   "sum(lines_added) as git_lines_added",
			"git_lines_removed": "sum(lines_removed) as git_lines_removed",
			"git_lines_changed": "sum(lines_changed) as git_lines_changed",
		}
	case "gerrit":
		fields = map[string]string{
			"gerrit_approvals":         "sum(is_gerrit_approval) as gerrit_approvals",
			"gerrit_changesets":        "sum(is_gerrit_changeset) as gerrit_changesets",
			"gerrit_merged_changesets": "count(status) as gerrit_merged_changesets",
		}
	case "jira":
		fields = map[string]string{
			"jira_issues_created":          "count(distinct issue_key) as jira_issues_created",
			"jira_issues_assigned":         "count(distinct assignee_uuid) as jira_issues_assigned",
			"jira_issues_closed":           "count(distinct assignee_uuid) as jira_issues_closed",
			"jira_comments":                "count(distinct comment_id) as jira_comments",
			"jira_average_issue_open_days": "avg(time_to_close_days) as jira_average_issue_open_days",
		}
	case "confluence":
		fields = map[string]string{
			"confluence_pages_created":    "sum(is_new_page) as confluence_pages_created",
			"confluence_pages_edited":     "sum(is_page) as confluence_pages_edited",
			"confluence_comments":         "sum(is_comment) as confluence_comments",
			"confluence_blog_posts":       "sum(is_blogpost) as confluence_blog_posts",
			"confluence_last_action_date": "max(grimoire_creation_date) as confluence_last_action_date",
		}
	case "github/issue":
		fields = map[string]string{
			"github_issue_issues_created":         "count(distinct id) as github_issue_issues_created",
			"github_issue_issues_assigned":        "count(distinct assignee_data_uuid) as github_issue_issues_assigned",
			"github_issue_average_time_open_days": "avg(time_open_days) as github_issue_average_time_open_days",
		}
	case "github/pull_request":
		fields = map[string]string{
			"github_pull_request_prs_created": "count(distinct id) as github_pull_request_prs_created",
			"github_pull_request_prs_merged":  "count(distinct id) as github_pull_request_prs_merged",
			"github_pull_request_prs_open":    "count(distinct id) as github_pull_request_prs_open",
			"github_pull_request_prs_closed":  "count(distinct id) as github_pull_request_prs_closed",
		}
	case "bugzilla", "bugzillarest":
		fields = map[string]string{
			"bugzilla_issues_created": "count(distinct url) as bugzilla_issues_created",
		}
	default:
		// FIXME: change to error when all known data sources are handled
		log.Info(fmt.Sprintf("WARNING: unknown data source type: %s", dataSourceType))
		//err = errs.Wrap(errs.New(fmt.Errorf("unknown data source type: %s", dataSourceType), errs.ErrBadRequest), "dataSourceTypeFields")
	}
	return
}

func (s *service) additionalWhere(dataSourceType, sortField string) (string, error) {
	if sortField == "cnt" {
		return "", nil
	}
	switch dataSourceType {
	case "all":
		switch sortField {
		case "cnt", "author_uuid":
			return "", nil
		}
	case "git":
		if len(sortField) > 4 && sortField[:4] != "git_" {
			return "", nil
		}
		switch sortField {
		case "git_commits":
			return `and \"hash\" is not null`, nil
		case "git_lines_added", "git_lines_removed", "git_lines_changed":
			sortField := sortField[4:]
			return fmt.Sprintf(`and \"%s\" is not null`, s.JSONEscape(sortField)), nil
		}
	case "gerrit":
		if len(sortField) > 7 && sortField[:7] != "gerrit_" {
			return "", nil
		}
		switch sortField {
		case "gerrit_approvals":
			return `and \"is_gerrit_approval\" is not null`, nil
		case "gerrit_changesets":
			return `and \"is_gerrit_changeset\" is not null`, nil
		case "gerrit_merged_changesets":
			return `and \"status\" = 'MERGED'`, nil
		}
	case "jira":
		if len(sortField) > 5 && sortField[:5] != "jira_" {
			return "", nil
		}
		switch sortField {
		case "jira_issues_created":
			return `and \"issue_key\" is not null`, nil
		case "jira_issues_assigned":
			return `and \"assignee_uuid\" is not null`, nil
		case "jira_average_issue_open_days":
			return `and \"time_to_close_days\" is not null`, nil
		case "jira_comments":
			return `and \"comment_id\" is not null and \"type\" = 'comment'`, nil
		case "jira_issues_closed":
			return `and \"assignee_uuid\" is not null and \"status\" in ('Closed', 'Resolved', 'Done')`, nil
		}
	case "confluence":
		if len(sortField) > 11 && sortField[:11] != "confluence_" {
			return "", nil
		}
		switch sortField {
		case "confluence_pages_created":
			return `and \"is_new_page\" is not null`, nil
		case "confluence_pages_edited":
			return `and \"is_page\" is not null`, nil
		case "confluence_comments":
			return `and \"is_comment\" is not null`, nil
		case "confluence_blog_posts":
			return `and \"is_blogpost\" is not null`, nil
		case "confluence_last_action_date":
			return `and \"grimoire_creation_date\" is not null`, nil
		}
	case "github/issue":
		if len(sortField) > 13 && sortField[:13] != "github_issue_" {
			return "", nil
		}
		switch sortField {
		case "github_issue_issues_created", "github_issue_average_time_open_days":
			return `and \"id\" is not null and \"pull_request\" = false`, nil
		case "github_issue_issues_assigned":
			return `and \"assignee_data_uuid\" is not null and \"id\" is not null and \"pull_request\" = false`, nil
		}
	case "github/pull_request":
		if len(sortField) > 20 && sortField[:20] != "github_pull_request_" {
			return "", nil
		}
		switch sortField {
		case "github_pull_request_prs_created":
			return `and \"id\" is not null and \"pull_request\" = true`, nil
		case "github_pull_request_prs_merged":
			return `and \"id\" is not null and \"pull_request\" = true and length(\"merged_by_data_uuid\") = 40 and \"merged\" = true`, nil
		case "github_pull_request_prs_open":
			return `and \"id\" is not null and \"pull_request\" = true and \"state\" = 'open'`, nil
		case "github_pull_request_prs_closed":
			return `and \"id\" is not null and \"pull_request\" = true and \"state\" = 'closed'`, nil
		}
	case "bugzilla", "bugzillarest":
		if len(sortField) > 9 && sortField[:9] != "bugzilla_" {
			return "", nil
		}
		switch sortField {
		case "bugzilla_issues_created":
			return `and \"url\" is not null`, nil
		}
	}
	return "", errs.Wrap(errs.New(fmt.Errorf("unknown dataSourceType/sortField: %s/%s", dataSourceType, sortField), errs.ErrBadRequest), "additionalWhere")
}

func (s *service) having(dataSourceType, sortField string) (string, error) {
	if sortField == "cnt" {
		return "", nil
	}
	switch dataSourceType {
	case "all":
		switch sortField {
		case "cnt", "author_uuid":
			return "", nil
		}
	case "git":
		if len(sortField) > 4 && sortField[:4] != "git_" {
			return "", nil
		}
		switch sortField {
		case "git_commits", "git_lines_added", "git_lines_removed", "git_lines_changed":
			return fmt.Sprintf(`having \"%s\" > 0`, s.JSONEscape(sortField)), nil
		}
	case "gerrit":
		if len(sortField) > 7 && sortField[:7] != "gerrit_" {
			return "", nil
		}
		switch sortField {
		case "gerrit_approvals", "gerrit_changesets", "gerrit_merged_changesets":
			return fmt.Sprintf(`having \"%s\" > 0`, s.JSONEscape(sortField)), nil
		}
	case "jira":
		if len(sortField) > 5 && sortField[:5] != "jira_" {
			return "", nil
		}
		switch sortField {
		case "jira_issues_created", "jira_issues_assigned", "jira_average_issue_open_days", "jira_comments", "jira_issues_closed":
			return fmt.Sprintf(`having \"%s\" > 0`, s.JSONEscape(sortField)), nil
		}
	case "confluence":
		if len(sortField) > 11 && sortField[:11] != "confluence_" {
			return "", nil
		}
		switch sortField {
		case "confluence_pages_created", "confluence_pages_edited", "confluence_comments", "confluence_blog_posts":
			return fmt.Sprintf(`having \"%s\" > 0`, s.JSONEscape(sortField)), nil
		case "confluence_last_action_date":
			return `having \"confluence_last_action_date\" > '1900-01-01'::timestamp`, nil
		}
	case "github/issue":
		if len(sortField) > 13 && sortField[:13] != "github_issue_" {
			return "", nil
		}
		switch sortField {
		case "github_issue_issues_created", "github_issue_average_time_open_days", "github_issue_issues_assigned":
			return fmt.Sprintf(`having \"%s\" > 0`, s.JSONEscape(sortField)), nil
		}
	case "github/pull_request":
		if len(sortField) > 20 && sortField[:20] != "github_pull_request_" {
			return "", nil
		}
		switch sortField {
		case "github_pull_request_prs_created", "github_pull_request_prs_merged", "github_pull_request_prs_closed", "github_pull_request_prs_open":
			return fmt.Sprintf(`having \"%s\" > 0`, s.JSONEscape(sortField)), nil
		}
	case "bugzilla", "bugzillarest":
		if len(sortField) > 9 && sortField[:9] != "bugzilla_" {
			return "", nil
		}
		switch sortField {
		case "bugzilla_issues_created":
			return fmt.Sprintf(`having \"%s\" > 0`, s.JSONEscape(sortField)), nil
		}
	}
	return "", errs.Wrap(errs.New(fmt.Errorf("unknown dataSourceType/sortField: %s/%s", dataSourceType, sortField), errs.ErrBadRequest), "having")
}

func (s *service) orderBy(dataSourceType, sortField, sortOrder string) (string, error) {
	dir := ""
	if sortOrder == "" || strings.ToLower(sortOrder) == "desc" {
		dir = "desc"
	} else if strings.ToLower(sortOrder) == "asc" {
		dir = "asc"
	} else {
		return "", errs.Wrap(errs.New(fmt.Errorf("unknown sortOrder: %s", sortOrder), errs.ErrBadRequest), "orderBy")
	}
	switch dataSourceType {
	case "all":
		switch sortField {
		case "author_uuid":
			return fmt.Sprintf(`order by \"%s\" %s`, s.JSONEscape(sortField), dir), nil
		}
	case "git":
		switch sortField {
		case "git_commits", "git_lines_added", "git_lines_removed", "git_lines_changed":
			return fmt.Sprintf(`order by \"%s\" %s`, s.JSONEscape(sortField), dir), nil
		}
	case "gerrit":
		switch sortField {
		case "gerrit_approvals", "gerrit_changesets", "gerrit_merged_changesets":
			return fmt.Sprintf(`order by \"%s\" %s`, s.JSONEscape(sortField), dir), nil
		}
	case "jira":
		switch sortField {
		case "jira_issues_created", "jira_issues_assigned", "jira_average_issue_open_days", "jira_comments", "jira_issues_closed":
			return fmt.Sprintf(`order by \"%s\" %s`, s.JSONEscape(sortField), dir), nil
		}
	case "confluence":
		switch sortField {
		case "confluence_pages_created", "confluence_pages_edited", "confluence_comments", "confluence_blog_posts", "confluence_last_action_date":
			return fmt.Sprintf(`order by \"%s\" %s`, s.JSONEscape(sortField), dir), nil
		}
	case "github/issue":
		switch sortField {
		case "github_issue_issues_created", "github_issue_average_time_open_days", "github_issue_issues_assigned":
			return fmt.Sprintf(`order by \"%s\" %s`, s.JSONEscape(sortField), dir), nil
		}
	case "github/pull_request":
		switch sortField {
		case "github_pull_request_prs_created", "github_pull_request_prs_merged", "github_pull_request_prs_closed", "github_pull_request_prs_open":
			return fmt.Sprintf(`order by \"%s\" %s`, s.JSONEscape(sortField), dir), nil
		}
	case "bugzilla", "bugzillarest":
		switch sortField {
		case "bugzilla_issues_created":
			return fmt.Sprintf(`order by \"%s\" %s`, s.JSONEscape(sortField), dir), nil
		}
	}
	return `order by \"cnt\" desc`, nil
}

func (s *service) contributorStatsMergeQuery(
	dataSourceType, indexPattern, column, columnStr, search, uuids string,
	from, to int64,
) (jsonStr string, err error) {
	additionalWhereStr := ""
	havingStr := ""
	additionalWhereStr, err = s.additionalWhere(dataSourceType, column)
	if err != nil {
		err = errs.Wrap(err, "contributorStatsMergeQuery")
		return
	}
	havingStr, err = s.having(dataSourceType, column)
	if err != nil {
		err = errs.Wrap(err, "contributorStatsMergeQuery")
		return
	}
	data := fmt.Sprintf(`
		select
			\"author_uuid\", %s
		from
			\"%s\"
		where
			\"author_uuid\" is not null
			and length(\"author_uuid\") = 40
			and not (\"author_bot\" = true)
			and cast(\"grimoire_creation_date\" as long) >= %d
			and cast(\"grimoire_creation_date\" as long) < %d
			%s
			%s
			%s
		group by
			\"author_uuid\"
			%s
		`,
		columnStr,
		s.JSONEscape(indexPattern),
		from,
		to,
		search,
		additionalWhereStr,
		uuids,
		havingStr,
	)
	re1 := regexp.MustCompile(`\r?\n`)
	re2 := regexp.MustCompile(`\s+`)
	data = strings.TrimSpace(re1.ReplaceAllString(re2.ReplaceAllString(data, " "), " "))
	jsonStr = `{"query":"` + data + `"}`
	return
}

func (s *service) contributorStatsMainQuery(
	dataSourceType, indexPattern, column string,
	from, to, limit, offset int64,
	search, sortField, sortOrder string,
) (jsonStr string, err error) {
	additionalWhereStr := ""
	havingStr := ""
	orderByClause := ""
	additionalWhereStr, err = s.additionalWhere(dataSourceType, sortField)
	if err != nil {
		err = errs.Wrap(err, "contributorStatsMainQuery")
		return
	}
	havingStr, err = s.having(dataSourceType, sortField)
	if err != nil {
		err = errs.Wrap(err, "contributorStatsMainQuery")
		return
	}
	orderByClause, err = s.orderBy(dataSourceType, sortField, sortOrder)
	if err != nil {
		err = errs.Wrap(err, "contributorStatsMainQuery")
		return
	}
	data := fmt.Sprintf(`
		select
			\"author_uuid\", %s
		from
			\"%s\"
		where
			\"author_uuid\" is not null
			and length(\"author_uuid\") = 40
			and not (\"author_bot\" = true)
			and cast(\"grimoire_creation_date\" as long) >= %d
			and cast(\"grimoire_creation_date\" as long) < %d
			%s
			%s
		group by
			\"author_uuid\"
			%s
			%s
		limit %d
		`,
		column,
		s.JSONEscape(indexPattern),
		from,
		to,
		search,
		additionalWhereStr,
		havingStr,
		orderByClause,
		(offset+1)*limit,
	)
	re1 := regexp.MustCompile(`\r?\n`)
	re2 := regexp.MustCompile(`\s+`)
	data = strings.TrimSpace(re1.ReplaceAllString(re2.ReplaceAllString(data, " "), " "))
	jsonStr = `{"query":"` + data + `"}`
	return
}

func (s *service) GetTopContributors(projectSlug string, dataSourceTypes []string, from, to, limit, offset int64, search, sortField, sortOrder string) (top *models.TopContributorsFlatOutput, err error) {
	// dataSourceTypes = []string{"git", "gerrit", "jira", "confluence", "github/issue", "github/pull_request", "bugzilla", "bugzillarest"}
	patterns := s.projectSlugToIndexPatterns(projectSlug, dataSourceTypes)
	log.Info(
		fmt.Sprintf(
			"GetTopContributors: projectSlug:%s dataSourceTypes:%+v patterns:%+v from:%d to:%d limit:%d offset:%d search:%s sortField:%s sortOrder:%s",
			projectSlug,
			dataSourceTypes,
			patterns,
			from,
			to,
			limit,
			offset,
			search,
			sortField,
			sortOrder,
		),
	)
	top = &models.TopContributorsFlatOutput{}
	defer func() {
		inf := ""
		nTop := len(top.Contributors)
		if nTop > shared.LogListMax {
			inf = fmt.Sprintf("%d", nTop)
		} else {
			inf = fmt.Sprintf("%+v", s.ToLocalTopContributorsFlatObj(top))
		}
		log.Info(
			fmt.Sprintf(
				"GetTopContributors(exit): projectSlug:%s dataSourceTypes:%+v patterns:%+v from:%d to:%d limit:%d offset:%d search:%s sortField:%s sortOrder:%s top:%+v err:%v",
				projectSlug,
				dataSourceTypes,
				patterns,
				from,
				to,
				limit,
				offset,
				search,
				sortField,
				sortOrder,
				inf,
				err,
			),
		)
	}()
	var dsFields map[string]string
	fields := make(map[string]map[string]string)
	mainPattern := ""
	mainDataSourceType := "all"
	mainColumn := "count(*) as cnt"
	mainSortField := "cnt"
	mainSortOrder := "desc"
	if sortField == "author_uuid" {
		mainSortField = "author_uuid"
		mainSortOrder = sortOrder
	}
	for i, dataSourceType := range dataSourceTypes {
		dsFields, err = s.dataSourceTypeFields(dataSourceType)
		if err != nil {
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
			return
		}
		fields[dataSourceType] = dsFields
		if mainPattern == "" {
			for column, columnStr := range dsFields {
				if column == sortField {
					mainPattern = patterns[i]
					mainDataSourceType = dataSourceType
					mainColumn = columnStr
					mainSortField = sortField
					mainSortOrder = sortOrder
					break
				}
			}
		}
	}
	if mainPattern == "" {
		if sortField != "" && sortField != "author_uuid" {
			err = errs.Wrap(errs.New(fmt.Errorf("cannot find main data source type for sort column: %s", sortField), errs.ErrBadRequest), "es.GetTopContributors")
			return
		}
		mainPattern = s.projectSlugToIndexPattern(projectSlug)
	}
	top.DataSourceTypes = []*models.DataSourceTypeFields{}
	for dataSourceType, dataSourceFields := range fields {
		dsFields := []string{}
		for field := range dataSourceFields {
			dsFields = append(dsFields, field)
		}
		top.DataSourceTypes = append(
			top.DataSourceTypes,
			&models.DataSourceTypeFields{
				Name:   dataSourceType,
				Fields: dsFields,
			},
		)
	}
	searchCond := ""
	searchCondMap := make(map[string]string)
	searchCond, err = s.searchCondition(mainPattern, search)
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
		return
	}
	searchCondMap[mainPattern] = searchCond
	query := ""
	query, err = s.contributorStatsMainQuery(mainDataSourceType, mainPattern, mainColumn, from, to, limit, offset, searchCond, mainSortField, mainSortOrder)
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
		return
	}
	var res map[string][]string
	res, err = s.dataSourceQuery(query)
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
		return
	}
	results := make(map[string]map[string]string)
	fromIdx := offset * limit
	toIdx := fromIdx + limit
	nResults := int64(len(res["author_uuid"]))
	if fromIdx > nResults {
		fromIdx = nResults
	}
	if toIdx > nResults {
		toIdx = nResults
	}
	if fromIdx == toIdx {
		return
	}
	var uuids []string
	for i := fromIdx; i < toIdx; i++ {
		uuid := res["author_uuid"][i]
		rec, ok := results[uuid]
		if !ok {
			rec = make(map[string]string)
		}
		for column, values := range res {
			if column == "author_uuid" || column == "cnt" {
				continue
			}
			rec[column] = values[i]
		}
		results[uuid] = rec
		uuids = append(uuids, uuid)
	}
	uuidsCond := `and \"author_uuid\" in (`
	for _, uuid := range uuids {
		uuidsCond += "'" + uuid + "',"
	}
	uuidsCond = uuidsCond[:len(uuidsCond)-1] + ")"
	thrN := s.GetThreadsNum()
	queries := make(map[string]map[string]string)
	if thrN > 1 {
		mtx := &sync.Mutex{}
		condMtx := &sync.Mutex{}
		ch := make(chan error)
		nThreads := 0
		for i, dataSourceType := range dataSourceTypes {
			queries[dataSourceType] = make(map[string]string)
			for column, columnStr := range fields[dataSourceType] {
				if column == sortField {
					continue
				}
				go func(ch chan error, dataSourceType, pattern, column, columnStr string) (err error) {
					defer func() {
						ch <- err
					}()
					var ok bool
					condMtx.Lock()
					searchCond, ok = searchCondMap[pattern]
					if !ok {
						searchCond, err = s.searchCondition(pattern, search)
						if err == nil {
							searchCondMap[pattern] = searchCond
						}
					}
					condMtx.Unlock()
					if err != nil {
						return
					}
					query := ""
					query, err = s.contributorStatsMergeQuery(dataSourceType, pattern, column, columnStr, searchCond, uuidsCond, from, to)
					if err != nil {
						return
					}
					mtx.Lock()
					queries[dataSourceType][column] = query
					mtx.Unlock()
					return
				}(ch, dataSourceType, patterns[i], column, columnStr)
				nThreads++
				if nThreads == thrN {
					err = <-ch
					nThreads--
					if err != nil {
						err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
						return
					}
				}
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
				return
			}
		}
	} else {
		for i, dataSourceType := range dataSourceTypes {
			queries[dataSourceType] = make(map[string]string)
			var ok bool
			searchCond, ok = searchCondMap[patterns[i]]
			if !ok {
				searchCond, err = s.searchCondition(patterns[i], search)
				if err != nil {
					err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
					return
				}
				searchCondMap[patterns[i]] = searchCond
			}
			for column, columnStr := range fields[dataSourceType] {
				if column == sortField {
					continue
				}
				queries[dataSourceType][column], err = s.contributorStatsMergeQuery(dataSourceType, patterns[i], column, columnStr, searchCond, uuidsCond, from, to)
				if err != nil {
					err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
					return
				}
			}
		}
	}
	mergeResults := func(res map[string][]string) (err error) {
		l := len(res["author_uuid"])
		for i := 0; i < l; i++ {
			uuid := res["author_uuid"][i]
			rec, ok := results[uuid]
			if !ok {
				err = errs.Wrap(errs.New(fmt.Errorf("merge query returned uuid %s which is not present in main query results", uuid), errs.ErrBadRequest), "mergeResults")
				return
			}
			for column, values := range res {
				if column == "author_uuid" {
					continue
				}
				rec[column] = values[i]
			}
			results[uuid] = rec
		}
		return
	}
	if thrN > 1 {
		ch := make(chan error)
		nThreads := 0
		mtx := &sync.Mutex{}
		for _, data := range queries {
			for column, query := range data {
				if column == sortField {
					continue
				}
				go func(ch chan error, query string) (err error) {
					defer func() {
						ch <- err
					}()
					var res map[string][]string
					res, err = s.dataSourceQuery(query)
					if err != nil {
						return
					}
					mtx.Lock()
					err = mergeResults(res)
					mtx.Unlock()
					return
				}(ch, query)
				nThreads++
				if nThreads == thrN {
					err = <-ch
					nThreads--
					if err != nil {
						err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
						return
					}
				}
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
				return
			}
		}
	} else {
		for _, data := range queries {
			for column, query := range data {
				if column == sortField {
					continue
				}
				var res map[string][]string
				res, err = s.dataSourceQuery(query)
				if err != nil {
					err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
					return
				}
				err = mergeResults(res)
				if err != nil {
					err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
					return
				}
			}
		}
	}
	getInt := func(uuid, column string) int64 {
		strVal, ok := results[uuid][column]
		if !ok {
			return 0
		}
		floatValue, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return 0
		}
		return int64(floatValue)
	}
	getFloat := func(uuid, column string) float64 {
		strVal, ok := results[uuid][column]
		if !ok {
			return 0
		}
		floatValue, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return 0
		}
		return floatValue
	}
	for _, uuid := range uuids {
		var ok bool
		confluenceLastActionDate := ""
		daysAgo := 0.0
		confluenceLastActionDate, ok = results[uuid]["confluence_last_action_date"]
		if ok {
			dt, err := s.TimeParseAny(confluenceLastActionDate)
			if err == nil {
				dtMillis := float64(dt.Unix() * 1000.0)
				nowMillis := float64(time.Now().Unix()) * 1000.0
				daysAgo = (nowMillis - dtMillis) / 86400000.0
			} else {
				confluenceLastActionDate = ""
			}
		}
		contributor := &models.ContributorFlatStats{
			UUID:                                 uuid,
			GitLinesOfCodeAdded:                  getInt(uuid, "git_lines_added"),
			GitLinesOfCodeChanged:                getInt(uuid, "git_lines_changed"),
			GitLinesOfCodeRemoved:                getInt(uuid, "git_lines_removed"),
			GitCommits:                           getInt(uuid, "git_commits"),
			GerritReviewsApproved:                getInt(uuid, "gerrit_approvals"),
			GerritMergedChangesets:               getInt(uuid, "gerrit_merged_changesets"),
			GerritChangesets:                     getInt(uuid, "gerrit_changesets"),
			JiraComments:                         getInt(uuid, "jira_comments"),
			JiraIssuesCreated:                    getInt(uuid, "jira_issues_created"),
			JiraIssuesAssigned:                   getInt(uuid, "jira_issues_assigned"),
			JiraIssuesClosed:                     getInt(uuid, "jira_issues_closed"),
			JiraAverageIssuesOpenDays:            getFloat(uuid, "jira_average_issue_open_days"),
			ConfluencePagesCreated:               getInt(uuid, "confluence_pages_created"),
			ConfluencePagesEdited:                getInt(uuid, "confluence_pages_edited"),
			ConfluenceBlogPosts:                  getInt(uuid, "confluence_blog_posts"),
			ConfluenceComments:                   getInt(uuid, "confluence_comments"),
			ConfluenceLastDocumentation:          confluenceLastActionDate,
			ConfluenceDateSinceLastDocumentation: daysAgo,
			GithubIssuesCreated:                  getInt(uuid, "github_issue_issues_created"),
			GithubIssuesAssigned:                 getInt(uuid, "github_issue_issues_assigned"),
			GithubIssuesAverageTimeOpenDays:      getFloat(uuid, "github_issue_average_time_open_days"),
			GithubPullRequestsCreated:            getInt(uuid, "github_pull_request_prs_created"),
			GithubPullRequestsMerged:             getInt(uuid, "github_pull_request_prs_merged"),
			GithubPullRequestsOpen:               getInt(uuid, "github_pull_request_prs_open"),
			GithubPullRequestsClosed:             getInt(uuid, "github_pull_request_prs_closed"),
			BugzillaIssuesCreated:                getInt(uuid, "bugzilla_issues_created"),
		}
		top.Contributors = append(top.Contributors, contributor)
	}
	return
}

func (s *service) search(index string, query io.Reader) (res *esapi.Response, err error) {
	return s.client.Search(
		s.client.Search.WithIndex(index),
		s.client.Search.WithBody(query),
	)
}
