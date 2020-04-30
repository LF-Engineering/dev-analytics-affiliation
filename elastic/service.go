package elastic

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"

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
	dataSourceQuery(string, string) (map[string][]string, error)
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
		err = fmt.Errorf("do request error: %+v for %s url: %s\n", err, method, url)
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
			err = fmt.Errorf("ReadAll non-ok request error: %+v for %s url: %s\n", err, method, url)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
			return
		}
		err = fmt.Errorf("Method:%s url:%s status:%d\n%s\n", method, url, resp.StatusCode, body)
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

func (s *service) dataSourceQuery(dataSourceType, query string) (result map[string][]string, err error) {
	payloadBytes := []byte(query)
	payloadBody := bytes.NewReader(payloadBytes)
	method := "POST"
	url := fmt.Sprintf("%s/_sql?format=csv", s.url)
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		err = fmt.Errorf("new request error: %+v for %s url: %s, quer: %s\n", err, method, url, query)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		err = fmt.Errorf("do request error: %+v for %s url: %s\n", err, method, url)
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
			err = fmt.Errorf("ReadAll non-ok request error: %+v for %s url: %s\n", err, method, url)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
			return
		}
		err = fmt.Errorf("Method:%s url:%s status:%d\n%s\n", method, url, resp.StatusCode, body)
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "getAllStringFields")
		fmt.Printf("\n>>>>>>>>>>>>>>>>>>>>>>>>> (%s)\n%s\n>>>>>>>>>>>>>>>>>>>>>>>>>\n", dataSourceType, query)
		return
	}
	fmt.Printf("\n+++++++++++++++++++++++++ (%s)\n%s\n+++++++++++++++++++++++++\n", dataSourceType, query)
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
		// FIXME: remove this
		fmt.Printf("row #%d --> %+v\n", n, row)
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
			value := "'%" + s.JSONEscape(value) + "%'"
			for _, field := range fieldsAry {
				field = `\"` + s.JSONEscape(field) + `\"`
				if condition == "" {
					condition = "and (" + field + " like " + value
				} else {
					condition += " or " + field + " like " + value
				}
			}
		}
		if condition != "" {
			condition += ")"
		}
	} else {
		escaped := "'%" + s.JSONEscape(search) + "%'"
		condition = fmt.Sprintf(`
      and (\"author_name\" like %[1]s
			or \"author_org_name\" like %[1]s
			or \"author_uuid\" like %[1]s)
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
	// FIXME: use correct fields: figure out gerrit merged-changesets
	switch dataSourceType {
	case "git":
		fields = map[string]string{
			"git_lines_added":   "sum(lines_added) as git_lines_added",
			"git_lines_removed": "sum(lines_removed) as git_lines_removed",
			"git_lines_changed": "sum(lines_changed) as git_lines_changed",
			"git_commits":       "count(distinct hash) as git_commits",
		}
	case "gerrit":
		fields = map[string]string{
			"gerrit_approvals":         "sum(is_gerrit_approval) as gerrit_approvals",
			"gerrit_changesets":        "sum(is_gerrit_changeset) as gerrit_changesets",
			"gerrit_merged_changesets": "count(status) as gerrit_merged_changesets",
		}
	default:
		err = errs.Wrap(errs.New(fmt.Errorf("unknown data source type: %s", dataSourceType), errs.ErrBadRequest), "dataSourceTypeFields")
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
		case "cnt":
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
		case "cnt":
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
		err = errs.Wrap(err, "contributorStatsMainQuery")
		return
	}
	havingStr, err = s.having(dataSourceType, column)
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
	/*
	  "aggs": {
	    "contributions": {
	      "terms": {
	        "field": "author_uuid",
	        "missing": "",
	        "size": %d
	      },
	        "jira": {
	          "filter": {
	            "wildcard": {
	              "_index": "*-jira"
	            }
	          },
	          "aggs": {
	            "jira_issues_created": {
	              "cardinality": {
	                "field": "issue_key"
	              }
	            },
	            "jira_issues_assigned": {
	              "cardinality": {
	                "field": "assignee_uuid"
	              }
	            },
	            "jira_average_issue_open_days": {
	              "avg": {
	                "field": "time_to_close_days"
	              }
	            }
	          }
	        },
	        "confluence": {
	          "filter": {
	            "wildcard": {
	              "_index": "*-confluence"
	            }
	          },
	          "aggs": {
	            "confluence_pages_created": {
	              "sum": {
	                "field": "is_new_page"
	              }
	            },
	            "confluence_pages_edited": {
	              "sum": {
	                "field": "is_page"
	              }
	            },
	            "confluence_comments": {
	              "sum": {
	                "field": "is_comment"
	              }
	            },
	            "confluence_blog_posts": {
	              "sum": {
	                "field": "is_blogpost"
	              }
	            },
	            "confluence_last_action_date": {
	              "max": {
	                "field": "grimoire_creation_date"
	              }
	            }
	          }
	        }
	      }
	    }
	  }
	*/
	return
}

func (s *service) GetTopContributors(projectSlug string, dataSourceTypes []string, from, to, limit, offset int64, search, sortField, sortOrder string) (top *models.TopContributorsFlatOutput, err error) {
	// FIXME: remove hardcoded data source type(s)
	dataSourceTypes = []string{"git", "gerrit"}
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
		if sortField != "" {
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
	res, err = s.dataSourceQuery(mainDataSourceType, query)
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
		// FIXME: what with empty result?
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
	for dataSourceType, data := range queries {
		for column, query := range data {
			fmt.Printf("%s/%s >>>>>>>>>>>>>>>>>>\n%s\n", dataSourceType, column, query)
		}
	}
	/*
		if thrN > 1 {
			ch := make(chan error)
			nThreads := 0
			mtx := &sync.Mutex{}
			for dataSourceType, query := range queries {
				go func(ch chan error, dataSourceType, query string) (err error) {
					defer func() {
						ch <- err
					}()
					var res map[string][]string
					res, err = s.dataSourceQuery(dataSourceType, query)
					if err != nil {
						return
					}
					mtx.Lock()
					results[dataSourceType] = res
					mtx.Unlock()
					return
				}(ch, dataSourceType, query)
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
			for nThreads > 0 {
				err = <-ch
				nThreads--
				if err != nil {
					err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
					return
				}
			}
		} else {
			for dataSourceType, query := range queries {
				results[dataSourceType], err = s.dataSourceQuery(dataSourceType, query)
				if err != nil {
					err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
					return
				}
			}
		}
		fmt.Printf("results:\n\n%+v\n\n", results)
	*/
	// FIXME: take contributors who come from current sortField and merge with other data sources
	// query that applies sort params should be main, others houdl only query for uuids returned from the main query
	// if no sort params are given we should use "cnt" special field and get Top N from all subqueries by cnt
	// so if git returns 10 objects and gerrit 10 objects, but there are 15 distinct UUIDs in general, choose
	// top 10 by cnt from both git and gerrit
	/*
		payloadBytes := []byte(data)
		payloadBody := bytes.NewReader(payloadBytes)
		var res *esapi.Response
		res, err = s.search(patterns[0], payloadBody)
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
		//body, err := ioutil.ReadAll(res.Body)
		//if err != nil {
		//	return
		//}
		//fmt.Printf("====================>\n%s\n", string(body))
		var result topContributorsResult
		if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "ES.search.aggs.decode")
			return
		}
		idxFrom := limit * offset
		idxTo := idxFrom + limit
		asc := strings.TrimSpace(strings.ToLower(sortOrder)) == "asc"
		buckets := result.Aggregations.Contributions.Buckets
		var sortFunc func(int, int) bool
		switch sortField {
		case "":
		case "uuid":
			sortFunc = func(i, j int) bool {
				return buckets[i].Key > buckets[j].Key
			}
		case "docs":
			sortFunc = func(i, j int) bool {
				return buckets[i].DocCount > buckets[j].DocCount
			}
		case "git_commits":
			sortFunc = func(i, j int) bool {
				return buckets[i].Git.Commits.Value > buckets[j].Git.Commits.Value
			}
		case "git_lines_of_code_added":
			sortFunc = func(i, j int) bool {
				return buckets[i].Git.LinesAdded.Value > buckets[j].Git.LinesAdded.Value
			}
		case "git_lines_of_code_removed":
			sortFunc = func(i, j int) bool {
				return buckets[i].Git.LinesRemoved.Value > buckets[j].Git.LinesRemoved.Value
			}
		case "git_lines_of_code_changed":
			sortFunc = func(i, j int) bool {
				return buckets[i].Git.LinesChanged.Value > buckets[j].Git.LinesChanged.Value
			}
		case "gerrit_merged_changesets":
			sortFunc = func(i, j int) bool {
				return buckets[i].Gerrit.GerritMergedChangesets.Buckets.Merged.Changesets.Value > buckets[j].Gerrit.GerritMergedChangesets.Buckets.Merged.Changesets.Value
			}
		case "gerrit_reviews_approved":
			sortFunc = func(i, j int) bool {
				return buckets[i].Gerrit.GerritApprovals.Value > buckets[j].Gerrit.GerritApprovals.Value
			}
		case "jira_issues_created":
			sortFunc = func(i, j int) bool {
				return buckets[i].Jira.IssuesCreated.Value > buckets[j].Jira.IssuesCreated.Value
			}
		case "jira_issues_assigned":
			sortFunc = func(i, j int) bool {
				return buckets[i].Jira.IssuesAssigned.Value > buckets[j].Jira.IssuesAssigned.Value
			}
		case "jira_average_issues_open_days":
			sortFunc = func(i, j int) bool {
				return buckets[i].Jira.AverageIssueOpenDays.Value > buckets[j].Jira.AverageIssueOpenDays.Value
			}
		case "confluence_pages_created":
			sortFunc = func(i, j int) bool {
				return buckets[i].Confluence.PagesCreated.Value > buckets[j].Confluence.PagesCreated.Value
			}
		case "confluence_pages_edited":
			sortFunc = func(i, j int) bool {
				return buckets[i].Confluence.PagesEdited.Value > buckets[j].Confluence.PagesEdited.Value
			}
		case "confluence_comments":
			sortFunc = func(i, j int) bool {
				return buckets[i].Confluence.Comments.Value > buckets[j].Confluence.Comments.Value
			}
		case "confluence_blog_posts":
			sortFunc = func(i, j int) bool {
				return buckets[i].Confluence.BlogPosts.Value > buckets[j].Confluence.BlogPosts.Value
			}
		case "confluence_last_documentation":
			sortFunc = func(i, j int) bool {
				return buckets[i].Confluence.LastActionDate.ValueAsString > buckets[j].Confluence.LastActionDate.ValueAsString
			}
		default:
			err = fmt.Errorf("unknown sort field: '%s'", sortField)
			err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "GetTopContributors")
			return
		}
		if sortFunc != nil {
			finalSortFunc := sortFunc
			if asc {
				finalSortFunc = func(i, j int) bool {
					return sortFunc(j, i)
				}
			}
			sort.SliceStable(result.Aggregations.Contributions.Buckets, finalSortFunc)
		}
		for idx, bucket := range result.Aggregations.Contributions.Buckets {
			if int64(idx) >= idxFrom && int64(idx) < idxTo {
				lastActionDateMillis := bucket.Confluence.LastActionDate.Value
				daysAgo := 0.0
				if lastActionDateMillis > 0 {
					nowMillis := float64(time.Now().Unix()) * 1000.0
					daysAgo = (nowMillis - lastActionDateMillis) / 86400000.0
				}
				contributor := &models.ContributorFlatStats{
					UUID:                      bucket.Key,
					GitLinesOfCodeAdded:       int64(bucket.Git.LinesAdded.Value),
					GitLinesOfCodeChanged:     int64(bucket.Git.LinesChanged.Value),
					GitLinesOfCodeRemoved:     int64(bucket.Git.LinesRemoved.Value),
					GitCommits:                int64(bucket.Git.Commits.Value),
					GerritReviewsApproved:     int64(bucket.Gerrit.GerritApprovals.Value),
					GerritMergedChangesets:    int64(bucket.Gerrit.GerritMergedChangesets.Buckets.Merged.Changesets.Value),
					JiraAverageIssuesOpenDays: bucket.Jira.AverageIssueOpenDays.Value,
					JiraIssuesAssigned:        int64(bucket.Jira.IssuesAssigned.Value),
					JiraIssuesCreated:         int64(bucket.Jira.IssuesCreated.Value),
					ConfluencePagesCreated:    int64(bucket.Confluence.PagesCreated.Value),
					ConfluencePagesEdited:     int64(bucket.Confluence.PagesEdited.Value),
					ConfluenceBlogPosts:       int64(bucket.Confluence.BlogPosts.Value),
					ConfluenceComments:        int64(bucket.Confluence.Comments.Value),
				}
				if lastActionDateMillis > 0 {
					contributor.ConfluenceDateSinceLastDocumentation = daysAgo
					contributor.ConfluenceLastDocumentation = bucket.Confluence.LastActionDate.ValueAsString
				}
				top.Contributors = append(top.Contributors, contributor)
			}
		}
	*/
	return
}

func (s *service) search(index string, query io.Reader) (res *esapi.Response, err error) {
	return s.client.Search(
		s.client.Search.WithIndex(index),
		s.client.Search.WithBody(query),
	)
}
