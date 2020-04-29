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
	contributorStatsQuery(string, string, int64, int64, int64, int64, string, string, string) (string, []string, error)
	dataSourceTypeFields(string) (string, []string, error)
	searchCondition(string, string) (string, error)
	getAllStringFields(string) ([]string, error)
	conditionForSort(string) string
	dataSourceQuery(string, string) ([][]string, error)
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

func (s *service) getAllStringFields(indexPattern string) (fieldsAry []string, err error) {
	log.Info(fmt.Sprintf("getAllStringFields: indexPattern:%s", indexPattern))
	defer func() {
		log.Info(fmt.Sprintf("getAllStringFields(exit): indexPattern:%s fieldsAry:%s err:%v", indexPattern, fieldsAry, err))
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
		}
		n++
		// hash_short,VARCHAR,keyword
		if row[1] == "VARCHAR" && row[2] == "keyword" {
			fieldsAry = append(fieldsAry, row[0])
		}
	}
	return
}

func (s *service) dataSourceQuery(dataSourceType, query string) (result [][]string, err error) {
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

func (s *service) dataSourceTypeFields(dataSourceType string) (fieldsStr string, fieldsAry []string, err error) {
	log.Info(fmt.Sprintf("dataSourceTypeFields: dataSourceType:%s", dataSourceType))
	defer func() {
		log.Info(fmt.Sprintf("dataSourceTypeFields(exit): dataSourceType:%s fieldsStr:%s fieldsAry:%+v err:%v", dataSourceType, fieldsStr, fieldsAry, err))
	}()
	// FIXME: use correct fields
	switch dataSourceType {
	case "git", "jira", "jenkins", "gerrit", "confluence", "groupsio":
		fieldsStr = "count(*) as cnt, sum(lines_added) as lines_added, sum(lines_removed) as lines_removed, sum(lines_changed) as lines_changed, count(distinct hash) as commits"
		fieldsAry = []string{"lines_added", "lines_removed", "lines_changed", "commits"}
	default:
		err = errs.Wrap(errs.New(fmt.Errorf("unknown data source type: %s", dataSourceType), errs.ErrBadRequest), "dataSourceTypeFields")
	}
	return
}

func (s *service) conditionForSort(sortField string) (condition string) {
	if sortField == "" {
		return ""
	}
	return fmt.Sprintf(`and \"%s\" is not null`, s.JSONEscape(sortField))
}

func (s *service) contributorStatsQuery(dataSourceType, indexPattern string, from, to, limit, offset int64, search, sortField, sortOrder string) (jsonStr string, fieldsAry []string, err error) {
	fieldsStr := ""
	fieldsStr, fieldsAry, err = s.dataSourceTypeFields(dataSourceType)
	if err != nil {
		err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "contributorStatsQuery")
		return
	}
	sortCondition := s.conditionForSort(sortField)
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
    limit %d
    `,
		fieldsStr,
		s.JSONEscape(indexPattern),
		from,
		to,
		search,
		sortCondition,
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
	      "aggs": {
	        "gerrit": {
	          "filter": {
	            "wildcard": {
	              "_index": "*-gerrit"
	            }
	          },
	          "aggs": {
	            "gerrit_approvals": {
	              "sum": {
	                "field": "is_gerrit_approval"
	              }
	            },
	            "gerrit-merged-changesets": {
	              "filters": {
	                "filters": {
	                  "merged": {
	                    "query_string": {
	                      "query": "status:\"MERGED\"",
	                      "analyze_wildcard": true,
	                      "default_field": "*"
	                    }
	                  }
	                }
	              },
	              "aggs": {
	                "changesets": {
	                  "sum": {
	                    "field": "is_gerrit_changeset"
	                  }
	                }
	              }
	            }
	          }
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

// Investigate via: `jq .aggregations.contributions.buckets[].gerrit` etc.
type topContributorsResult struct {
	Aggregations struct {
		Contributions struct {
			Buckets []struct {
				Key      string `json:"key"`
				DocCount int64  `json:"doc_count"`
				Git      struct {
					LinesAdded struct {
						Value float64 `json:"value"`
					} `json:"lines_added"`
					LinesChanged struct {
						Value float64 `json:"value"`
					} `json:"lines_changed"`
					LinesRemoved struct {
						Value float64 `json:"value"`
					} `json:"lines_removed"`
					Commits struct {
						Value float64 `json:"value"`
					} `json:"commits"`
				} `json:"git"`
				Gerrit struct {
					GerritApprovals struct {
						Value float64 `json:"value"`
					} `json:"gerrit_approvals"`
					GerritMergedChangesets struct {
						Buckets struct {
							Merged struct {
								Changesets struct {
									Value float64 `json:"value"`
								} `json:"changesets"`
							} `json:"merged"`
						} `json:"buckets"`
					} `json:"gerrit-merged-changesets"`
				} `json:"gerrit"`
				Jira struct {
					IssuesCreated struct {
						Value float64 `json:"value"`
					} `json:"jira_issues_created"`
					IssuesAssigned struct {
						Value float64 `json:"value"`
					} `json:"jira_issues_assigned"`
					AverageIssueOpenDays struct {
						Value float64 `json:"value"`
					} `json:"jira_average_issue_open_days"`
				} `json:"jira"`
				Confluence struct {
					PagesCreated struct {
						Value float64 `json:"value"`
					} `json:"confluence_pages_created"`
					PagesEdited struct {
						Value float64 `json:"value"`
					} `json:"confluence_pages_edited"`
					BlogPosts struct {
						Value float64 `json:"value"`
					} `json:"confluence_blog_posts"`
					Comments struct {
						Value float64 `json:"value"`
					} `json:"confluence_comments"`
					LastActionDate struct {
						Value         float64 `json:"value"`
						ValueAsString string  `json:"value_as_string"`
					} `json:"confluence_last_action_date"`
				} `json:"confluence"`
			} `json:"buckets"`
		} `json:"contributions"`
	} `json:"aggregations"`
}

func (s *service) GetTopContributors(projectSlug string, dataSourceTypes []string, from, to, limit, offset int64, search, sortField, sortOrder string) (top *models.TopContributorsFlatOutput, err error) {
	// FIXME
	dataSourceTypes = []string{"git"}
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
	thrN := s.GetThreadsNum()
	queries := make(map[string]string)
	fields := make(map[string][]string)
	searchCond := ""
	if thrN > 1 {
		mtx := &sync.Mutex{}
		ch := make(chan error)
		nThreads := 0
		for i, dataSourceType := range dataSourceTypes {
			go func(ch chan error, dataSourceType, pattern string) (err error) {
				defer func() {
					ch <- err
				}()
				searchCond, err = s.searchCondition(pattern, search)
				if err != nil {
					return
				}
				query := ""
				fieldsAry := []string{}
				query, fieldsAry, err = s.contributorStatsQuery(dataSourceType, pattern, from, to, limit, offset, searchCond, sortField, sortOrder)
				if err != nil {
					return
				}
				mtx.Lock()
				queries[dataSourceType] = query
				fields[dataSourceType] = fieldsAry
				mtx.Unlock()
				return
			}(ch, dataSourceType, patterns[i])
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
		for i, dataSourceType := range dataSourceTypes {
			searchCond, err = s.searchCondition(patterns[i], search)
			if err != nil {
				err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
				return
			}
			queries[dataSourceType], fields[dataSourceType], err = s.contributorStatsQuery(dataSourceType, patterns[i], from, to, limit, offset, searchCond, sortField, sortOrder)
			if err != nil {
				err = errs.Wrap(errs.New(err, errs.ErrBadRequest), "es.GetTopContributors")
				return
			}
		}
	}
	top.DataSourceTypes = []*models.DataSourceTypeFields{}
	for dataSourceType, dataSourceFields := range fields {
		top.DataSourceTypes = append(
			top.DataSourceTypes,
			&models.DataSourceTypeFields{
				Name:   dataSourceType,
				Fields: dataSourceFields,
			},
		)
	}
	results := make(map[string][][]string)
	if thrN > 1 {
		ch := make(chan error)
		nThreads := 0
		mtx := &sync.Mutex{}
		for dataSourceType, query := range queries {
			go func(ch chan error, dataSourceType, query string) (err error) {
				defer func() {
					ch <- err
				}()
				var res [][]string
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
