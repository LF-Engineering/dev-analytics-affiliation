package elastic

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"encoding/json"

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
	GetTopContributors(string, int64, int64, int64, int64) (*models.GetTopContributorsOutput, error)
	// Internal methods
	projectSlugToIndexPattern(string) string
	contributorStatsQuery(int64, int64, int64, int64) string
	search(string, io.Reader) (*esapi.Response, error)
}

type service struct {
	shared.ServiceStruct
	client *elasticsearch.Client
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
func New(client *elasticsearch.Client) Service {
	return &service{
		client: client,
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
		return
	}
	defer res.Body.Close()
	if res.IsError() {
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			return
		}
		err = fmt.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
		return
	}
	var result aggsUnaffiliatedResult
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return
	}
	for _, bucket := range result.Aggregations.Unaffiliated.Unaffiliated.Buckets {
		// We don't have Name here yet (from the ES aggregation)
		unaffiliated = append(unaffiliated, &models.UnaffiliatedDataOutput{Contributions: bucket.DocCount, UUID: bucket.Key})
	}
	return
}

func (s *service) contributorStatsQuery(from, to, limit, offset int64) string {
	return fmt.Sprintf(`{
  "size": 0,
  "query": {
    "bool": {
      "must": {
        "exists": {
          "field": "author_uuid"
        }
      },
      "filter": {
        "range": {
          "grimoire_creation_date": {
            "gte": %d,
            "lte": %d
          }
        }
      }
    }
  },
  "aggs": {
    "contributions": {
      "terms": {
        "field": "author_uuid",
        "missing": "",
        "size": %d
      },
      "aggs": {
        "lines_added": {
          "sum": {
            "field": "lines_added"
          }
        },
        "lines_changed": {
          "sum": {
            "field": "lines_changed"
          }
        },
        "lines_removed": {
          "sum": {
            "field": "lines_removed"
          }
        },
        "commits": {
          "cardinality": {
            "field": "hash"
          }
        },
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
    }
  }
  }`, from, to, (offset+1)*limit)
}

type topContributorsResult struct {
	Aggregations struct {
		Contributions struct {
			Buckets []struct {
				Key        string `json:"key"`
				DocCount   int64  `json:"doc_count"`
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
			} `json:"buckets"`
		} `json:"contributions"`
	} `json:"aggregations"`
}

func (s *service) GetTopContributors(projectSlug string, from, to, limit, offset int64) (top *models.GetTopContributorsOutput, err error) {
	pattern := s.projectSlugToIndexPattern(projectSlug)
	log.Info(fmt.Sprintf("GetTopContributors: projectSlug:%s pattern:%s from:%d to:%d limit:%d offset:%d", projectSlug, pattern, from, to, limit, offset))
	top = &models.GetTopContributorsOutput{}
	data := s.contributorStatsQuery(from, to, limit, offset)
	defer func() {
		inf := ""
		nTop := len(top.Contributors)
		if nTop > shared.LogListMax {
			inf = fmt.Sprintf("%d", nTop)
		} else {
			inf = fmt.Sprintf("%+v", s.ToLocalTopContributorsObj(top))
		}
		log.Info(
			fmt.Sprintf(
				"GetTopContributors(exit): projectSlug:%s pattern:%s from:%d to:%d limit:%d offset:%d data:%s top:%+v err:%v",
				projectSlug,
				pattern,
				from,
				to,
				limit,
				offset,
				data,
				inf,
				err,
			),
		)
	}()
	payloadBytes := []byte(data)
	payloadBody := bytes.NewReader(payloadBytes)
	var res *esapi.Response
	res, err = s.search(pattern, payloadBody)
	if err != nil {
		return
	}
	defer res.Body.Close()
	if res.IsError() {
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			return
		}
		err = fmt.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
		return
	}
	/*
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return
		}
		log.Info(string(body))
	*/
	var result topContributorsResult
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return
	}
	idxFrom := limit * offset
	idxTo := idxFrom + limit
	for idx, bucket := range result.Aggregations.Contributions.Buckets {
		if int64(idx) >= idxFrom && int64(idx) < idxTo {
			contributor := &models.ContributorStats{UUID: bucket.Key}
			contributor.Git = &models.ContributorStatsGit{
				LinesOfCodeAdded:   int64(bucket.LinesAdded.Value),
				LinesOfCodeChanged: int64(bucket.LinesChanged.Value),
				LinesOfCodeRemoved: int64(bucket.LinesRemoved.Value),
				Commits:            int64(bucket.Commits.Value),
			}
			contributor.Gerrit = &models.ContributorStatsGerrit{
				ReviewsApproved:  int64(bucket.GerritApprovals.Value),
				MergedChangesets: int64(bucket.GerritMergedChangesets.Buckets.Merged.Changesets.Value),
			}
			top.Contributors = append(top.Contributors, contributor)
		}
	}
	return
}

func (s *service) search(index string, query io.Reader) (res *esapi.Response, err error) {
	return s.client.Search(
		s.client.Search.WithIndex(index),
		s.client.Search.WithBody(query),
	)
}
