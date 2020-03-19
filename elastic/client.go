package elastic

import (
	"fmt"
	"strings"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/elastic/go-elasticsearch/v7"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
)

// Service - interface to access ES data
type Service interface {
	// External methods
	GetUnaffiliated(string) (*models.GetUnaffiliatedOutput, error)
	// Internal methods
	toLocalUnaffiliated(*models.GetUnaffiliatedOutput) []interface{}
}

type service struct {
	client *elasticsearch.Client
}

// New return ES connection
func New(client *elasticsearch.Client) Service {
	return &service{
		client: client,
	}
}

func (s *service) GetUnaffiliated(projectSlug string) (getUnaffiliated *models.GetUnaffiliatedOutput, err error) {
	log.Info(fmt.Sprintf("GetUnaffiliated: projectSlug:%s", projectSlug))
	pattern := strings.TrimSpace(projectSlug)
	getUnaffiliated = &models.GetUnaffiliatedOutput{}
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetUnaffiliated(exit): projectSlug:%s pattern:%s getUnaffiliated:%+v err:%v",
				projectSlug,
				pattern,
				s.toLocalUnaffiliated(getUnaffiliated),
				err,
			),
		)
	}()
	//${idx}-*,-${idx}-*-raw
	if strings.HasPrefix(pattern, "/projects/") {
		pattern = pattern[10:]
	}
	pattern = "sds-" + strings.Replace(pattern, "/", "-", -1)
	pattern = pattern + "-*,-" + pattern + "-*-raw"

	//getUnaffiliated.Unaffiliated = ary
	return
}

func (s *service) toLocalUnaffiliated(ia *models.GetUnaffiliatedOutput) (oa []interface{}) {
	for _, i := range ia.Unaffiliated {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}
