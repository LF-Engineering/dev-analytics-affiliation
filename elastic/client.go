package elastic

import (
	"fmt"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/elastic/go-elasticsearch/v7"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
)

// Service - interface to access ES data
type Service interface {
	// External methods
	GetUnaffiliated() (*models.GetUnaffiliatedOutput, error)
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

func (s *service) GetUnaffiliated() (getUnaffiliated *models.GetUnaffiliatedOutput, err error) {
	log.Info("GetUnaffiliated")
	getUnaffiliated = &models.GetUnaffiliatedOutput{}
	defer func() {
		log.Info(
			fmt.Sprintf(
				"GetUnaffiliated(exit): getUnaffiliated:%+v err:%v",
				s.toLocalUnaffiliated(getUnaffiliated),
				err,
			),
		)
	}()
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
