package health

import (
	"context"
	"fmt"
	"time"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
)

// Service handles async log of audit event
type Service interface {
	GetHealth(ctx context.Context) (*models.Health, error)
	SetServiceRequestID(requestID string)
	GetServiceRequestID() string
}

func (s *service) SetServiceRequestID(requestID string) {
	s.requestID = requestID
}

func (s *service) GetServiceRequestID() string {
	return s.requestID
}

type service struct {
	requestID string
}

// New is a simple helper function to create a service instance
func New() Service {
	return &service{}
}

func (s *service) GetHealth(ctx context.Context) (*models.Health, error) {
	log.WithField("X-REQUEST-ID", s.GetServiceRequestID()).Info("entered service GetHealth")

	t := time.Now()
	health := models.Health{
		DateTime:                       t.String(),
		DevAnalyticsAffiliationService: "Running",
	}

	log.WithField("X-REQUEST-ID", s.GetServiceRequestID()).Debug(fmt.Sprintf("%#v", health))

	return &health, nil
}
