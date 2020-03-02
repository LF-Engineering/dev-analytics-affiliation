package elastic

import (
	"github.com/elastic/go-elasticsearch/v7"
)

// Service - interface to access ES data
type Service interface {
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
