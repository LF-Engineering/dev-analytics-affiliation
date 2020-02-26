package elastic

import (
	"github.com/elastic/go-elasticsearch/v7"
)

type Service interface {
}

type service struct {
	client *elasticsearch.Client
}

func New(client *elasticsearch.Client) Service {
	return &service{
		client: client,
	}
}
