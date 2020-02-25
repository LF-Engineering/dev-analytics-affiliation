package apidb

import (
	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
)

type Service interface {
}

type service struct {
	db *sqlx.DB
}

// New creates new db service instance with given db
func New(db *sqlx.DB) Service {
	return &service{
		db: db,
	}
}
