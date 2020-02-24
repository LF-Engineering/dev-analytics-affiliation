package shdb

import (
	"database/sql"
)

type Service interface {
}

type service struct {
	db *sql.DB
}

// New creates new db service instance with given db
func New(db *sql.DB) Service {
	return &service{
		db: db,
	}
}
