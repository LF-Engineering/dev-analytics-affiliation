package usersvc

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/LF-Engineering/dev-analytics-affiliation/shared"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
	"github.com/LF-Engineering/dev-analytics-libraries/users"
)

// Service - access platform user services
type Service interface {
	GetList(string, int64, int64) (*models.UserDataArray, error)
	GetListAll() (*models.UserDataArray, error)
}

type service struct {
	shared.ServiceStruct
	usr *users.Client
}

// New return ES connection
func New(usr *users.Client) Service {
	return &service{
		usr: usr,
	}
}

// GetList ...
func (s *service) GetList(q string, rows, page int64) (*models.UserDataArray, error) {
	getList := &models.UserDataArray{}
	var users []*models.UserData
	response, err := s.usr.List(q, strconv.FormatInt(rows, 10), strconv.FormatInt(page-1, 10))
	if err != nil {
		return nil, err
	}
	for _, usr := range response.Data {
		users = append(users, &models.UserData{ID: usr.ID, Name: usr.Name, Email: usr.Email, Username: usr.Username})
	}
	log.Info(fmt.Sprintf("GetList: q:%s rows:%d page:%d", q, rows, page))
	getList.Users = users
	return getList, nil
}

// GetListAll ...
func (s *service) GetListAll() (*models.UserDataArray, error) {
	getList := &models.UserDataArray{}
	var users []*models.UserData
	pageSize := 6000
	offset := 0
	total := -1
	for {
		response, err := s.usr.List("", strconv.Itoa(pageSize), strconv.Itoa(offset))
		if err != nil {
			if strings.Contains(err.Error(), "502 Bad Gateway") {
				time.Sleep(3 * time.Second)
				continue
			}
			return nil, err
		}
		for _, usr := range response.Data {
			users = append(users, &models.UserData{ID: usr.ID, Name: usr.Name, Email: usr.Email, Username: usr.Username})
		}
		if total < 0 {
			total = response.Metadata.TotalSize
		}
		if offset+pageSize < total {
			offset += pageSize
		}
		if offset >= total {
			break
		}
		log.Info(fmt.Sprintf("GetListAll: got %d users so far, page size: %d, offset: %d", len(users), pageSize, offset))
		//log.Info(fmt.Sprintf("Metadata: %+v\n", response.Metadata))
	}
	getList.Users = users
	return getList, nil
}
