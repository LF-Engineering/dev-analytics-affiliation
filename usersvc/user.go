package usersvc

import (
	"fmt"
	"strconv"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
	"github.com/LF-Engineering/dev-analytics-affiliation/shared"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
	"github.com/LF-Engineering/dev-analytics-libraries/users"
)

// Service - access platform user services
type Service interface {
	GetListUsers(string, int64, int64) (*models.UserDataArray, error)
	GetListAllUsers() (*models.UserDataArray, error)
}

type service struct {
	shared.ServiceStruct
	client *users.Usr
}

// New return ES connection
func New(client *users.Usr) Service {
	return &service{
		client: client,
	}
}

// GetListUsers ...
func (s *service) GetListUsers(q string, rows, page int64) (*models.UserDataArray, error) {
	getListUsers := &models.UserDataArray{}
	var users []*models.UserData
	response, err := s.client.ListUsers(q, strconv.FormatInt(rows, 10), strconv.FormatInt(page-1, 10))
	if err != nil {
		return nil, err
	}
	for _, usr := range response.Data {
		users = append(users, &models.UserData{ID: usr.ID, Name: usr.Name, Email: usr.Email, Username: usr.Username})
	}
	log.Info(fmt.Sprintf("GetListUsers: q:%s rows:%d page:%d", q, rows, page))
	getListUsers.Users = users
	return getListUsers, nil
}

// GetListAllUsers ...
func (s *service) GetListAllUsers() (*models.UserDataArray, error) {
	getListUsers := &models.UserDataArray{}
	var users []*models.UserData
	pageSize := 5000
	offset := 0
	total := -1
	for {
		response, err := s.client.ListUsers("", strconv.Itoa(pageSize), strconv.Itoa(offset))
		if err != nil {
			if strings.Contains(err.Error(), "502 Bad Gateway") {
				os.Sleep(5)
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
		log.Info(fmt.Sprintf("GetListAllUsers: got %d users so far, page size: %d, offset: %d", len(users), pageSize, offset))
		//log.Info(fmt.Sprintf("Metadata: %+v\n", response.Metadata))
	}
	getListUsers.Users = users
	return getListUsers, nil
}
