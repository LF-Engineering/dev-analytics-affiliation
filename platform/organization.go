package platform

import (
	"fmt"
	"strconv"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"

	log "github.com/LF-Engineering/dev-analytics-affiliation/logging"
	"github.com/LF-Engineering/dev-analytics-libraries/orgs"
)

// Service - access platform org services
type Service interface {
	GetListOrganizations(string, int64, int64) (*models.GetListOrganizationsServiceOutput, error)
	LookupOrganization(name string) (*models.OrganizationServiceDataOutput, error)
}

type service struct {
	client *orgs.Org
}

// New return ES connection
func New(client *orgs.Org) Service {
	return &service{
		client: client,
	}
}

// GetListOrganizations ...
func (s *service) GetListOrganizations(q string, rows, page int64) (*models.GetListOrganizationsServiceOutput, error) {
	getListOrganizations := &models.GetListOrganizationsServiceOutput{}
	nRows := int64(0)
	var orgs []*models.OrganizationServiceDataOutput

	// lookup for exact org name match first
	sfdcOrg, err := s.client.LookupOrganization(q)
	if err != nil {
		return nil, err
	}

	// append if found in sfdc
	if sfdcOrg.Name != "" && sfdcOrg.ID != "" {
		orgs = append(orgs, &models.OrganizationServiceDataOutput{ID: (sfdcOrg.ID), Name: sfdcOrg.Name, Domains: []*models.DomainDataOutput{}})
	}

	// next, search for org name match.
	response, err := s.client.SearchOrganization(q, strconv.FormatInt(rows, 10), strconv.FormatInt(page-1, 10))
	if err != nil {
		return nil, err
	}

	for _, org := range response.Data {
		orgs = append(orgs, &models.OrganizationServiceDataOutput{ID: (org.ID), Name: org.Name, Domains: []*models.DomainDataOutput{}})
	}

	log.Info(fmt.Sprintf("GetListOrganizations: q:%s rows:%d page:%d", q, rows, page))

	getListOrganizations.Organizations = orgs
	getListOrganizations.NRecords = nRows
	getListOrganizations.Rows = int64(len(orgs))

	if rows == 0 {
		getListOrganizations.NPages = 1
	} else {
		pages := nRows / rows
		if nRows%rows != 0 {
			pages++
		}
		getListOrganizations.NPages = pages
	}

	getListOrganizations.Page = page
	if q != "" {
		getListOrganizations.Search = "q=" + q
	}

	return getListOrganizations, nil
}

// LookupOrganization ...
func (s *service) LookupOrganization(name string) (*models.OrganizationServiceDataOutput, error) {
	org, err := s.client.LookupOrganization(name)
	if err != nil {
		return nil, err
	}

	return &models.OrganizationServiceDataOutput{ID: org.ID, Name: org.Name,
			Domains: []*models.DomainDataOutput{{Name: org.Link, OrganizationName: org.Name}}},
		nil
}
