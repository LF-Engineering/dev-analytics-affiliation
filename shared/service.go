package shared

import (
	"fmt"
	"strconv"

	"github.com/LF-Engineering/dev-analytics-affiliation/gen/models"
)

const (
	LogListMax = 10
)

// SharedServiceInterface - Shared API interface
type SharedServiceInterface interface {
	ToLocalOrganizations([]*models.OrganizationDataOutput) []interface{}
	ToLocalNestedOrganizations([]*models.OrganizationNestedDataOutput) []interface{}
	ToLocalMatchingBlacklist([]*models.MatchingBlacklistOutput) []interface{}
	ToLocalUnaffiliatedObj(*models.GetUnaffiliatedOutput) []interface{}
	ToLocalUnaffiliated([]*models.UnaffiliatedDataOutput) []interface{}
	ToLocalProfile(*models.ProfileDataOutput) *LocalProfile
	ToLocalIdentity(*models.IdentityDataOutput) *LocalIdentity
	ToLocalUniqueIdentity(*models.UniqueIdentityDataOutput) *LocalUniqueIdentity
	ToLocalEnrollments([]*models.EnrollmentDataOutput) []interface{}
}

// SharedServiceStruct - Shared API Struct
type SharedServiceStruct struct {
}

type LocalProfile struct {
	*models.ProfileDataOutput
}

type LocalIdentity struct {
	*models.IdentityDataOutput
}

type LocalUniqueIdentity struct {
	*models.UniqueIdentityDataOutput
}

func (p *LocalProfile) String() (s string) {
	s = "{UUID:" + p.UUID + ","
	if p.Name == nil {
		s += "Name:nil,"
	} else {
		s += "Name:" + *p.Name + ","
	}
	if p.Email == nil {
		s += "Email:nil,"
	} else {
		s += "Email:" + *p.Email + ","
	}
	if p.Gender == nil {
		s += "Gender:nil,"
	} else {
		s += "Gender:" + *p.Gender + ","
	}
	if p.GenderAcc == nil {
		s += "GenderAcc:nil,"
	} else {
		s += "GenderAcc:" + strconv.FormatInt(*p.GenderAcc, 10) + ","
	}
	if p.IsBot == nil {
		s += "IsBot:nil,"
	} else {
		s += "IsBot:" + strconv.FormatInt(*p.IsBot, 10) + ","
	}
	if p.CountryCode == nil {
		s += "CountryCode:nil}"
	} else {
		s += "CountryCode:" + *p.CountryCode + "}"
	}
	return
}

func (p *LocalIdentity) String() (s string) {
	s = "{ID:" + p.ID + ",UUID:" + p.UUID + ",Source:" + p.Source + ","
	if p.Name == nil {
		s += "Name:nil,"
	} else {
		s += "Name:" + *p.Name + ","
	}
	if p.Username == nil {
		s += "Username:nil,"
	} else {
		s += "Username:" + *p.Username + ","
	}
	if p.Email == nil {
		s += "Email:nil,"
	} else {
		s += "Email:" + *p.Email + ","
	}
	if p.LastModified == nil {
		s += "LastModified:nil}"
	} else {
		s += fmt.Sprintf("LastModified:%+v}", *p.LastModified)
	}
	return
}

func (p *LocalUniqueIdentity) String() (s string) {
	s = "{UUID:" + p.UUID + ","
	if p.LastModified == nil {
		s += "LastModified:nil}"
	} else {
		s += fmt.Sprintf("LastModified:%+v}", *p.LastModified)
	}
	return
}

func (s *SharedServiceStruct) ToLocalOrganizations(ia []*models.OrganizationDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

func (s *SharedServiceStruct) ToLocalNestedOrganizations(ia []*models.OrganizationNestedDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		ary := []interface{}{i.ID, i.Name}
		ary2 := []interface{}{}
		for _, d := range i.Domains {
			if d != nil {
				ary2 = append(ary2, *d)
			}
		}
		if len(ary2) > 0 {
			ary = append(ary, ary2)
		}
		oa = append(oa, ary)
	}
	return
}

func (s *SharedServiceStruct) ToLocalMatchingBlacklist(ia []*models.MatchingBlacklistOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

func (s *SharedServiceStruct) ToLocalUnaffiliatedObj(ia *models.GetUnaffiliatedOutput) (oa []interface{}) {
	for _, i := range ia.Unaffiliated {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

func (s *SharedServiceStruct) ToLocalUnaffiliated(ia []*models.UnaffiliatedDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

func (s *SharedServiceStruct) ToLocalEnrollments(ia []*models.EnrollmentDataOutput) (oa []interface{}) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, *i)
	}
	return
}

func (s *SharedServiceStruct) ToLocalProfile(i *models.ProfileDataOutput) (o *LocalProfile) {
	if i == nil {
		return
	}
	o = &LocalProfile{i}
	return
}

func (s *SharedServiceStruct) ToLocalIdentity(i *models.IdentityDataOutput) (o *LocalIdentity) {
	if i == nil {
		return
	}
	o = &LocalIdentity{i}
	return
}

func (s *SharedServiceStruct) ToLocalIdentities(ia []*models.IdentityDataOutput) (oa []*LocalIdentity) {
	for _, i := range ia {
		if i == nil {
			oa = append(oa, nil)
			continue
		}
		oa = append(oa, &LocalIdentity{i})
	}
	return
}

func (s *SharedServiceStruct) ToLocalUniqueIdentity(i *models.UniqueIdentityDataOutput) (o *LocalUniqueIdentity) {
	if i == nil {
		return
	}
	o = &LocalUniqueIdentity{i}
	return
}
