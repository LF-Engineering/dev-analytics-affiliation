package main

import (
	"fmt"
	"testing"
  "time"

	"github.com/go-openapi/strfmt"

	"github.com/LF-Engineering/dev-analytics-affiliation/shdb"
)

func equalDates(a1, a2 [][]strfmt.DateTime) bool {
	if len(a1) != len(a2) {
		fmt.Printf("different lengths %d != %d\n", len(a1), len(a2))
		return false
	}
	for i := range a1 {
		b1 := a1[i]
		b2 := a2[i]
		if len(b1) != len(b2) {
			fmt.Printf("different #%d element lengths: %d != %d\n", i, len(b1), len(b2))
			return false
		}
		for j := range b1 {
			if b1[j] != b2[j] {
				fmt.Printf("different (#%d, #%d) elements: %v != %v\n", i, j, b1[j], b2[j])
				return false
			}
		}
	}
	return true
}

func TestMergeDateRanges(t *testing.T) {
	var testCases = []struct {
		input       [][]strfmt.DateTime
		expected    [][]strfmt.DateTime
		expectedErr bool
	}{
		{},
    //[2012-08-01T00:00:00.000Z 2013-10-15T00:00:00.000Z] [2014-04-15T00:00:00.000Z 2015-11-20T00:00:00.000Z]
		{
			input:       [][]strfmt.DateTime{{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))}},
      expectedErr: true,
		},
		{
			input:    [][]strfmt.DateTime{{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))},{strfmt.DateTime(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))}},
      expectedErr: true,
		},
		{
			input:    [][]strfmt.DateTime{{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)),strfmt.DateTime(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))}},
			expected: [][]strfmt.DateTime{{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)),strfmt.DateTime(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))}},
		},
	}
	s := shdb.New(nil)
	for index, test := range testCases {
		expected := test.expected
		expectedErr := test.expectedErr
		got, err := s.MergeDateRanges(test.input)
    gotErr := err != nil
		if gotErr != expectedErr {
			t.Errorf(
          "test number %d, expected error %v, got %v, test case: {input:%v, expected:%v, expectedErr:%v}",
				index+1, expectedErr, gotErr, test.input, test.expected, test.expectedErr,
			)
		}
		if !equalDates(got, expected) {
			t.Errorf(
				"test number %d, expected '%v', got '%v', test case: {input:%v, expected:%v, expectedErr:%v}",
				index+1, expectedErr, gotErr, test.input, test.expected, test.expectedErr,
			)
		}
	}
}
