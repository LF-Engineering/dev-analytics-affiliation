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
		name        string
		input       [][]strfmt.DateTime
		expected    [][]strfmt.DateTime
		expectedErr bool
	}{
		{
			name: "empty",
		},
		{
			name:        "empty 1900",
			input:       [][]strfmt.DateTime{{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))}},
			expectedErr: true,
		},
		{
			name:        "wrong items length",
			input:       [][]strfmt.DateTime{{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))}, {strfmt.DateTime(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))}},
			expectedErr: true,
		},
		{
			name:     "single",
			input:    [][]strfmt.DateTime{{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))}},
			expected: [][]strfmt.DateTime{{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))}},
		},
		{
			name: "unchanged",
			input: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(2012, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2015, 11, 20, 0, 0, 0, 0, time.UTC))},
			},
			expected: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(2012, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2015, 11, 20, 0, 0, 0, 0, time.UTC))},
			},
		},
		{
			name: "merge 2 -> 1",
			input: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(2012, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2014, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2013, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2015, 11, 20, 0, 0, 0, 0, time.UTC))},
			},
			expected: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(2012, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2015, 11, 20, 0, 0, 0, 0, time.UTC))},
			},
		},
		{
			name: "merge 3 -> 2",
			input: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(2012, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2015, 11, 20, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2015, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2017, 11, 20, 0, 0, 0, 0, time.UTC))},
			},
			expected: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(2012, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2017, 11, 20, 0, 0, 0, 0, time.UTC))},
			},
		},
		{
			name: "merge 4 -> 2",
			input: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(2010, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2012, 12, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2012, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2015, 11, 20, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2015, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2017, 11, 20, 0, 0, 0, 0, time.UTC))},
			},
			expected: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(2010, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2017, 11, 20, 0, 0, 0, 0, time.UTC))},
			},
		},
		{
			name: "merge 4 -> 2 (from 1900)",
			input: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2012, 12, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2012, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2015, 11, 20, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2015, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2017, 11, 20, 0, 0, 0, 0, time.UTC))},
			},
			expected: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2017, 11, 20, 0, 0, 0, 0, time.UTC))},
			},
		},
		{
			name: "merge 4 -> 2 (to 2100)",
			input: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(2010, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2012, 12, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2012, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2015, 11, 20, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2015, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))},
			},
			expected: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(2010, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))},
			},
		},
		{
			name: "merge 4 -> 2 (from 1900 to 2100)",
			input: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2012, 12, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2012, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2015, 11, 20, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2015, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))},
			},
			expected: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))},
			},
		},
		{
			name: "merge 4 -> 2 shuffled",
			input: [][]strfmt.DateTime{
				{
					strfmt.DateTime(time.Date(2017, 11, 20, 0, 0, 0, 0, time.UTC)),
					strfmt.DateTime(time.Date(2015, 4, 15, 0, 0, 0, 0, time.UTC)),
				},
				{
					strfmt.DateTime(time.Date(2015, 11, 20, 0, 0, 0, 0, time.UTC)),
					strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)),
				},
				{
					strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC)),
					strfmt.DateTime(time.Date(2012, 8, 1, 0, 0, 0, 0, time.UTC)),
				},
				{
					strfmt.DateTime(time.Date(2012, 12, 15, 0, 0, 0, 0, time.UTC)),
					strfmt.DateTime(time.Date(2010, 8, 1, 0, 0, 0, 0, time.UTC)),
				},
			},
			expected: [][]strfmt.DateTime{
				{strfmt.DateTime(time.Date(2010, 8, 1, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2013, 10, 15, 0, 0, 0, 0, time.UTC))},
				{strfmt.DateTime(time.Date(2014, 4, 15, 0, 0, 0, 0, time.UTC)), strfmt.DateTime(time.Date(2017, 11, 20, 0, 0, 0, 0, time.UTC))},
			},
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
				"test number %d (%s), expected error %v, got %v, test case: {input:%v, expected:%v, expectedErr:%v}",
				index+1, test.name, expectedErr, gotErr, test.input, test.expected, test.expectedErr,
			)
		}
		if !equalDates(got, expected) {
			t.Errorf(
				"test number %d (%s), expected '%v', got '%v', test case: {input:%v, expected:%v, expectedErr:%v}",
				index+1, test.name, expected, got, test.input, test.expected, test.expectedErr,
			)
		}
	}
}
