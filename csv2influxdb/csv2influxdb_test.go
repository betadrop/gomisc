package main

import "testing"

func TestParsing(t *testing.T) {
	var okTests = []struct {
		s string
		i info
	}{
		{"CSGN_2010-12-16.csv", info{"CSGN", 2010, 12, 16, false}},
		{"CSGN_2010-12-16.csv.gz", info{"CSGN", 2010, 12, 16, true}},
		{"CSGNji_2000-12-01.csv.gz", info{"CSGNji", 2000, 12, 1, true}},
	}
	for _, test := range okTests {
		i, err := parse(test.s)
		if err != nil {
			t.Error(err)
		}
		if i != test.i {
			t.Error("expected:", test.i, " got:", i)
		}
	}
	var errTests = []struct {
		s string
	}{
		{"CSGN*2010-12-16.csv"},
		{"CSGN-2010*12-16.csv"},
		{"CSGN-2010-12*16.csv"},
		{"CSGN_2010-12-16csv"},
		{"CSGN_2010-12-16.csv.sz"},
		{"CSGN_2010-12-16.csv."},
	}
	for _, test := range errTests {
		_, err := parse(test.s)
		if err == nil {
			t.Error("expected failure for", test.s)
		}
	}
}
