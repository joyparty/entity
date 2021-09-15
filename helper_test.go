package entity

import (
	"reflect"
	"testing"
)

func TestPagination(t *testing.T) {
	cases := []struct {
		Actual   Pagination
		Excepted Pagination
	}{
		{
			Actual: Pagination{
				First:   1,
				Last:    1,
				Current: 1,
				Size:    10,
				Items:   0,
			},
			Excepted: NewPagination(1, 10, 0),
		},
		{
			Actual: Pagination{
				First:   1,
				Last:    1,
				Current: 1,
				Size:    10,
				Items:   9,
			},
			Excepted: NewPagination(1, 10, 9),
		},
		{
			Actual: Pagination{
				First:   1,
				Last:    2,
				Current: 1,
				Next:    2,
				Size:    10,
				Items:   11,
			},
			Excepted: NewPagination(1, 10, 11),
		},
		{
			Actual: Pagination{
				First:    1,
				Last:     3,
				Previous: 1,
				Current:  2,
				Next:     3,
				Size:     10,
				Items:    21,
			},
			Excepted: NewPagination(2, 10, 21),
		},
		{
			Actual:   NewPagination(3, 10, 21),
			Excepted: NewPagination(4, 10, 21),
		},
	}

	for _, c := range cases {
		if !reflect.DeepEqual(c.Actual, c.Excepted) {
			t.Fatalf("Pagination, Actual=%+v, Excepted=%+v\n", c.Actual, c.Excepted)
		}
	}
}
