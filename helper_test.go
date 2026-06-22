package entity

import (
	"reflect"
	"strings"
	"testing"

	goqu "github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
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

func TestNewUpsertRecord(t *testing.T) {
	// GenernalEntity columns:
	//   id, id2          -> primaryKey/autoIncrement, RefuseUpdate=true (excluded)
	//   create_at        -> refuseUpdate (excluded)
	//   version          -> returning (RefuseUpdate=true, excluded)
	//   name, extra      -> included
	ent := &GenernalEntity{}
	record := NewUpsertRecord(ent, "custom_col")

	// only updatable columns + otherColumns make it into the record
	wantKeys := []string{"name", "extra", "custom_col"}
	if len(record) != len(wantKeys) {
		t.Fatalf("record keys count = %d, want %d; record=%+v", len(record), len(wantKeys), record)
	}
	for _, k := range wantKeys {
		if _, ok := record[k]; !ok {
			t.Fatalf("missing record key %q; record=%+v", k, record)
		}
	}

	// rendered SQL must use raw EXCLUDED. prefix and quote the column per dialect
	for _, dialect := range []string{"postgres", "sqlite3"} {
		sqlStr, _, err := goqu.Dialect(dialect).
			Insert("tbl").
			Rows(goqu.Record{"id": 1}).
			OnConflict(goqu.DoUpdate("id", record)).
			ToSQL()
		if err != nil {
			t.Fatalf("dialect %s: ToSQL error: %v", dialect, err)
		}

		// EXCLUDED must NOT be quoted (would break PG on conflict pseudo-table)
		if strings.Contains(sqlStr, `"EXCLUDED"`) || strings.Contains(sqlStr, "`EXCLUDED`") {
			t.Fatalf("dialect %s: EXCLUDED is quoted in SQL: %s", dialect, sqlStr)
		}

		// updatable columns must be referenced via EXCLUDED.<col>
		plain := strings.NewReplacer("\"", "", "`", "").Replace(sqlStr)
		for _, k := range []string{"name", "extra", "custom_col"} {
			if !strings.Contains(plain, "EXCLUDED."+k) {
				t.Fatalf("dialect %s: missing EXCLUDED.%s in SQL: %s", dialect, k, sqlStr)
			}
		}
	}
}
