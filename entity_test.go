package entity

import (
	"context"
	"testing"
	"time"
)

func TestMetadata(t *testing.T) {
	_, err := NewMetadata(&EmptyEntity{})
	if err == nil {
		t.Fatalf(`EmptyEntity metadata, Expected="empty empty", Actual=nil`)
	}

	_, err = NewMetadata(&NoPrimaryKeyEntity{})
	if err == nil {
		t.Fatalf(`NoPrimaryKeyEntity metadata, Expected="primary key undefined", Actual=nil`)
	}

	md, err := NewMetadata(&GenernalEntity{})
	if err != nil {
		t.Fatalf(`GenernalEntity metadata, Expected=nil, Actual=%q`, err.Error())
	}

	if n := len(md.PrimaryKeys); n != 2 {
		t.Fatalf(`GenernalEntity metadata primary key, Expected=1, Actual=%d`, n)
	} else if n := len(md.Columns); n != 5 {
		t.Fatalf(`GenernalEntity metadata columns, Expected=4, Actual=%d`, n)
	} else if v := (&GenernalEntity{}).TableName(); md.TableName != v {
		t.Fatalf(`GenernalEntity metadata tablename, Expected=%q, Actual=%q`, v, md.TableName)
	}
}

func TestColumns(t *testing.T) {
	cases := map[string]struct {
		primaryKey    bool
		refuseUpdate  bool
		autoIncrement bool
		returning     bool
	}{
		"id": {
			primaryKey:    true,
			autoIncrement: true,
			refuseUpdate:  true,
		},
		"id2": {
			primaryKey:   true,
			refuseUpdate: true,
		},
		"name": {},
		"create_at": {
			refuseUpdate: true,
		},
		"version": {
			returning: true,
		},
	}

	columns, err := getColumns(&GenernalEntity{})
	if err != nil {
		t.Fatalf("GenernalEntity column error, Expected=nil, Actual=%s", err.Error())
	}

	for _, col := range columns {
		expected := cases[col.DBField]

		if expected.primaryKey != col.PrimaryKey {
			t.Fatalf("GenernalEntity column %q PrimaryKey, Expected=%v, Actual=%v", col.DBField, expected.primaryKey, col.PrimaryKey)
		} else if expected.refuseUpdate != col.RefuseUpdate {
			t.Fatalf("GenernalEntity column %q RefuseUpdate, Expected=%v, Actual=%v", col.DBField, expected.refuseUpdate, col.RefuseUpdate)
		} else if expected.autoIncrement != col.AutoIncrement {
			t.Fatalf("GenernalEntity column %q AutoIncrement, Expected=%v, Actual=%v", col.DBField, expected.autoIncrement, col.AutoIncrement)
		} else if expected.returning != col.Returning {
			t.Fatalf("GenernalEntity column %q Returning, Expected=%v, Actual=%v", col.DBField, expected.returning, col.Returning)
		}
	}

	_, err = getColumns(GenernalEntity{})
	t.Log(err)
	if err == nil {
		t.Fatal(`GenernalEntity column, Expected non-pointer error, Actual=nil`)
	}
}

type GenernalEntity struct {
	ID             int       `db:"id" entity:"primaryKey,autoIncrement"`
	ID2            int       `db:"id2" entity:"primaryKey"`
	Name           string    `db:"name"`
	CreateAt       time.Time `db:"create_at" entity:"refuseUpdate"`
	Version        int       `db:"version" entity:"returning"`
	Deprecated     bool      `db:"deprecated" entity:"deprecated"`
	ExplicitIgnore bool      `db:"-"`
	ImplicitIgnore bool
}

func (ge GenernalEntity) TableName() string {
	return "genernal"
}

func (ge GenernalEntity) OnEntityEvent(ctx context.Context, ev Event) error {
	return nil
}

type EmptyEntity struct {
	ID   int
	Name string
}

func (ee EmptyEntity) TableName() string {
	return "emtpy"
}

func (ee *EmptyEntity) OnEntityEvent(ctx context.Context, ev Event) error {
	return nil
}

type NoPrimaryKeyEntity struct {
	ID   int    `db:"int"`
	Name string `db:"name"`
}

func (npe NoPrimaryKeyEntity) TableName() string {
	return "no_primary_key"
}

func (npe *NoPrimaryKeyEntity) OnEntityEvent(ctx context.Context, ev Event) error {
	return nil
}
