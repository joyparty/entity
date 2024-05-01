package entity

import (
	"reflect"
	"testing"
	"time"

	"github.com/jmoiron/sqlx/reflectx"
)

func TestMetadata(t *testing.T) {
	t.Run("NewMetadata", func(t *testing.T) {
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
			t.Fatalf(`GenernalEntity metadata primary key, Expected=2, Actual=%d`, n)
		} else if n := len(md.Columns); n != 6 {
			t.Fatalf(`GenernalEntity metadata columns, Expected=6, Actual=%d`, n)
		} else if v := (&GenernalEntity{}).TableName(); md.TableName != v {
			t.Fatalf(`GenernalEntity metadata tablename, Expected=%q, Actual=%q`, v, md.TableName)
		}
	})

	t.Run("getMetadata", func(t *testing.T) {
		md1, err := getMetadata(&GenernalEntity{})
		if err != nil {
			t.Fatalf("getMetadata(), %v", err)
		}

		md2, err := getMetadata(&GenernalEntity{})
		if err != nil {
			t.Fatalf("getMetadata(), %v", err)
		}

		if !reflect.DeepEqual(md1, md2) {
			t.Fatal("different metadata")
		}
	})
}

func TestColumns(t *testing.T) {
	cases := map[string]struct {
		primaryKey      bool
		refuseUpdate    bool
		autoIncrement   bool
		returningInsert bool
		returningUpdate bool
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
			refuseUpdate:    true,
			returningInsert: true,
		},
		"version": {
			returningInsert: true,
			returningUpdate: true,
			refuseUpdate:    true,
		},
		"extra": {},
	}

	for _, col := range getColumns(&GenernalEntity{}) {
		expected, ok := cases[col.DBField]
		if !ok {
			t.Fatalf("got column '%s' that expections does not point out", col.DBField)
		}

		if expected.primaryKey != col.PrimaryKey {
			t.Fatalf("GenernalEntity column %q PrimaryKey, Expected=%v, Actual=%v", col.DBField, expected.primaryKey, col.PrimaryKey)
		} else if expected.refuseUpdate != col.RefuseUpdate {
			t.Fatalf("GenernalEntity column %q RefuseUpdate, Expected=%v, Actual=%v", col.DBField, expected.refuseUpdate, col.RefuseUpdate)
		} else if expected.autoIncrement != col.AutoIncrement {
			t.Fatalf("GenernalEntity column %q AutoIncrement, Expected=%v, Actual=%v", col.DBField, expected.autoIncrement, col.AutoIncrement)
		} else if expected.returningInsert != col.ReturningInsert {
			t.Fatalf("GenernalEntity column %q ReturningInsert, Expected=%v, Actual=%v", col.DBField, expected.returningInsert, col.ReturningInsert)
		} else if expected.returningUpdate != col.ReturningUpdate {
			t.Fatalf("GenernalEntity column %q ReturningUpdate, Expected=%v, Actual=%v", col.DBField, expected.returningUpdate, col.ReturningUpdate)
		}
	}
}

func TestFields(t *testing.T) {
	type NestExtend struct {
		Foo string `db:"foo"`
		Bar bool   `db:"bar"`
	}

	type MoreNestExtend struct {
		Foo int `db:"foo"`
	}

	type Other struct {
		Baz string `json:"baz"`
	}

	type Extend struct {
		NestExtend
		MoreNestExtend
		Name string `db:"name"`
	}

	type Row struct {
		Extend
		ID    int   `db:"id,primaryKey"`
		Other Other `db:"other"`
	}

	vt := reflectx.Deref(reflect.TypeOf(&Row{}))
	tm := reflectx.NewMapper("db").TypeMap(vt)

	fs := map[string]*reflectx.FieldInfo{}
	for _, v := range getFields(tm.Tree) {
		fs[v.Name] = v
	}

	expected := map[string]struct {
		TypeKind reflect.Kind
	}{
		"id": {
			TypeKind: reflect.Int,
		},
		"name": {
			TypeKind: reflect.String,
		},
		"other": {
			TypeKind: reflect.Struct,
		},
		"foo": {
			TypeKind: reflect.Int,
		},
		"bar": {
			TypeKind: reflect.Bool,
		},
	}

	if len(fs) != len(expected) {
		t.Fatalf("fields count, Expected=%d, Actual=%d", len(expected), len(fs))
	}

	for name, info := range expected {
		fi, ok := fs[name]
		if !ok {
			t.Fatalf("fields(), %q not found", name)
		}

		if fi.Field.Type.Kind() != info.TypeKind {
			t.Fatalf("fields() %q type, Expected=%s, Actual=%s", name, info.TypeKind, fi.Field.Type.Kind())
		}
	}
}

type TestExtra struct {
	E1 string `json:"e1"`
	E2 int    `json:"e2"`
}

type GenernalEntity struct {
	ID             int       `db:"id,primaryKey,autoIncrement"`
	ID2            int       `db:"id2,primaryKey"`
	Name           string    `db:"name"`
	CreateAt       time.Time `db:"create_at,refuseUpdate,returningInsert"`
	Version        int       `db:"version,returning"`
	Extra          TestExtra `db:"extra"`
	ExplicitIgnore bool      `db:"-"`
}

func (ge GenernalEntity) TableName() string {
	return "genernal"
}

type EmptyEntity struct {
	ID   int    `db:"-"`
	Name string `db:"-"`
}

func (ee EmptyEntity) TableName() string {
	return "emtpy"
}

type NoPrimaryKeyEntity struct {
	ID   int    `db:"int"`
	Name string `db:"name"`
}

func (npe NoPrimaryKeyEntity) TableName() string {
	return "no_primary_key"
}
