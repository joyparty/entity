package entity

import (
	"sort"
	"testing"
)

func TestStatement(t *testing.T) {
	t.Run("newStatement", func(t *testing.T) {
		t.Run("select", func(t *testing.T) {
			md, _ := newTestMetadata(&GenernalEntity{})

			stmt := newSelectStatement(md, driverMysql)
			expected := "SELECT `create_at`, `extra`, `id`, `id2`, `name`, `version` FROM `genernal` WHERE `id` = :id AND `id2` = :id2 LIMIT 1"
			if stmt != expected {
				t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
			}

			stmt = newSelectStatement(md, driverPostgres)
			expected = `SELECT "create_at", "extra", "id", "id2", "name", "version" FROM "genernal" WHERE "id" = :id AND "id2" = :id2 LIMIT 1`
			if stmt != expected {
				t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
			}
		})

		t.Run("insert", func(t *testing.T) {
			md, _ := newTestMetadata(&GenernalEntity{})

			stmt := newInsertStatement(md, driverMysql)
			expected := "INSERT INTO `genernal` (`extra`, `id2`, `name`) VALUES (:extra, :id2, :name) RETURNING `create_at`, `version`"
			if stmt != expected {
				t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
			}

			stmt = newInsertStatement(md, driverPostgres)
			expected = `INSERT INTO "genernal" ("extra", "id2", "name") VALUES (:extra, :id2, :name) RETURNING "create_at", "version"`
			if stmt != expected {
				t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
			}
		})

		t.Run("update", func(t *testing.T) {
			md, _ := newTestMetadata(&GenernalEntity{})

			stmt := newUpdateStatement(md, driverMysql)
			expected := "UPDATE `genernal` SET `extra` = :extra, `name` = :name WHERE `id` = :id AND `id2` = :id2 RETURNING `version`"
			if stmt != expected {
				t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
			}

			stmt = newUpdateStatement(md, driverPostgres)
			expected = `UPDATE "genernal" SET "extra" = :extra, "name" = :name WHERE "id" = :id AND "id2" = :id2 RETURNING "version"`
			if stmt != expected {
				t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
			}
		})

		t.Run("upsert", func(t *testing.T) {
			md, _ := newTestMetadata(&GenernalEntity{})

			stmt := newUpsertStatement(md, driverMysql)
			expected := "INSERT INTO `genernal` (`extra`, `id2`, `name`) VALUES (:extra, :id2, :name) ON CONFLICT KEY UPDATE `extra` = :extra, `name` = :name RETURNING `create_at`, `version`"
			if stmt != expected {
				t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
			}

			stmt = newUpsertStatement(md, driverPostgres)
			expected = `INSERT INTO "genernal" ("extra", "id2", "name") VALUES (:extra, :id2, :name) ON CONFLICT ("id", "id2") DO UPDATE SET "extra" = :extra, "name" = :name RETURNING "create_at", "version"`
			if stmt != expected {
				t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
			}
		})

		t.Run("delete", func(t *testing.T) {
			md, _ := newTestMetadata(&GenernalEntity{})

			stmt := newDeleteStatement(md, driverMysql)
			expected := "DELETE FROM `genernal` WHERE `id` = :id AND `id2` = :id2"
			if stmt != expected {
				t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
			}

			stmt = newDeleteStatement(md, driverPostgres)
			expected = `DELETE FROM "genernal" WHERE "id" = :id AND "id2" = :id2`
			if stmt != expected {
				t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
			}
		})
	})

	t.Run("getStatement", func(t *testing.T) {
		md, _ := getMetadata(&GenernalEntity{})

		for _, cmd := range []string{commandSelect, commandInsert, commandUpdate, commandDelete} {
			stmt1 := getStatement(cmd, md, driverPostgres)
			stmt2 := getStatement(cmd, md, driverPostgres)

			if stmt1 != stmt2 {
				t.Fatalf("different %s statement", cmd)
			}
		}
	})
}

func TestQuoteColumn(t *testing.T) {
	tests := []struct {
		driver   string
		column   string
		expected string
	}{
		{
			driver:   driverMysql,
			column:   "id",
			expected: "`id`",
		},
		{
			driver:   driverPostgres,
			column:   "id",
			expected: `"id"`,
		},
	}

	for _, test := range tests {
		if actual := quoteColumn(test.column, test.driver); actual != test.expected {
			t.Fatalf("%q quote column, Expected=%v, Actual=%v", test.driver, test.expected, actual)
		}
	}
}

func TestQuoteIdentifier(t *testing.T) {
	cases := []struct {
		driver     string
		identifier string
		expected   string
	}{
		{
			driver:     driverMysql,
			identifier: "foobar",
			expected:   "`foobar`",
		},
		{
			driver:     driverPostgres,
			identifier: "foobar",
			expected:   `"foobar"`,
		},
		{
			driver:     driverPostgres,
			identifier: "foo.bar",
			expected:   `"foo"."bar"`,
		},
		{
			driver:     driverPostgres,
			identifier: `"foo".bar`,
			expected:   `"foo"."bar"`,
		},
		{
			driver:     driverPostgres,
			identifier: `foo.*`,
			expected:   `"foo".*`,
		},
	}

	for _, c := range cases {
		if actual := quoteIdentifier(c.identifier, c.driver); actual != c.expected {
			t.Fatalf("%q quote identifier, Expected=%v, Actual=%v", c.identifier, c.expected, actual)
		}
	}
}

// 把字段排序处理一下，否则生成的sql里面的字段每次都是随机排序的
func newTestMetadata(ent Entity) (*Metadata, error) {
	md, err := NewMetadata(ent)
	if err != nil {
		return nil, err
	}

	sort.Slice(md.Columns, func(i int, j int) bool {
		return md.Columns[i].DBField < md.Columns[j].DBField
	})

	if len(md.PrimaryKeys) > 1 {
		sort.Slice(md.PrimaryKeys, func(i int, j int) bool {
			return md.PrimaryKeys[i].DBField < md.PrimaryKeys[j].DBField
		})
	}

	return md, nil
}
