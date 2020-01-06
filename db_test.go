package entity

import (
	"sort"
	"testing"
)

func TestStatement(t *testing.T) {
	t.Run("select", func(t *testing.T) {
		md, _ := newTestMetadata(&GenernalEntity{})

		stmt := selectStatement(&GenernalEntity{}, md, "mysql")
		expected := "SELECT `create_at`, `extra`, `id`, `id2`, `name`, `version` FROM genernal WHERE `id` = :id AND `id2` = :id2 LIMIT 1"
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}

		stmt = selectStatement(&GenernalEntity{}, md, "postgres")
		expected = `SELECT "create_at", "extra", "id", "id2", "name", "version" FROM genernal WHERE "id" = :id AND "id2" = :id2 LIMIT 1`
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}
	})

	t.Run("insert", func(t *testing.T) {
		md, _ := newTestMetadata(&GenernalEntity{})

		stmt := insertStatement(&GenernalEntity{}, md, "mysql")
		expected := "INSERT INTO genernal (`extra`, `id2`, `name`) VALUES (:extra, :id2, :name) RETURNING `create_at`, `version`"
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}

		stmt = insertStatement(&GenernalEntity{}, md, "postgres")
		expected = `INSERT INTO genernal ("extra", "id2", "name") VALUES (:extra, :id2, :name) RETURNING "create_at", "version"`
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}
	})

	t.Run("update", func(t *testing.T) {
		md, _ := newTestMetadata(&GenernalEntity{})

		stmt := updateStatement(&GenernalEntity{}, md, "mysql")
		expected := "UPDATE genernal SET `extra` = :extra, `name` = :name WHERE `id` = :id AND `id2` = :id2 RETURNING `version`"
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}

		stmt = updateStatement(&GenernalEntity{}, md, "postgres")
		expected = `UPDATE genernal SET "extra" = :extra, "name" = :name WHERE "id" = :id AND "id2" = :id2 RETURNING "version"`
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}
	})

	t.Run("delete", func(t *testing.T) {
		md, _ := newTestMetadata(&GenernalEntity{})

		stmt := deleteStatement(&GenernalEntity{}, md, "mysql")
		expected := "DELETE FROM genernal WHERE `id` = :id AND `id2` = :id2"
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}

		stmt = deleteStatement(&GenernalEntity{}, md, "postgres")
		expected = `DELETE FROM genernal WHERE "id" = :id AND "id2" = :id2`
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
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
			driver:   "mysql",
			column:   "id",
			expected: "`id`",
		},
		{
			driver:   "postgres",
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
