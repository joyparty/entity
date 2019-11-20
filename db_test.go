package entity

import "testing"

func TestStatement(t *testing.T) {
	t.Run("select", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

		stmt := selectStatement(&GenernalEntity{}, md, "mysql")
		expected := "SELECT `id`, `id2`, `name`, `create_at`, `version` FROM genernal WHERE `id` = :id AND `id2` = :id2 LIMIT 1"
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}

		stmt = selectStatement(&GenernalEntity{}, md, "postgres")
		expected = `SELECT "id", "id2", "name", "create_at", "version" FROM genernal WHERE "id" = :id AND "id2" = :id2 LIMIT 1`
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}
	})

	t.Run("insert", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

		stmt := insertStatement(&GenernalEntity{}, md, "mysql")
		expected := "INSERT INTO genernal (`id2`, `name`) VALUES (:id2, :name) RETURNING `create_at`, `version`"
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}

		stmt = insertStatement(&GenernalEntity{}, md, "postgres")
		expected = `INSERT INTO genernal ("id2", "name") VALUES (:id2, :name) RETURNING "create_at", "version"`
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}
	})

	t.Run("update", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

		stmt := updateStatement(&GenernalEntity{}, md, "mysql")
		expected := "UPDATE genernal SET `name` = :name WHERE `id` = :id AND `id2` = :id2 RETURNING `version`"
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}

		stmt = updateStatement(&GenernalEntity{}, md, "postgres")
		expected = `UPDATE genernal SET "name" = :name WHERE "id" = :id AND "id2" = :id2 RETURNING "version"`
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}
	})

	t.Run("delete", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

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
