package entity

import "testing"

func TestStatement(t *testing.T) {
	t.Run("select", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

		dia := &dialect{Driver: "mysql", Returning: false}
		stmt := selectStatement(&GenernalEntity{}, md, dia)
		expected := "SELECT `id`, `id2`, `name`, `create_at`, `version` FROM genernal WHERE `id` = :id AND `id2` = :id2 LIMIT 1"
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}

		dia = &dialect{Driver: "postgres", Returning: true}
		stmt = selectStatement(&GenernalEntity{}, md, dia)
		expected = `SELECT "id", "id2", "name", "create_at", "version" FROM genernal WHERE "id" = :id AND "id2" = :id2 LIMIT 1`
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}
	})

	t.Run("insert", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

		dia := &dialect{Driver: "mysql", Returning: false}
		stmt := insertStatement(&GenernalEntity{}, md, dia)
		expected := "INSERT INTO genernal (`id2`, `name`, `create_at`, `version`) VALUES (:id2, :name, :create_at, :version)"
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}

		dia = &dialect{Driver: "postgres", Returning: true}
		stmt = insertStatement(&GenernalEntity{}, md, dia)
		expected = `INSERT INTO genernal ("id2", "name", "create_at") VALUES (:id2, :name, :create_at) RETURNING "version"`
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}
	})

	t.Run("update", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

		dia := &dialect{Driver: "mysql", Returning: false}
		stmt := updateStatement(&GenernalEntity{}, md, dia)
		expected := "UPDATE genernal SET `name` = :name, `version` = :version WHERE `id` = :id AND `id2` = :id2"
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}

		dia = &dialect{Driver: "postgres", Returning: true}
		stmt = updateStatement(&GenernalEntity{}, md, dia)
		expected = `UPDATE genernal SET "name" = :name WHERE "id" = :id AND "id2" = :id2 RETURNING "version"`
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}
	})

	t.Run("delete", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

		dia := &dialect{Driver: "mysql", Returning: false}
		stmt := deleteStatement(&GenernalEntity{}, md, dia)
		expected := "DELETE FROM genernal WHERE `id` = :id AND `id2` = :id2"
		if stmt != expected {
			t.Fatalf("GenernalEntity, Expected=%s, Actual=%s", expected, stmt)
		}

		dia = &dialect{Driver: "postgres", Returning: true}
		stmt = deleteStatement(&GenernalEntity{}, md, dia)
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
		dia := &dialect{Driver: test.driver}

		if actual := quoteColumn(test.column, dia); actual != test.expected {
			t.Fatalf("%q quote column, Expected=%v, Actual=%v", test.driver, test.expected, actual)
		}
	}
}
