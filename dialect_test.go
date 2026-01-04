package migrator

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestDialect_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dialect  Dialect
		expected string
	}{
		{"PostgreSQL", DialectPostgreSQL, "PostgreSQL"},
		{"MySQL", DialectMySQL, "MySQL"},
		{"SQLite", DialectSQLite, "SQLite"},
		{"Unknown", DialectUnknown, "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.dialect.String(); got != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, got)
			}
		})
	}
}

func TestDetectDialect_SQLite(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	dialect := detectDialect(db)
	if dialect != DialectSQLite {
		t.Errorf("expected DialectSQLite, got %v", dialect)
	}
}

func TestDialect_Placeholder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dialect  Dialect
		query    string
		expected string
	}{
		{
			name:     "PostgreSQL single param",
			dialect:  DialectPostgreSQL,
			query:    "SELECT * FROM users WHERE id = ?",
			expected: "SELECT * FROM users WHERE id = $1",
		},
		{
			name:     "PostgreSQL multiple params",
			dialect:  DialectPostgreSQL,
			query:    "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
			expected: "INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
		},
		{
			name:     "PostgreSQL no params",
			dialect:  DialectPostgreSQL,
			query:    "SELECT * FROM users",
			expected: "SELECT * FROM users",
		},
		{
			name:     "MySQL unchanged",
			dialect:  DialectMySQL,
			query:    "INSERT INTO users (name, email) VALUES (?, ?)",
			expected: "INSERT INTO users (name, email) VALUES (?, ?)",
		},
		{
			name:     "SQLite unchanged",
			dialect:  DialectSQLite,
			query:    "INSERT INTO users (name, email) VALUES (?, ?)",
			expected: "INSERT INTO users (name, email) VALUES (?, ?)",
		},
		{
			name:     "PostgreSQL complex query",
			dialect:  DialectPostgreSQL,
			query:    "UPDATE users SET name = ?, email = ? WHERE id = ? AND active = ?",
			expected: "UPDATE users SET name = $1, email = $2 WHERE id = $3 AND active = $4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.dialect.placeholder(tt.query)
			if result != tt.expected {
				t.Errorf("\nexpected: %s\ngot:      %s", tt.expected, result)
			}
		})
	}
}

func TestDialect_RebindQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		dialect       Dialect
		query         string
		args          []any
		expectedQuery string
		expectedArgs  []any
	}{
		{
			name:          "PostgreSQL with args",
			dialect:       DialectPostgreSQL,
			query:         "SELECT * FROM users WHERE id = ?",
			args:          []any{1},
			expectedQuery: "SELECT * FROM users WHERE id = $1",
			expectedArgs:  []any{1},
		},
		{
			name:          "MySQL unchanged",
			dialect:       DialectMySQL,
			query:         "SELECT * FROM users WHERE id = ?",
			args:          []any{1},
			expectedQuery: "SELECT * FROM users WHERE id = ?",
			expectedArgs:  []any{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			query, args := tt.dialect.rebindQuery(tt.query, tt.args...)
			if query != tt.expectedQuery {
				t.Errorf("expected query %s, got %s", tt.expectedQuery, query)
			}
			if len(args) != len(tt.expectedArgs) {
				t.Errorf("expected %d args, got %d", len(tt.expectedArgs), len(args))
			}
		})
	}
}

func BenchmarkDialect_Placeholder_PostgreSQL(b *testing.B) {
	dialect := DialectPostgreSQL
	query := "INSERT INTO users (name, email, age, city, country) VALUES (?, ?, ?, ?, ?)"

	for b.Loop() {
		_ = dialect.placeholder(query)
	}
}

func BenchmarkDialect_Placeholder_MySQL(b *testing.B) {
	dialect := DialectMySQL
	query := "INSERT INTO users (name, email, age, city, country) VALUES (?, ?, ?, ?, ?)"

	for b.Loop() {
		_ = dialect.placeholder(query)
	}
}
