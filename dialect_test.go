package migrator

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type postgresTestDriver struct{}

func (d *postgresTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("not implemented")
}

type mysqlTestDriver struct{}

func (d *mysqlTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("not implemented")
}

type unknownTestDriver struct{}

func (d *unknownTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("not implemented")
}

func init() {
	sql.Register("fake_postgres_driver", &postgresTestDriver{})
	sql.Register("fake_mysql_driver", &mysqlTestDriver{})
	sql.Register("fake_unknown_driver", &unknownTestDriver{})
}

func TestDialect_String_AllCases(t *testing.T) {
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
		{"InvalidValue", Dialect(99), "Unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.dialect.String()
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestDialect_QuoteIdentifier_AllDialects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		dialect  Dialect
		input    string
		expected string
	}{
		{"MySQL_simple", DialectMySQL, "col", "`col`"},
		{"MySQL_backtick", DialectMySQL, "co`l", "`co``l`"},
		{"Postgres_simple", DialectPostgreSQL, "col", `"col"`},
		{"Postgres_quote", DialectPostgreSQL, `co"l`, `"co""l"`},
		{"SQLite_simple", DialectSQLite, "col", `"col"`},
		{"Unknown_simple", DialectUnknown, "col", `"col"`},
		{"MySQL_empty", DialectMySQL, "", "``"},
		{"Postgres_empty", DialectPostgreSQL, "", `""`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.dialect.QuoteIdentifier(tt.input)
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestDetectDialect_SQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	got := detectDialect(db)
	if got != DialectSQLite {
		t.Errorf("expected DialectSQLite, got %v", got)
	}
}

func TestDetectDialect_PostgreSQL(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("fake_postgres_driver", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	got := detectDialect(db)
	if got != DialectPostgreSQL {
		t.Errorf("expected DialectPostgreSQL, got %v", got)
	}
}

func TestDetectDialect_MySQL(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("fake_mysql_driver", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	got := detectDialect(db)
	if got != DialectMySQL {
		t.Errorf("expected DialectMySQL, got %v", got)
	}
}

func TestDetectDialect_Unknown(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("fake_unknown_driver", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	got := detectDialect(db)
	if got != DialectUnknown {
		t.Errorf("expected DialectUnknown, got %v", got)
	}
}

func TestDialect_Placeholder_AllCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		dialect  Dialect
		query    string
		expected string
	}{
		{"NonPostgres_passthrough", DialectMySQL, "SELECT ? FROM t WHERE id = ?", "SELECT ? FROM t WHERE id = ?"},
		{"Postgres_single", DialectPostgreSQL, "SELECT ?", "SELECT $1"},
		{"Postgres_multiple", DialectPostgreSQL, "INSERT INTO t VALUES (?, ?, ?)", "INSERT INTO t VALUES ($1, $2, $3)"},
		{"Postgres_no_placeholder", DialectPostgreSQL, "SELECT 1", "SELECT 1"},
		{"Postgres_empty", DialectPostgreSQL, "", ""},
		{"SQLite_passthrough", DialectSQLite, "SELECT ?", "SELECT ?"},
		{"Unknown_passthrough", DialectUnknown, "SELECT ?", "SELECT ?"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.dialect.placeholder(tt.query)
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}
