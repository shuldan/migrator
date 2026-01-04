package migrator

import (
	"database/sql"
	"fmt"
	"strings"
)

type Dialect int

const (
	DialectUnknown Dialect = iota
	DialectPostgreSQL
	DialectMySQL
	DialectSQLite
)

func (d Dialect) String() string {
	switch d {
	case DialectPostgreSQL:
		return "PostgreSQL"
	case DialectMySQL:
		return "MySQL"
	case DialectSQLite:
		return "SQLite"
	default:
		return "Unknown"
	}
}

func detectDialect(db *sql.DB) Dialect {
	driver := fmt.Sprintf("%T", db.Driver())

	switch {
	case strings.Contains(driver, "postgres"), strings.Contains(driver, "pq"), strings.Contains(driver, "pgx"):
		return DialectPostgreSQL
	case strings.Contains(driver, "mysql"):
		return DialectMySQL
	case strings.Contains(driver, "sqlite"):
		return DialectSQLite
	default:
		return DialectUnknown
	}
}

func (d Dialect) placeholder(query string) string {
	if d != DialectPostgreSQL {
		return query
	}

	var result strings.Builder
	paramNum := 1

	for _, ch := range query {
		if ch == '?' {
			fmt.Fprintf(&result, "$%d", paramNum)
			paramNum++
		} else {
			result.WriteRune(ch)
		}
	}

	return result.String()
}

func (d Dialect) rebindQuery(query string, args ...any) (string, []any) {
	return d.placeholder(query), args
}
