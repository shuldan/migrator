package migrator

import (
	"errors"
	"strings"
	"testing"
)

func TestMigrationError_Error_WithShortQuery(t *testing.T) {
	t.Parallel()
	e := &MigrationError{
		MigrationID: "001",
		Phase:       "up",
		Query:       "CREATE TABLE t (id INT);",
		Err:         errors.New("syntax error"),
	}
	got := e.Error()
	if !strings.Contains(got, `"001"`) {
		t.Errorf("expected migration id in output, got %q", got)
	}
	if !strings.Contains(got, "up") {
		t.Errorf("expected phase in output, got %q", got)
	}
	if !strings.Contains(got, "CREATE TABLE") {
		t.Errorf("expected query in output, got %q", got)
	}
}

func TestMigrationError_Error_WithLongQuery(t *testing.T) {
	t.Parallel()
	longQuery := strings.Repeat("A", 200)
	e := &MigrationError{
		MigrationID: "002",
		Phase:       "down",
		Query:       longQuery,
		Err:         errors.New("fail"),
	}
	got := e.Error()
	if !strings.Contains(got, "...") {
		t.Errorf("expected truncated query with '...', got %q", got)
	}
}

func TestMigrationError_Error_WithoutQuery(t *testing.T) {
	t.Parallel()
	e := &MigrationError{
		MigrationID: "003",
		Phase:       "up",
		Query:       "",
		Err:         errors.New("some error"),
	}
	got := e.Error()
	if strings.Contains(got, "[query:") {
		t.Errorf("expected no query in output, got %q", got)
	}
	if !strings.Contains(got, `"003"`) || !strings.Contains(got, "some error") {
		t.Errorf("expected id and error in output, got %q", got)
	}
}

func TestMigrationError_Unwrap(t *testing.T) {
	t.Parallel()
	inner := errors.New("inner error")
	e := &MigrationError{
		MigrationID: "004",
		Phase:       "up",
		Err:         inner,
	}
	if !errors.Is(e, inner) {
		t.Errorf("expected Unwrap to return inner error")
	}
}

func TestMigrationError_Unwrap_WrappedSentinel(t *testing.T) {
	t.Parallel()
	e := &MigrationError{
		MigrationID: "005",
		Phase:       "down",
		Err:         ErrIrreversibleMigration,
	}
	if !errors.Is(e, ErrIrreversibleMigration) {
		t.Errorf("expected errors.Is to match ErrIrreversibleMigration")
	}
}

func TestMigrationError_Error_QueryExactly120(t *testing.T) {
	t.Parallel()
	q := strings.Repeat("X", 120)
	e := &MigrationError{MigrationID: "006", Phase: "up", Query: q, Err: errors.New("e")}
	got := e.Error()
	if strings.Contains(got, "...") {
		t.Errorf("query of exactly 120 chars should not be truncated, got %q", got)
	}
}

func TestMigrationError_Error_QueryExactly121(t *testing.T) {
	t.Parallel()
	q := strings.Repeat("X", 121)
	e := &MigrationError{MigrationID: "007", Phase: "up", Query: q, Err: errors.New("e")}
	got := e.Error()
	if !strings.Contains(got, "...") {
		t.Errorf("query of 121 chars should be truncated, got %q", got)
	}
}
