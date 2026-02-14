package migrator

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestWithDialect(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	m := New(db, WithDialect(DialectMySQL))
	if m.dialect != DialectMySQL {
		t.Errorf("expected DialectMySQL, got %v", m.dialect)
	}
}

func TestWithTableName(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	m := New(db, WithTableName("my_migrations"))
	if m.tableName != "my_migrations" {
		t.Errorf("expected %q, got %q", "my_migrations", m.tableName)
	}
}

func TestWithSchema(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	m := New(db, WithSchema("public"))
	if m.schema != "public" {
		t.Errorf("expected %q, got %q", "public", m.schema)
	}
}

func TestWithLogger(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	l := &noopLogger{}
	m := New(db, WithLogger(l))
	if m.logger != l {
		t.Error("expected logger to be set")
	}
}

func TestWithLogger_Nil(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	m := New(db, WithLogger(nil))
	if m.logger == nil {
		t.Error("expected logger to not be nil when nil passed")
	}
}

func TestWithAdvisoryLock(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	m := New(db, WithDialect(DialectPostgreSQL), WithAdvisoryLock())
	if _, ok := m.lockStrategy.(*postgresAdvisoryLock); !ok {
		t.Errorf("expected postgresAdvisoryLock, got %T", m.lockStrategy)
	}
}

func TestWithLockStrategy(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	custom := &noopLock{}
	m := New(db, WithLockStrategy(custom))
	if m.lockStrategy != custom {
		t.Error("expected custom lock strategy")
	}
}

func TestWithTxStrategy(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	m := New(db, WithTxStrategy(TxDisabled))
	if m.txStrategy != TxDisabled {
		t.Errorf("expected TxDisabled, got %v", m.txStrategy)
	}
}

func TestWithBeforeMigration(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	fn := func(ctx context.Context, mig Migration) error { return nil }
	m := New(db, WithBeforeMigration(fn))
	if m.beforeMigration == nil {
		t.Error("expected beforeMigration to be set")
	}
}

func TestWithAfterMigration(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	fn := func(ctx context.Context, mig Migration, d time.Duration, err error) {}
	m := New(db, WithAfterMigration(fn))
	if m.afterMigration == nil {
		t.Error("expected afterMigration to be set")
	}
}

func TestWithForce(t *testing.T) {
	t.Parallel()
	cfg := newDownConfig([]DownOption{WithForce()})
	if !cfg.force {
		t.Error("expected force to be true")
	}
}

func TestNewDownConfig_Empty(t *testing.T) {
	t.Parallel()
	cfg := newDownConfig(nil)
	if cfg.force {
		t.Error("expected force to be false")
	}
}

func TestResolveOptions_TxDefault_SQLite(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	m := New(db)
	if m.txStrategy != TxPerMigration {
		t.Errorf("expected TxPerMigration for SQLite default, got %v", m.txStrategy)
	}
}

func TestResolveOptions_TxDefault_PostgreSQL(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	m := New(db, WithDialect(DialectPostgreSQL))
	if m.txStrategy != TxPerBatch {
		t.Errorf("expected TxPerBatch for PostgreSQL default, got %v", m.txStrategy)
	}
}

func TestResolveOptions_TxDefault_MySQL(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	m := New(db, WithDialect(DialectMySQL))
	if m.txStrategy != TxPerMigration {
		t.Errorf("expected TxPerMigration for MySQL default, got %v", m.txStrategy)
	}
}

func TestWithAdvisoryLock_MySQL(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	m := New(db, WithDialect(DialectMySQL), WithAdvisoryLock())
	if _, ok := m.lockStrategy.(*mysqlLock); !ok {
		t.Errorf("expected *mysqlLock, got %T", m.lockStrategy)
	}
}

func TestWithLockStrategy_DisablesAdvisory(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	custom := &noopLock{}
	m := New(db, WithAdvisoryLock(), WithLockStrategy(custom))
	if m.advisoryLock {
		t.Error("expected advisoryLock to be false after WithLockStrategy")
	}
}
