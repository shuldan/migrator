package migrator

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestNoopLock_LockUnlock(t *testing.T) {
	t.Parallel()
	lock := NoopLockStrategy()
	ctx := context.Background()
	if err := lock.Lock(ctx); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if err := lock.Unlock(ctx); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestNewAdvisoryLock_PostgreSQL(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	lock := newAdvisoryLock(db, DialectPostgreSQL, "migrations")
	if _, ok := lock.(*postgresAdvisoryLock); !ok {
		t.Errorf("expected *postgresAdvisoryLock, got %T", lock)
	}
}

func TestNewAdvisoryLock_MySQL(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	lock := newAdvisoryLock(db, DialectMySQL, "migrations")
	if _, ok := lock.(*mysqlLock); !ok {
		t.Errorf("expected *mysqlLock, got %T", lock)
	}
}

func TestNewAdvisoryLock_Default(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	lock := newAdvisoryLock(db, DialectSQLite, "migrations")
	if _, ok := lock.(*noopLock); !ok {
		t.Errorf("expected *noopLock, got %T", lock)
	}
}

func TestHashToInt64_Deterministic(t *testing.T) {
	t.Parallel()
	a := hashToInt64("test_table")
	b := hashToInt64("test_table")
	if a != b {
		t.Errorf("expected deterministic result, got %d and %d", a, b)
	}
}

func TestHashToInt64_DifferentInputs(t *testing.T) {
	t.Parallel()
	a := hashToInt64("table_a")
	b := hashToInt64("table_b")
	if a == b {
		t.Errorf("expected different hashes for different inputs")
	}
}

func TestHashToInt64_NonNegative(t *testing.T) {
	t.Parallel()
	val := hashToInt64("anything")
	if val < 0 {
		t.Errorf("expected non-negative, got %d", val)
	}
}

func TestPostgresAdvisoryLock_UnlockNilConn(t *testing.T) {
	t.Parallel()
	lock := &postgresAdvisoryLock{conn: nil}
	if err := lock.Unlock(context.Background()); err != nil {
		t.Errorf("expected nil for unlock with nil conn, got %v", err)
	}
}

func TestMySQLLock_UnlockNilConn(t *testing.T) {
	t.Parallel()
	lock := &mysqlLock{conn: nil}
	if err := lock.Unlock(context.Background()); err != nil {
		t.Errorf("expected nil for unlock with nil conn, got %v", err)
	}
}

func TestNewPostgresAdvisoryLock_Fields(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	lock := newPostgresAdvisoryLock(db, "my_table")
	if lock.db != db {
		t.Error("expected db to be set")
	}
	if lock.lockKey != hashToInt64("my_table") {
		t.Errorf("expected lockKey %d, got %d", hashToInt64("my_table"), lock.lockKey)
	}
}

func TestNewMySQLLock_Fields(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	lock := newMySQLLock(db, "my_table")
	if lock.lockName != "migrator_my_table" {
		t.Errorf("expected lockName %q, got %q", "migrator_my_table", lock.lockName)
	}
	if lock.timeout != 10 {
		t.Errorf("expected timeout 10, got %d", lock.timeout)
	}
}

func TestPostgresAdvisoryLock_Lock_ExecError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	lock := newPostgresAdvisoryLock(db, "test_table")
	err = lock.Lock(context.Background())
	if err == nil {
		t.Fatal("expected error from pg_advisory_lock on sqlite")
	}
	if !errors.Is(err, ErrFailedToAcquireLock) {
		t.Errorf("expected ErrFailedToAcquireLock, got %v", err)
	}
}

func TestPostgresAdvisoryLock_Lock_ConnError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.Close()
	lock := newPostgresAdvisoryLock(db, "test_table")
	err = lock.Lock(context.Background())
	if err == nil {
		t.Fatal("expected error from closed db")
	}
	if !errors.Is(err, ErrFailedToAcquireLock) {
		t.Errorf("expected ErrFailedToAcquireLock, got %v", err)
	}
}

func TestPostgresAdvisoryLock_Unlock_ExecError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	lock := &postgresAdvisoryLock{db: db, lockKey: 12345, conn: conn}
	err = lock.Unlock(context.Background())
	if err == nil {
		t.Fatal("expected error from pg_advisory_unlock on sqlite")
	}
	if !errors.Is(err, ErrFailedToReleaseLock) {
		t.Errorf("expected ErrFailedToReleaseLock, got %v", err)
	}
	if lock.conn != nil {
		t.Error("expected conn to be nil after unlock")
	}
}

func TestMySQLLock_Lock_QueryError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	lock := newMySQLLock(db, "test_table")
	err = lock.Lock(context.Background())
	if err == nil {
		t.Fatal("expected error from GET_LOCK on sqlite")
	}
	if !errors.Is(err, ErrFailedToAcquireLock) {
		t.Errorf("expected ErrFailedToAcquireLock, got %v", err)
	}
}

func TestMySQLLock_Lock_ConnError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.Close()
	lock := newMySQLLock(db, "test_table")
	err = lock.Lock(context.Background())
	if err == nil {
		t.Fatal("expected error from closed db")
	}
	if !errors.Is(err, ErrFailedToAcquireLock) {
		t.Errorf("expected ErrFailedToAcquireLock, got %v", err)
	}
}

func TestMySQLLock_Unlock_ExecError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	lock := &mysqlLock{db: db, lockName: "test_lock", timeout: 10, conn: conn}
	err = lock.Unlock(context.Background())
	if err == nil {
		t.Fatal("expected error from RELEASE_LOCK on sqlite")
	}
	if !errors.Is(err, ErrFailedToReleaseLock) {
		t.Errorf("expected ErrFailedToReleaseLock, got %v", err)
	}
	if lock.conn != nil {
		t.Error("expected conn to be nil after unlock")
	}
}
