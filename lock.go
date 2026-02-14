package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
)

type LockStrategy interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

func NoopLockStrategy() LockStrategy {
	return &noopLock{}
}

type noopLock struct{}

func (n *noopLock) Lock(context.Context) error   { return nil }
func (n *noopLock) Unlock(context.Context) error { return nil }

// newAdvisoryLock создаёт блокировку, соответствующую диалекту.
func newAdvisoryLock(db *sql.DB, dialect Dialect, qualifiedTable string) LockStrategy {
	switch dialect {
	case DialectPostgreSQL:
		return newPostgresAdvisoryLock(db, qualifiedTable)
	case DialectMySQL:
		return newMySQLLock(db, qualifiedTable)
	default:
		return &noopLock{}
	}
}

type postgresAdvisoryLock struct {
	db      *sql.DB
	lockKey int64
	conn    *sql.Conn
}

func newPostgresAdvisoryLock(db *sql.DB, tableName string) *postgresAdvisoryLock {
	return &postgresAdvisoryLock{
		db:      db,
		lockKey: hashToInt64(tableName),
	}
}

func (l *postgresAdvisoryLock) Lock(ctx context.Context) error {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFailedToAcquireLock, err)
	}

	if _, err = conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", l.lockKey); err != nil {
		_ = conn.Close()
		return fmt.Errorf("%w: %w", ErrFailedToAcquireLock, err)
	}

	l.conn = conn
	return nil
}

func (l *postgresAdvisoryLock) Unlock(ctx context.Context) error {
	if l.conn == nil {
		return nil
	}

	_, err := l.conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", l.lockKey)
	closeErr := l.conn.Close()
	l.conn = nil

	if err != nil {
		return fmt.Errorf("%w: %w", ErrFailedToReleaseLock, err)
	}
	if closeErr != nil {
		return fmt.Errorf("%w: %w", ErrFailedToReleaseLock, closeErr)
	}

	return nil
}

type mysqlLock struct {
	db       *sql.DB
	lockName string
	timeout  int
	conn     *sql.Conn
}

func newMySQLLock(db *sql.DB, tableName string) *mysqlLock {
	return &mysqlLock{
		db:       db,
		lockName: "migrator_" + tableName,
		timeout:  10,
	}
}

func (l *mysqlLock) Lock(ctx context.Context) error {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFailedToAcquireLock, err)
	}

	var result int
	if err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", l.lockName, l.timeout).Scan(&result); err != nil {
		_ = conn.Close()
		return fmt.Errorf("%w: %w", ErrFailedToAcquireLock, err)
	}

	if result != 1 {
		_ = conn.Close()
		return fmt.Errorf("%w: timeout acquiring lock %q", ErrFailedToAcquireLock, l.lockName)
	}

	l.conn = conn
	return nil
}

func (l *mysqlLock) Unlock(ctx context.Context) error {
	if l.conn == nil {
		return nil
	}

	_, err := l.conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", l.lockName)
	closeErr := l.conn.Close()
	l.conn = nil

	if err != nil {
		return fmt.Errorf("%w: %w", ErrFailedToReleaseLock, err)
	}
	if closeErr != nil {
		return fmt.Errorf("%w: %w", ErrFailedToReleaseLock, closeErr)
	}

	return nil
}

func hashToInt64(s string) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int64(h.Sum32())
}
