package migrator

import (
	"context"
	"time"
)

type TxStrategy int

const (
	TxDefault TxStrategy = iota
	TxPerBatch
	TxPerMigration
	TxDisabled
)

type BeforeMigrationFunc func(ctx context.Context, migration Migration) error

type AfterMigrationFunc func(ctx context.Context, migration Migration, duration time.Duration, err error)

type Option func(*Migrator)

func WithDialect(d Dialect) Option {
	return func(m *Migrator) {
		m.dialect = d
	}
}

func WithTableName(name string) Option {
	return func(m *Migrator) {
		m.tableName = name
	}
}

func WithSchema(schema string) Option {
	return func(m *Migrator) {
		m.schema = schema
	}
}

func WithLogger(l Logger) Option {
	return func(m *Migrator) {
		if l != nil {
			m.logger = l
		}
	}
}

func WithAdvisoryLock() Option {
	return func(m *Migrator) {
		m.advisoryLock = true
	}
}

func WithLockStrategy(s LockStrategy) Option {
	return func(m *Migrator) {
		m.lockStrategy = s
		m.advisoryLock = false
	}
}

func WithTxStrategy(s TxStrategy) Option {
	return func(m *Migrator) {
		m.txStrategy = s
	}
}

func WithBeforeMigration(fn BeforeMigrationFunc) Option {
	return func(m *Migrator) {
		m.beforeMigration = fn
	}
}

func WithAfterMigration(fn AfterMigrationFunc) Option {
	return func(m *Migrator) {
		m.afterMigration = fn
	}
}

type DownOption func(*downConfig)

type downConfig struct {
	force bool
}

func WithForce() DownOption {
	return func(c *downConfig) {
		c.force = true
	}
}

func newDownConfig(opts []DownOption) *downConfig {
	cfg := &downConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}
