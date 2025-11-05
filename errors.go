package migrator

import "errors"

var (
	ErrMigrationFailed                     = errors.New("database migration failed")
	ErrFailedToCreateSchemaMigrationsTable = errors.New("failed to create schema_migrations table")
	ErrFailedToCreateSchemaMigrationsIndex = errors.New("failed to create index on schema_migrations table")
	ErrFailedToGetAppliedMigrations        = errors.New("failed to fetch applied migrations")
	ErrFailedToBeginTransaction            = errors.New("failed to begin database transaction")
	ErrNoMigrationsToRollback              = errors.New("no applied migrations to rollback")
	ErrFailedToExecuteQuery                = errors.New("failed to execute database query")
)
