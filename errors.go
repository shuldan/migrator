package migrator

import (
	"errors"
	"fmt"
)

var (
	ErrMigrationFailed                     = errors.New("database migration failed")
	ErrFailedToCreateSchemaMigrationsTable = errors.New("failed to create schema_migrations table")
	ErrFailedToCreateSchemaMigrationsIndex = errors.New("failed to create index on schema_migrations table")
	ErrFailedToGetAppliedMigrations        = errors.New("failed to fetch applied migrations")
	ErrFailedToBeginTransaction            = errors.New("failed to begin database transaction")
	ErrNoMigrationsToRollback              = errors.New("no applied migrations to rollback")
	ErrFailedToExecuteQuery                = errors.New("failed to execute database query")
	ErrIrreversibleMigration               = errors.New("migration is irreversible")
	ErrDuplicateMigrationID                = errors.New("duplicate migration id")
	ErrNilMigration                        = errors.New("migration is nil")
	ErrEmptyMigrationID                    = errors.New("migration id must not be empty")
	ErrEmptyMigrationDescription           = errors.New("migration description must not be empty")
	ErrNoUpQueries                         = errors.New("migration must have at least one up query")
	ErrInvalidColumnDefinition             = errors.New("column definition must contain at least name and type")
	ErrInvalidArgument                     = errors.New("invalid argument")
	ErrFailedToAcquireLock                 = errors.New("failed to acquire migration lock")
	ErrFailedToReleaseLock                 = errors.New("failed to release migration lock")
)

type MigrationError struct {
	MigrationID string
	Phase       string
	Query       string
	Err         error
}

func (e *MigrationError) Error() string {
	if e.Query != "" {
		q := e.Query
		if len(q) > 120 {
			q = q[:120] + "..."
		}
		return fmt.Sprintf("migration %q (%s): %v [query: %s]", e.MigrationID, e.Phase, e.Err, q)
	}
	return fmt.Sprintf("migration %q (%s): %v", e.MigrationID, e.Phase, e.Err)
}

func (e *MigrationError) Unwrap() error {
	return e.Err
}
