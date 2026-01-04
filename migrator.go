package migrator

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

const migrationTableSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    id VARCHAR(255) PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch INTEGER NOT NULL
);
`

const migrationTableIndexSQL = `
CREATE INDEX IF NOT EXISTS idx_schema_migrations_batch ON schema_migrations(batch);
`

type Migrator struct {
	db         *sql.DB
	mu         sync.Mutex
	migrations []Migration
	dialect    Dialect
}

func New(db *sql.DB) *Migrator {
	return &Migrator{
		db:      db,
		dialect: detectDialect(db),
	}
}

func (m *Migrator) Dialect() Dialect {
	return m.dialect
}

func (m *Migrator) Register(migration ...Migration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.migrations = append(m.migrations, migration...)
}

func (m *Migrator) Up() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ctx := context.Background()

	applied, err := m.getAppliedMigrations(ctx)
	if err != nil {
		return errors.Join(ErrFailedToGetAppliedMigrations, err)
	}

	appliedMap := make(map[string]bool)
	for _, a := range applied {
		appliedMap[a.ID] = true
	}

	migrations := m.migrations

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].ID() < migrations[j].ID()
	})

	var newMigrations []Migration
	for _, migration := range migrations {
		if !appliedMap[migration.ID()] {
			newMigrations = append(newMigrations, migration)
		}
	}

	if len(newMigrations) == 0 {
		return nil
	}

	nextBatch := m.getNextBatchNumber(applied)

	return m.executeMigrationBatch(ctx, newMigrations, nextBatch)
}

func (m *Migrator) Down(steps int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ctx := context.Background()

	applied, err := m.getAppliedMigrations(ctx)
	if err != nil {
		return errors.Join(ErrFailedToGetAppliedMigrations, err)
	}

	if len(applied) == 0 {
		return ErrNoMigrationsToRollback
	}

	migrationMap := m.buildMigrationMap(m.migrations)
	rollbackList := m.buildRollbackList(applied, steps)

	return m.executeRollback(ctx, rollbackList, migrationMap)
}

func (m *Migrator) Status() ([]MigrationStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getAppliedMigrations(context.Background())
}

func (m *Migrator) createMigrationTable() error {
	_, err := m.db.Exec(migrationTableSQL)
	if err != nil {
		return errors.Join(ErrFailedToCreateSchemaMigrationsTable, err)
	}

	_, err = m.db.Exec(migrationTableIndexSQL)
	if err != nil {
		return errors.Join(ErrFailedToCreateSchemaMigrationsIndex, err)
	}

	return nil
}

func (m *Migrator) executeMigrationBatch(ctx context.Context, migrations []Migration, batch int) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Join(ErrFailedToBeginTransaction, err)
	}

	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	for _, migration := range migrations {
		if err := m.executeMigrationUp(ctx, tx, migration, batch); err != nil {
			return errors.Join(ErrMigrationFailed, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil
	return nil
}

func (m *Migrator) buildMigrationMap(migrations []Migration) map[string]Migration {
	migrationMap := make(map[string]Migration)
	for _, migration := range migrations {
		migrationMap[migration.ID()] = migration
	}
	return migrationMap
}

func (m *Migrator) buildRollbackList(applied []MigrationStatus, steps int) []MigrationStatus {
	sort.Slice(applied, func(i, j int) bool {
		return applied[i].Batch > applied[j].Batch ||
			(applied[i].Batch == applied[j].Batch && applied[i].ID > applied[j].ID)
	})

	if steps <= 0 || steps > len(applied) {
		steps = len(applied)
	}

	return applied[:steps]
}

func (m *Migrator) executeRollback(ctx context.Context, rollbackList []MigrationStatus, migrationMap map[string]Migration) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Join(ErrFailedToBeginTransaction, err)
	}

	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	for _, migrationStatus := range rollbackList {
		if err := m.rollbackSingleMigration(ctx, tx, migrationStatus, migrationMap); err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil
	return nil
}

func (m *Migrator) rollbackSingleMigration(ctx context.Context, tx *sql.Tx, migrationStatus MigrationStatus, migrationMap map[string]Migration) error {
	if migration, exists := migrationMap[migrationStatus.ID]; exists {
		for _, query := range migration.Down() {
			trimmedQuery := strings.TrimSpace(query)
			if trimmedQuery == "" || strings.HasPrefix(trimmedQuery, "--") {
				continue
			}

			if _, err := tx.ExecContext(ctx, query); err != nil {
				return errors.Join(ErrMigrationFailed, err)
			}
		}
	}

	if err := m.deleteMigrationRecord(ctx, tx, migrationStatus.ID); err != nil {
		return errors.Join(ErrMigrationFailed, err)
	}

	return nil
}

func (m *Migrator) executeMigrationUp(ctx context.Context, tx *sql.Tx, migration Migration, batch int) error {
	for _, query := range migration.Up() {
		if strings.TrimSpace(query) == "" {
			continue
		}

		if _, err := tx.ExecContext(ctx, query); err != nil {
			return errors.Join(ErrFailedToExecuteQuery, err)
		}
	}

	query := m.dialect.placeholder("INSERT INTO schema_migrations (id, description, batch) VALUES (?, ?, ?)")
	_, err := tx.ExecContext(ctx, query, migration.ID(), migration.Description(), batch)

	return err
}

func (m *Migrator) deleteMigrationRecord(ctx context.Context, tx *sql.Tx, migrationID string) error {
	query := m.dialect.placeholder("DELETE FROM schema_migrations WHERE id = ?")
	_, err := tx.ExecContext(ctx, query, migrationID)
	return err
}

func (m *Migrator) getAppliedMigrations(ctx context.Context) ([]MigrationStatus, error) {
	if err := m.createMigrationTable(); err != nil {
		return nil, err
	}
	query := "SELECT id, description, applied_at, batch FROM schema_migrations ORDER BY batch, id"
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer func() {
		if rows != nil {
			_ = rows.Close()
		}
	}()

	var migrations []MigrationStatus
	for rows.Next() {
		var migration MigrationStatus
		var appliedAt time.Time

		err := rows.Scan(&migration.ID, &migration.Description, &appliedAt, &migration.Batch)
		if err != nil {
			return nil, err
		}

		migration.AppliedAt = &appliedAt
		migrations = append(migrations, migration)
	}

	return migrations, rows.Err()
}

func (m *Migrator) getNextBatchNumber(applied []MigrationStatus) int {
	maxBatch := 0
	for _, migration := range applied {
		if migration.Batch > maxBatch {
			maxBatch = migration.Batch
		}
	}
	return maxBatch + 1
}
