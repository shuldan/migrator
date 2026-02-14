package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultTableName = "schema_migrations"

type executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type preparedQueries struct {
	createTable     string
	createIndex     string
	selectApplied   string
	insertMigration string
	deleteMigration string
}

type Migrator struct {
	db              *sql.DB
	mu              sync.Mutex
	migrations      []Migration
	initialized     bool
	queries         preparedQueries
	dialect         Dialect
	tableName       string
	schema          string
	logger          Logger
	lockStrategy    LockStrategy
	txStrategy      TxStrategy
	advisoryLock    bool
	beforeMigration BeforeMigrationFunc
	afterMigration  AfterMigrationFunc
}

func New(db *sql.DB, opts ...Option) *Migrator {
	m := &Migrator{
		db:           db,
		dialect:      detectDialect(db),
		tableName:    defaultTableName,
		logger:       &noopLogger{},
		lockStrategy: &noopLock{},
		txStrategy:   TxDefault,
	}

	for _, opt := range opts {
		opt(m)
	}

	m.resolveOptions()

	return m
}

func (m *Migrator) resolveOptions() {
	if m.advisoryLock {
		m.lockStrategy = newAdvisoryLock(m.db, m.dialect, m.qualifiedTableName())
	}

	if m.txStrategy == TxDefault {
		switch m.dialect {
		case DialectPostgreSQL:
			m.txStrategy = TxPerBatch
		default:
			m.txStrategy = TxPerMigration
		}
	}

	m.buildQueries()
}

func (m *Migrator) buildQueries() {
	table := m.qualifiedTableName()
	indexName := "idx_" + strings.ReplaceAll(m.tableName, ".", "_") + "_batch"

	m.queries.createTable = "CREATE TABLE IF NOT EXISTS " + table + ` (
    id VARCHAR(255) PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch INTEGER NOT NULL
);`

	m.queries.createIndex = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + table + "(batch);"

	m.queries.selectApplied = "SELECT id, description, applied_at, batch FROM " + table + " ORDER BY batch, id"

	m.queries.insertMigration = m.dialect.placeholder(
		"INSERT INTO " + table + " (id, description, batch) VALUES (?, ?, ?)",
	)

	m.queries.deleteMigration = m.dialect.placeholder(
		"DELETE FROM " + table + " WHERE id = ?",
	)
}

func (m *Migrator) Dialect() Dialect {
	return m.dialect
}

func (m *Migrator) qualifiedTableName() string {
	if m.schema != "" {
		return m.schema + "." + m.tableName
	}
	return m.tableName
}

func (m *Migrator) Register(migrations ...Migration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids := make(map[string]bool, len(m.migrations))
	for _, existing := range m.migrations {
		ids[existing.ID()] = true
	}

	for _, migration := range migrations {
		if migration == nil {
			return ErrNilMigration
		}
		if ids[migration.ID()] {
			return fmt.Errorf("%w: %s", ErrDuplicateMigrationID, migration.ID())
		}
		ids[migration.ID()] = true
	}

	m.migrations = append(m.migrations, migrations...)

	sort.Slice(m.migrations, func(i, j int) bool {
		return m.migrations[i].ID() < m.migrations[j].ID()
	})

	return nil
}

func (m *Migrator) MustRegister(migrations ...Migration) {
	if err := m.Register(migrations...); err != nil {
		panic(fmt.Sprintf("migrator: %v", err))
	}
}

func (m *Migrator) ensureInitialized(ctx context.Context) error {
	if m.initialized {
		return nil
	}

	if _, err := m.db.ExecContext(ctx, m.queries.createTable); err != nil {
		return fmt.Errorf("%w: %w", ErrFailedToCreateSchemaMigrationsTable, err)
	}

	if _, err := m.db.ExecContext(ctx, m.queries.createIndex); err != nil {
		return fmt.Errorf("%w: %w", ErrFailedToCreateSchemaMigrationsIndex, err)
	}

	m.initialized = true
	return nil
}

func (m *Migrator) Up(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.lockStrategy.Lock(ctx); err != nil {
		return err
	}
	defer func() { _ = m.lockStrategy.Unlock(ctx) }()

	if err := m.ensureInitialized(ctx); err != nil {
		return err
	}

	applied, err := m.getAppliedFromDB(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFailedToGetAppliedMigrations, err)
	}

	pending := m.pendingMigrations(applied)
	if len(pending) == 0 {
		m.logger.Info("no pending migrations")
		return nil
	}

	batch := m.nextBatchNumber(applied)

	m.logger.Info("applying migrations", "count", len(pending), "batch", batch)

	return m.executeMigrationsUp(ctx, pending, batch)
}

func (m *Migrator) Plan(ctx context.Context) ([]PlannedMigration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	applied, err := m.getAppliedFromDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFailedToGetAppliedMigrations, err)
	}

	pending := m.pendingMigrations(applied)

	planned := make([]PlannedMigration, 0, len(pending))
	for _, migration := range pending {
		planned = append(planned, PlannedMigration{
			ID:          migration.ID(),
			Description: migration.Description(),
			Queries:     migration.Up(),
		})
	}

	return planned, nil
}

func (m *Migrator) Down(ctx context.Context, steps int, opts ...DownOption) error {
	if steps < 1 {
		return fmt.Errorf("%w: steps must be >= 1", ErrInvalidArgument)
	}
	return m.performDown(ctx, func(applied []MigrationStatus) []MigrationStatus {
		return m.rollbackListBySteps(applied, steps)
	}, opts...)
}

func (m *Migrator) DownBatch(ctx context.Context, opts ...DownOption) error {
	return m.performDown(ctx, m.rollbackListByLastBatch, opts...)
}

func (m *Migrator) DownAll(ctx context.Context, opts ...DownOption) error {
	return m.performDown(ctx, m.rollbackListAll, opts...)
}

func (m *Migrator) PlanDown(ctx context.Context, steps int) ([]PlannedMigration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	applied, err := m.getAppliedFromDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFailedToGetAppliedMigrations, err)
	}

	rollbackList := m.rollbackListBySteps(applied, steps)
	migrationMap := m.buildMigrationMap()

	planned := make([]PlannedMigration, 0, len(rollbackList))
	for _, status := range rollbackList {
		p := PlannedMigration{
			ID:          status.ID,
			Description: status.Description,
		}
		if migration, ok := migrationMap[status.ID]; ok {
			p.Queries = migration.Down()
		}
		planned = append(planned, p)
	}

	return planned, nil
}

func (m *Migrator) performDown(ctx context.Context, buildList func([]MigrationStatus) []MigrationStatus, opts ...DownOption) error {
	cfg := newDownConfig(opts)
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.lockStrategy.Lock(ctx); err != nil {
		return err
	}
	defer func() { _ = m.lockStrategy.Unlock(ctx) }()

	if err := m.ensureInitialized(ctx); err != nil {
		return err
	}

	applied, err := m.getAppliedFromDB(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFailedToGetAppliedMigrations, err)
	}

	if len(applied) == 0 {
		return ErrNoMigrationsToRollback
	}

	rollbackList := buildList(applied)
	if len(rollbackList) == 0 {
		return ErrNoMigrationsToRollback
	}

	if !cfg.force {
		if err := m.checkIrreversible(rollbackList); err != nil {
			return err
		}
	}

	m.logger.Info("rolling back migrations", "count", len(rollbackList))

	return m.executeMigrationsDown(ctx, rollbackList, cfg.force)
}

func (m *Migrator) Status(ctx context.Context) ([]MigrationStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	applied, err := m.getAppliedFromDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFailedToGetAppliedMigrations, err)
	}

	appliedMap := make(map[string]MigrationStatus, len(applied))
	for _, a := range applied {
		appliedMap[a.ID] = a
	}

	seen := make(map[string]bool)
	var result []MigrationStatus

	for _, migration := range m.migrations {
		seen[migration.ID()] = true
		if a, ok := appliedMap[migration.ID()]; ok {
			a.State = MigrationStateApplied
			result = append(result, a)
		} else {
			result = append(result, MigrationStatus{
				ID:          migration.ID(),
				Description: migration.Description(),
				State:       MigrationStatePending,
			})
		}
	}

	for _, a := range applied {
		if !seen[a.ID] {
			a.State = MigrationStateGhost
			result = append(result, a)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

func (m *Migrator) getAppliedFromDB(ctx context.Context) ([]MigrationStatus, error) {
	rows, err := m.db.QueryContext(ctx, m.queries.selectApplied)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			m.logger.Error("failed to close rows", "error", closeErr)
		}
	}()

	var result []MigrationStatus
	for rows.Next() {
		var s MigrationStatus
		var appliedAt time.Time
		if err := rows.Scan(&s.ID, &s.Description, &appliedAt, &s.Batch); err != nil {
			return nil, err
		}
		s.AppliedAt = &appliedAt
		s.State = MigrationStateApplied
		result = append(result, s)
	}

	return result, rows.Err()
}

func (m *Migrator) pendingMigrations(applied []MigrationStatus) []Migration {
	appliedMap := make(map[string]bool, len(applied))
	for _, a := range applied {
		appliedMap[a.ID] = true
	}

	var pending []Migration
	for _, migration := range m.migrations {
		if !appliedMap[migration.ID()] {
			pending = append(pending, migration)
		}
	}

	return pending
}

func (m *Migrator) nextBatchNumber(applied []MigrationStatus) int {
	maxBatch := 0
	for _, a := range applied {
		if a.Batch > maxBatch {
			maxBatch = a.Batch
		}
	}
	return maxBatch + 1
}

func (m *Migrator) buildMigrationMap() map[string]Migration {
	mm := make(map[string]Migration, len(m.migrations))
	for _, migration := range m.migrations {
		mm[migration.ID()] = migration
	}
	return mm
}

func (m *Migrator) rollbackListBySteps(applied []MigrationStatus, steps int) []MigrationStatus {
	sorted := m.sortedForRollback(applied)
	if steps > len(sorted) {
		steps = len(sorted)
	}
	return sorted[:steps]
}

func (m *Migrator) rollbackListByLastBatch(applied []MigrationStatus) []MigrationStatus {
	maxBatch := 0
	for _, a := range applied {
		if a.Batch > maxBatch {
			maxBatch = a.Batch
		}
	}

	var list []MigrationStatus
	for _, a := range applied {
		if a.Batch == maxBatch {
			list = append(list, a)
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ID > list[j].ID
	})

	return list
}

func (m *Migrator) rollbackListAll(applied []MigrationStatus) []MigrationStatus {
	return m.sortedForRollback(applied)
}

func (m *Migrator) sortedForRollback(applied []MigrationStatus) []MigrationStatus {
	sorted := make([]MigrationStatus, len(applied))
	copy(sorted, applied)

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Batch != sorted[j].Batch {
			return sorted[i].Batch > sorted[j].Batch
		}
		return sorted[i].ID > sorted[j].ID
	})

	return sorted
}

func (m *Migrator) checkIrreversible(rollbackList []MigrationStatus) error {
	migrationMap := m.buildMigrationMap()
	for _, status := range rollbackList {
		if migration, ok := migrationMap[status.ID]; ok && migration.Irreversible() {
			return &MigrationError{
				MigrationID: status.ID,
				Phase:       "down",
				Err:         ErrIrreversibleMigration,
			}
		}
	}
	return nil
}

func (m *Migrator) executeMigrationsUp(ctx context.Context, migrations []Migration, batch int) error {
	switch m.txStrategy {
	case TxPerBatch:
		return m.executeUpPerBatch(ctx, migrations, batch)
	case TxDisabled:
		return m.executeUpNoTx(ctx, migrations, batch)
	default:
		return m.executeUpPerMigration(ctx, migrations, batch)
	}
}

func (m *Migrator) executeUpPerBatch(ctx context.Context, migrations []Migration, batch int) error {
	var group []Migration

	flush := func() error {
		if len(group) == 0 {
			return nil
		}

		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrFailedToBeginTransaction, err)
		}
		defer func() { _ = tx.Rollback() }()

		for _, migration := range group {
			if err := m.runSingleUp(ctx, tx, migration, batch); err != nil {
				return err
			}
		}

		group = nil
		return tx.Commit()
	}

	for _, migration := range migrations {
		if migration.DisableTx() {
			if err := flush(); err != nil {
				return err
			}
			if err := m.runSingleUp(ctx, m.db, migration, batch); err != nil {
				return err
			}
		} else {
			group = append(group, migration)
		}
	}

	return flush()
}

func (m *Migrator) executeUpPerMigration(ctx context.Context, migrations []Migration, batch int) error {
	for _, migration := range migrations {
		if migration.DisableTx() {
			if err := m.runSingleUp(ctx, m.db, migration, batch); err != nil {
				return err
			}
			continue
		}

		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrFailedToBeginTransaction, err)
		}

		if err := m.runSingleUp(ctx, tx, migration, batch); err != nil {
			_ = tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) executeUpNoTx(ctx context.Context, migrations []Migration, batch int) error {
	for _, migration := range migrations {
		if err := m.runSingleUp(ctx, m.db, migration, batch); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) runSingleUp(ctx context.Context, exec executor, migration Migration, batch int) error {
	if m.beforeMigration != nil {
		if err := m.beforeMigration(ctx, migration); err != nil {
			return &MigrationError{
				MigrationID: migration.ID(),
				Phase:       "up",
				Err:         fmt.Errorf("before hook: %w", err),
			}
		}
	}

	start := time.Now()
	err := m.executeUpQueries(ctx, exec, migration, batch)
	duration := time.Since(start)

	if m.afterMigration != nil {
		m.afterMigration(ctx, migration, duration, err)
	}

	if err != nil {
		return err
	}

	m.logger.Info("migration applied",
		"id", migration.ID(),
		"duration", duration.String(),
	)

	return nil
}

func (m *Migrator) executeUpQueries(ctx context.Context, exec executor, migration Migration, batch int) error {
	for _, query := range migration.Up() {
		if strings.TrimSpace(query) == "" {
			continue
		}
		if _, err := exec.ExecContext(ctx, query); err != nil {
			return &MigrationError{
				MigrationID: migration.ID(),
				Phase:       "up",
				Query:       query,
				Err:         fmt.Errorf("%w: %w", ErrFailedToExecuteQuery, err),
			}
		}
	}

	return m.recordMigration(ctx, exec, migration, batch)
}

func (m *Migrator) recordMigration(ctx context.Context, exec executor, migration Migration, batch int) error {
	if _, err := exec.ExecContext(ctx, m.queries.insertMigration, migration.ID(), migration.Description(), batch); err != nil {
		return &MigrationError{
			MigrationID: migration.ID(),
			Phase:       "up",
			Err:         fmt.Errorf("failed to record migration: %w", err),
		}
	}
	return nil
}

func (m *Migrator) executeMigrationsDown(ctx context.Context, rollbackList []MigrationStatus, force bool) error {
	switch m.txStrategy {
	case TxPerBatch:
		return m.executeDownPerBatch(ctx, rollbackList, force)
	case TxDisabled:
		return m.executeDownNoTx(ctx, rollbackList, force)
	default:
		return m.executeDownPerMigration(ctx, rollbackList, force)
	}
}

func (m *Migrator) executeDownPerBatch(ctx context.Context, rollbackList []MigrationStatus, force bool) error {
	migrationMap := m.buildMigrationMap()
	var group []MigrationStatus

	flush := func() error {
		if len(group) == 0 {
			return nil
		}

		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrFailedToBeginTransaction, err)
		}
		defer func() { _ = tx.Rollback() }()

		for i := range group {
			if err := m.runSingleDown(ctx, tx, group[i], migrationMap, force); err != nil {
				return err
			}
		}

		group = nil
		return tx.Commit()
	}

	for i := range rollbackList {
		migration, exists := migrationMap[rollbackList[i].ID]
		if exists && migration.DisableTx() {
			if err := flush(); err != nil {
				return err
			}
			if err := m.runSingleDown(ctx, m.db, rollbackList[i], migrationMap, force); err != nil {
				return err
			}
		} else {
			group = append(group, rollbackList[i])
		}
	}

	return flush()
}

func (m *Migrator) executeDownPerMigration(ctx context.Context, rollbackList []MigrationStatus, force bool) error {
	migrationMap := m.buildMigrationMap()

	for i := range rollbackList {
		migration, exists := migrationMap[rollbackList[i].ID]
		disableTx := exists && migration.DisableTx()

		if disableTx {
			if err := m.runSingleDown(ctx, m.db, rollbackList[i], migrationMap, force); err != nil {
				return err
			}
			continue
		}

		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrFailedToBeginTransaction, err)
		}

		if err := m.runSingleDown(ctx, tx, rollbackList[i], migrationMap, force); err != nil {
			_ = tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) executeDownNoTx(ctx context.Context, rollbackList []MigrationStatus, force bool) error {
	migrationMap := m.buildMigrationMap()
	for i := range rollbackList {
		if err := m.runSingleDown(ctx, m.db, rollbackList[i], migrationMap, force); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) runSingleDown(ctx context.Context, exec executor, status MigrationStatus, migrationMap map[string]Migration, force bool) error {
	migration, exists := migrationMap[status.ID]

	if m.beforeMigration != nil && exists {
		if err := m.beforeMigration(ctx, migration); err != nil {
			return &MigrationError{
				MigrationID: status.ID,
				Phase:       "down",
				Err:         fmt.Errorf("before hook: %w", err),
			}
		}
	}

	start := time.Now()
	err := m.executeDownQueries(ctx, exec, status, migration, exists, force)
	duration := time.Since(start)

	if m.afterMigration != nil && exists {
		m.afterMigration(ctx, migration, duration, err)
	}

	if err != nil {
		return err
	}

	m.logger.Info("migration rolled back",
		"id", status.ID,
		"duration", duration.String(),
	)

	return nil
}

func (m *Migrator) executeDownQueries(ctx context.Context, exec executor, status MigrationStatus, migration Migration, exists, force bool) error {
	if exists {
		skipQueries := migration.Irreversible() && force

		if !skipQueries {
			for _, query := range migration.Down() {
				trimmed := strings.TrimSpace(query)
				if trimmed == "" || strings.HasPrefix(trimmed, "--") {
					continue
				}
				if _, err := exec.ExecContext(ctx, query); err != nil {
					return &MigrationError{
						MigrationID: status.ID,
						Phase:       "down",
						Query:       query,
						Err:         fmt.Errorf("%w: %w", ErrFailedToExecuteQuery, err),
					}
				}
			}
		}
	}

	return m.deleteMigrationRecord(ctx, exec, status.ID)
}

func (m *Migrator) deleteMigrationRecord(ctx context.Context, exec executor, id string) error {
	if _, err := exec.ExecContext(ctx, m.queries.deleteMigration, id); err != nil {
		return &MigrationError{
			MigrationID: id,
			Phase:       "down",
			Err:         fmt.Errorf("failed to delete migration record: %w", err),
		}
	}
	return nil
}
