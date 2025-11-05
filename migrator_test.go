package migrator

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type mockMigration struct {
	id          string
	connName    string
	description string
	upQueries   []string
	downQueries []string
}

func (m *mockMigration) ID() string {
	return m.id
}

func (m *mockMigration) ConnectionName() string {
	return m.connName
}

func (m *mockMigration) Description() string {
	return m.description
}

func (m *mockMigration) Up() []string {
	return m.upQueries
}

func (m *mockMigration) Down() []string {
	return m.downQueries
}

func TestMigrator_New(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	migrator := New(db)
	if migrator == nil {
		t.Fatal("expected non-nil migrator")
	}
	if migrator.db != db {
		t.Error("expected db to be set correctly")
	}
}

func TestMigrator_createMigrationTable_Success(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	migrator := New(db)
	err = migrator.createMigrationTable()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'").Scan(&count)
	if err != nil {
		t.Errorf("failed to check table existence: %v", err)
	}
	if count != 1 {
		t.Error("expected schema_migrations table to exist")
	}

	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_schema_migrations_batch'").Scan(&count)
	if err != nil {
		t.Errorf("failed to check index existence: %v", err)
	}
	if count != 1 {
		t.Error("expected idx_schema_migrations_batch index to exist")
	}
}

func TestMigrator_createMigrationTable_CreateTableError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	db.Close()

	migrator := New(db)
	err = migrator.createMigrationTable()
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrFailedToCreateSchemaMigrationsTable) && !errors.Is(err, ErrFailedToCreateSchemaMigrationsIndex) {
		t.Errorf("expected error to be related to schema migrations table creation, got %v", err)
	}
}

func TestMigrator_getNextBatchNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		applied  []MigrationStatus
		expected int
	}{
		{
			name:     "empty migrations",
			applied:  []MigrationStatus{},
			expected: 1,
		},
		{
			name: "single batch",
			applied: []MigrationStatus{
				{ID: "1", Batch: 1},
			},
			expected: 2,
		},
		{
			name: "multiple batches",
			applied: []MigrationStatus{
				{ID: "1", Batch: 1},
				{ID: "2", Batch: 3},
				{ID: "3", Batch: 2},
			},
			expected: 4,
		},
		{
			name: "zero batch",
			applied: []MigrationStatus{
				{ID: "1", Batch: 0},
			},
			expected: 1,
		},
	}

	migrator := &Migrator{}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := migrator.getNextBatchNumber(tt.applied)
			if result != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMigrator_buildMigrationMap(t *testing.T) {
	t.Parallel()

	migrations := []Migration{
		&mockMigration{id: "1", description: "first"},
		&mockMigration{id: "2", description: "second"},
		&mockMigration{id: "3", description: "third"},
	}

	migrator := &Migrator{}
	migrationMap := migrator.buildMigrationMap(migrations)

	if len(migrationMap) != 3 {
		t.Errorf("expected map length 3, got %d", len(migrationMap))
	}

	for _, m := range migrations {
		if mapped, exists := migrationMap[m.ID()]; !exists {
			t.Errorf("expected migration %s to exist in map", m.ID())
		} else if mapped != m {
			t.Errorf("expected migration %s to match", m.ID())
		}
	}
}

func TestMigrator_buildRollbackList(t *testing.T) {
	t.Parallel()

	now := time.Now()
	applied := []MigrationStatus{
		{ID: "1", Batch: 1, AppliedAt: &now},
		{ID: "2", Batch: 1, AppliedAt: &now},
		{ID: "3", Batch: 2, AppliedAt: &now},
		{ID: "4", Batch: 3, AppliedAt: &now},
	}

	// Sort applied migrations as expected by the function
	sort.Slice(applied, func(i, j int) bool {
		return applied[i].Batch > applied[j].Batch ||
			(applied[i].Batch == applied[j].Batch && applied[i].ID > applied[j].ID)
	})

	migrator := &Migrator{}

	tests := []struct {
		name     string
		steps    int
		expected []string
	}{
		{
			name:     "all migrations",
			steps:    0,
			expected: []string{"4", "3", "2", "1"},
		},
		{
			name:     "more steps than migrations",
			steps:    10,
			expected: []string{"4", "3", "2", "1"},
		},
		{
			name:     "single step",
			steps:    1,
			expected: []string{"4"},
		},
		{
			name:     "two steps",
			steps:    2,
			expected: []string{"4", "3"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := migrator.buildRollbackList(applied, tt.steps)
			if len(result) != len(tt.expected) {
				t.Errorf("expected length %d, got %d", len(tt.expected), len(result))
				return
			}

			for i, id := range tt.expected {
				if result[i].ID != id {
					t.Errorf("expected %s at position %d, got %s", id, i, result[i].ID)
				}
			}
		})
	}
}

func TestMigrator_executeMigrationUp_EmptyQuery(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	// Create the schema_migrations table first
	_, err = db.Exec(migrationTableSQL)
	if err != nil {
		t.Fatalf("failed to create schema_migrations table: %v", err)
	}

	migrator := New(db)
	tx, _ := db.BeginTx(context.Background(), nil)
	defer func() { _ = tx.Rollback() }()

	migration := &mockMigration{
		id:          "1",
		description: "test",
		upQueries:   []string{"", "  ", "\n\t"},
	}

	err = migrator.executeMigrationUp(context.Background(), tx, migration, 1)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestMigrator_executeMigrationUp_QueryError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	// Create the schema_migrations table first
	_, err = db.Exec(migrationTableSQL)
	if err != nil {
		t.Fatalf("failed to create schema_migrations table: %v", err)
	}

	migrator := New(db)
	tx, _ := db.BeginTx(context.Background(), nil)
	defer func() { _ = tx.Rollback() }()

	migration := &mockMigration{
		id:          "1",
		description: "test",
		upQueries:   []string{"INVALID SQL STATEMENT"},
	}

	err = migrator.executeMigrationUp(context.Background(), tx, migration, 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrFailedToExecuteQuery) {
		t.Errorf("expected error to be ErrFailedToExecuteQuery, got %v", err)
	}
}

func TestMigrator_executeMigrationUp_InsertError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	// Create the schema_migrations table first
	_, err = db.Exec(migrationTableSQL)
	if err != nil {
		t.Fatalf("failed to create schema_migrations table: %v", err)
	}

	migrator := New(db)
	tx, _ := db.BeginTx(context.Background(), nil)
	defer func() { _ = tx.Rollback() }()

	// Insert a record with the same ID to cause a conflict
	_, err = tx.ExecContext(context.Background(), "INSERT INTO schema_migrations (id, description, batch) VALUES (?, ?, ?)", "1", "test", 1)
	if err != nil {
		t.Fatalf("failed to insert initial record: %v", err)
	}

	migration := &mockMigration{
		id:          "1",
		description: "test",
		upQueries:   []string{"SELECT 1"},
	}

	err = migrator.executeMigrationUp(context.Background(), tx, migration, 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if errors.Is(err, ErrFailedToExecuteQuery) {
		t.Errorf("did not expect ErrFailedToExecuteQuery, got %v", err)
	}
}

func TestMigrator_deleteMigrationRecord(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	// Create the schema_migrations table first
	_, err = db.Exec(migrationTableSQL)
	if err != nil {
		t.Fatalf("failed to create schema_migrations table: %v", err)
	}

	_, err = db.Exec("INSERT INTO schema_migrations (id, description, batch) VALUES (?, ?, ?)", "1", "test", 1)
	if err != nil {
		t.Fatalf("failed to insert test record: %v", err)
	}

	migrator := New(db)
	tx, _ := db.BeginTx(context.Background(), nil)
	defer func() { _ = tx.Rollback() }()

	err = migrator.deleteMigrationRecord(context.Background(), tx, "1")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM schema_migrations WHERE id = ?", "1").Scan(&count)
	if err != nil {
		t.Errorf("failed to check record existence: %v", err)
	}
	if count != 0 {
		t.Error("expected record to be deleted")
	}
}

func TestMigrator_deleteMigrationRecord_Error(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	migrator := New(db)
	tx, _ := db.BeginTx(context.Background(), nil)
	defer func() { _ = tx.Rollback() }()

	err = migrator.deleteMigrationRecord(context.Background(), tx, "1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestMigrator_rollbackSingleMigration_NotFound(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	// Create the schema_migrations table first
	_, err = db.Exec(migrationTableSQL)
	if err != nil {
		t.Fatalf("failed to create schema_migrations table: %v", err)
	}

	migrator := New(db)
	tx, _ := db.BeginTx(context.Background(), nil)
	defer func() { _ = tx.Rollback() }()

	migrationMap := make(map[string]Migration)
	migrationStatus := MigrationStatus{ID: "1"}

	err = migrator.rollbackSingleMigration(context.Background(), tx, migrationStatus, migrationMap)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestMigrator_rollbackSingleMigration_EmptyDown(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	// Create the schema_migrations table first
	_, err = db.Exec(migrationTableSQL)
	if err != nil {
		t.Fatalf("failed to create schema_migrations table: %v", err)
	}

	migrator := New(db)
	tx, _ := db.BeginTx(context.Background(), nil)
	defer func() { _ = tx.Rollback() }()

	migration := &mockMigration{
		id:          "1",
		description: "test",
		downQueries: []string{},
	}

	migrationMap := map[string]Migration{
		"1": migration,
	}

	migrationStatus := MigrationStatus{ID: "1"}

	err = migrator.rollbackSingleMigration(context.Background(), tx, migrationStatus, migrationMap)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestMigrator_rollbackSingleMigration_DownWithComments(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	// Create the schema_migrations table first
	_, err = db.Exec(migrationTableSQL)
	if err != nil {
		t.Fatalf("failed to create schema_migrations table: %v", err)
	}

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	migrator := New(db)
	tx, _ := db.BeginTx(context.Background(), nil)
	defer func() { _ = tx.Rollback() }()

	migration := &mockMigration{
		id:          "1",
		description: "test",
		downQueries: []string{
			"",
			"  ",
			"-- This is a comment",
			"DROP TABLE test",
			"\n",
		},
	}

	migrationMap := map[string]Migration{
		"1": migration,
	}

	migrationStatus := MigrationStatus{ID: "1"}

	err = migrator.rollbackSingleMigration(context.Background(), tx, migrationStatus, migrationMap)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestMigrator_rollbackSingleMigration_DownError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	// Create the schema_migrations table first
	_, err = db.Exec(migrationTableSQL)
	if err != nil {
		t.Fatalf("failed to create schema_migrations table: %v", err)
	}

	migrator := New(db)
	tx, _ := db.BeginTx(context.Background(), nil)
	defer func() { _ = tx.Rollback() }()

	migration := &mockMigration{
		id:          "1",
		description: "test",
		downQueries: []string{"INVALID SQL STATEMENT"},
	}

	migrationMap := map[string]Migration{
		"1": migration,
	}

	migrationStatus := MigrationStatus{ID: "1"}

	err = migrator.rollbackSingleMigration(context.Background(), tx, migrationStatus, migrationMap)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrMigrationFailed) {
		t.Errorf("expected error to be ErrMigrationFailed, got %v", err)
	}
}

func TestMigrator_rollbackSingleMigration_DeleteError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	migrator := New(db)
	tx, _ := db.BeginTx(context.Background(), nil)
	defer func() { _ = tx.Rollback() }()

	migrationMap := make(map[string]Migration)
	migrationStatus := MigrationStatus{ID: "1"}

	err = migrator.rollbackSingleMigration(context.Background(), tx, migrationStatus, migrationMap)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrMigrationFailed) {
		t.Errorf("expected error to be ErrMigrationFailed, got %v", err)
	}
}

func TestMigrator_MigrateUp_NoMigrations(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	migrator := New(db)
	err = migrator.MigrateUp([]Migration{})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestMigrator_MigrateUp_Success(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	migrations := []Migration{
		&mockMigration{
			id:          "1",
			description: "create users table",
			upQueries:   []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"},
		},
		&mockMigration{
			id:          "2",
			description: "add email column",
			upQueries:   []string{"ALTER TABLE users ADD COLUMN email TEXT"},
		},
	}

	migrator := New(db)
	err = migrator.MigrateUp(migrations)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count)
	if err != nil {
		t.Errorf("failed to count migrations: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 migrations, got %d", count)
	}
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='users'").Scan(&count)
	if err != nil {
		t.Errorf("failed to check table existence: %v", err)
	}
	if count != 1 {
		t.Error("expected users table to exist")
	}
}

func TestMigrator_MigrateUp_AlreadyApplied(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	migrations := []Migration{
		&mockMigration{
			id:          "1",
			description: "create users table",
			upQueries:   []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"},
		},
	}

	migrator := New(db)
	err = migrator.MigrateUp(migrations)
	if err != nil {
		t.Fatalf("failed to apply initial migrations: %v", err)
	}
	err = migrator.MigrateUp(migrations)
	if err != nil {
		t.Errorf("expected no error when applying already applied migrations, got %v", err)
	}
	var count int
	err = db.QueryRow("SELECT COUNT(DISTINCT batch) FROM schema_migrations").Scan(&count)
	if err != nil {
		t.Errorf("failed to count batches: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 batch, got %d", count)
	}
}

func TestMigrator_MigrateUp_TransactionError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	db.Close()

	migrations := []Migration{
		&mockMigration{
			id:          "1",
			description: "test migration",
			upQueries:   []string{"SELECT 1"},
		},
	}

	migrator := New(db)
	err = migrator.MigrateUp(migrations)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrFailedToGetAppliedMigrations) {
		t.Errorf("expected error to be ErrFailedToGetAppliedMigrations, got %v", err)
	}
}

func TestMigrator_MigrateDown_NoMigrations(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	migrator := New(db)
	err = migrator.MigrateDown(1, []Migration{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrNoMigrationsToRollback) {
		t.Errorf("expected error to be ErrNoMigrationsToRollback, got %v", err)
	}
}

func TestMigrator_MigrateDown_Success(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	migrations := []Migration{
		&mockMigration{
			id:          "1",
			description: "create users table",
			upQueries:   []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"},
			downQueries: []string{"DROP TABLE users"},
		},
	}

	migrator := New(db)
	err = migrator.MigrateUp(migrations)
	if err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}
	err = migrator.MigrateDown(1, migrations)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count)
	if err != nil {
		t.Errorf("failed to count migrations: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 migrations, got %d", count)
	}
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='users'").Scan(&count)
	if err != nil {
		t.Errorf("failed to check table existence: %v", err)
	}
	if count != 0 {
		t.Error("expected users table to be dropped")
	}
}

func TestMigrator_MigrateDown_TransactionError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	db.Close()

	migrator := New(db)
	err = migrator.MigrateDown(1, []Migration{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrFailedToGetAppliedMigrations) {
		t.Errorf("expected error to be ErrFailedToGetAppliedMigrations, got %v", err)
	}
}

func TestMigrator_Status_Success(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	migrations := []Migration{
		&mockMigration{
			id:          "1",
			description: "create users table",
			upQueries:   []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"},
		},
	}

	migrator := New(db)
	err = migrator.MigrateUp(migrations)
	if err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}
	status, err := migrator.Status()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if len(status) != 1 {
		t.Errorf("expected 1 migration status, got %d", len(status))
	}

	if status[0].ID != "1" {
		t.Errorf("expected migration ID '1', got '%s'", status[0].ID)
	}

	if status[0].Description != "create users table" {
		t.Errorf("expected description 'create users table', got '%s'", status[0].Description)
	}

	if status[0].AppliedAt == nil {
		t.Error("expected AppliedAt to be non-nil")
	}

	if status[0].Batch != 1 {
		t.Errorf("expected batch 1, got %d", status[0].Batch)
	}
}

func TestMigrator_Status_Error(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	db.Close()

	migrator := New(db)
	_, err = migrator.Status()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestMigrator_executeMigrationBatch_Success(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	// Create the schema_migrations table first
	_, err = db.Exec(migrationTableSQL)
	if err != nil {
		t.Fatalf("failed to create schema_migrations table: %v", err)
	}

	migrator := New(db)
	migrations := []Migration{
		&mockMigration{
			id:          "1",
			description: "test migration",
			upQueries:   []string{"CREATE TABLE test (id INTEGER)"},
		},
	}

	err = migrator.executeMigrationBatch(context.Background(), migrations, 1)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM schema_migrations WHERE id = ?", "1").Scan(&count)
	if err != nil {
		t.Errorf("failed to check migration record: %v", err)
	}
	if count != 1 {
		t.Error("expected migration to be recorded")
	}
}

func TestMigrator_executeMigrationBatch_TransactionError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	db.Close()

	migrator := New(db)
	migrations := []Migration{
		&mockMigration{
			id:          "1",
			description: "test migration",
			upQueries:   []string{"SELECT 1"},
		},
	}

	err = migrator.executeMigrationBatch(context.Background(), migrations, 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrFailedToBeginTransaction) && !errors.Is(err, ErrMigrationFailed) {
		t.Errorf("expected transaction or migration error, got %v", err)
	}
}

func TestMigrator_executeRollback_Success(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	// Create the schema_migrations table first
	_, err = db.Exec(migrationTableSQL)
	if err != nil {
		t.Fatalf("failed to create schema_migrations table: %v", err)
	}

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	migrator := New(db)
	rollbackList := []MigrationStatus{
		{ID: "1", Description: "test migration", Batch: 1},
	}

	migrationMap := map[string]Migration{
		"1": &mockMigration{
			id:          "1",
			description: "test migration",
			downQueries: []string{"DROP TABLE test"},
		},
	}

	err = migrator.executeRollback(context.Background(), rollbackList, migrationMap)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='test'").Scan(&count)
	if err != nil {
		t.Errorf("failed to check table existence: %v", err)
	}
	if count != 0 {
		t.Error("expected test table to be dropped")
	}
}

func TestMigrator_executeRollback_TransactionError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	db.Close()

	migrator := New(db)
	rollbackList := []MigrationStatus{
		{ID: "1", Description: "test migration", Batch: 1},
	}
	migrationMap := map[string]Migration{}

	err = migrator.executeRollback(context.Background(), rollbackList, migrationMap)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrFailedToBeginTransaction) && !errors.Is(err, ErrMigrationFailed) {
		t.Errorf("expected transaction or migration error, got %v", err)
	}
}

func TestMigrator_getAppliedMigrations_Success(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	// Create the schema_migrations table first
	_, err = db.Exec(migrationTableSQL)
	if err != nil {
		t.Fatalf("failed to create schema_migrations table: %v", err)
	}

	_, err = db.Exec("INSERT INTO schema_migrations (id, description, batch) VALUES (?, ?, ?)", "1", "first migration", 1)
	if err != nil {
		t.Fatalf("failed to insert first migration: %v", err)
	}

	_, err = db.Exec("INSERT INTO schema_migrations (id, description, batch) VALUES (?, ?, ?)", "2", "second migration", 1)
	if err != nil {
		t.Fatalf("failed to insert second migration: %v", err)
	}

	migrator := New(db)
	migrations, err := migrator.getAppliedMigrations(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if len(migrations) != 2 {
		t.Errorf("expected 2 migrations, got %d", len(migrations))
	}

	if migrations[0].ID != "1" {
		t.Errorf("expected first migration ID '1', got '%s'", migrations[0].ID)
	}

	if migrations[1].ID != "2" {
		t.Errorf("expected second migration ID '2', got '%s'", migrations[1].ID)
	}
}

func TestMigrator_getAppliedMigrations_CreateTableError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	db.Close()

	migrator := New(db)
	_, err = migrator.getAppliedMigrations(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrFailedToCreateSchemaMigrationsTable) && !errors.Is(err, ErrFailedToCreateSchemaMigrationsIndex) {
		t.Errorf("expected schema migration table creation error, got %v", err)
	}
}

func TestMigrator_getAppliedMigrations_QueryError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
CREATE TABLE schema_migrations (
    id VARCHAR(255) PRIMARY KEY,
    invalid_column BLOB NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch INTEGER NOT NULL
)`)
	if err != nil {
		t.Fatalf("failed to create invalid schema_migrations table: %v", err)
	}

	migrator := New(db)
	_, err = migrator.getAppliedMigrations(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
