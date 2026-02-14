package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func newTestMigrator(t *testing.T, opts ...Option) (*Migrator, *sql.DB) {
	t.Helper()
	db := openTestDB(t)
	defaultOpts := []Option{WithDialect(DialectSQLite)}
	allOpts := append(defaultOpts, opts...)
	m := New(db, allOpts...)
	return m, db
}

func buildTestMigration(t *testing.T, id, desc, upSQL string) Migration {
	t.Helper()
	m, err := CreateMigration(id, desc).RawUp(upSQL).Build()
	if err != nil {
		t.Fatalf("failed to build migration: %v", err)
	}
	return m
}

type failingLock struct {
	lockErr error
}

func (f *failingLock) Lock(context.Context) error   { return f.lockErr }
func (f *failingLock) Unlock(context.Context) error { return nil }

type recordingLogger struct {
	infoMsgs  []string
	errorMsgs []string
}

func (l *recordingLogger) Info(msg string, args ...any) {
	l.infoMsgs = append(l.infoMsgs, fmt.Sprintf(msg, args...))
}

func (l *recordingLogger) Error(msg string, args ...any) {
	l.errorMsgs = append(l.errorMsgs, fmt.Sprintf(msg, args...))
}

func TestNew_Defaults(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	if m.tableName != defaultTableName {
		t.Errorf("expected %q, got %q", defaultTableName, m.tableName)
	}
	if m.Dialect() != DialectSQLite {
		t.Errorf("expected SQLite, got %v", m.Dialect())
	}
}

func TestQualifiedTableName_WithSchema(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t, WithSchema("public"))
	expected := "public.schema_migrations"
	if got := m.qualifiedTableName(); got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestQualifiedTableName_WithoutSchema(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	if got := m.qualifiedTableName(); got != defaultTableName {
		t.Errorf("expected %q, got %q", defaultTableName, got)
	}
}

func TestRegister_Success(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	mig := buildTestMigration(t, "001", "first", "SELECT 1")
	if err := m.Register(mig); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m.migrations) != 1 {
		t.Errorf("expected 1 migration, got %d", len(m.migrations))
	}
}

func TestRegister_NilMigration(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	err := m.Register(nil)
	if !errors.Is(err, ErrNilMigration) {
		t.Errorf("expected ErrNilMigration, got %v", err)
	}
}

func TestRegister_DuplicateID(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	mig1 := buildTestMigration(t, "001", "first", "SELECT 1")
	mig2 := buildTestMigration(t, "001", "second", "SELECT 2")
	_ = m.Register(mig1)
	err := m.Register(mig2)
	if !errors.Is(err, ErrDuplicateMigrationID) {
		t.Errorf("expected ErrDuplicateMigrationID, got %v", err)
	}
}

func TestRegister_Sorting(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	m2 := buildTestMigration(t, "002", "second", "SELECT 2")
	m1 := buildTestMigration(t, "001", "first", "SELECT 1")
	_ = m.Register(m2, m1)
	if m.migrations[0].ID() != "001" || m.migrations[1].ID() != "002" {
		t.Errorf("expected sorted, got %v, %v", m.migrations[0].ID(), m.migrations[1].ID())
	}
}

func TestRegister_DuplicateInSameCall(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	mig1 := buildTestMigration(t, "001", "first", "SELECT 1")
	mig2 := buildTestMigration(t, "001", "second", "SELECT 2")
	err := m.Register(mig1, mig2)
	if !errors.Is(err, ErrDuplicateMigrationID) {
		t.Errorf("expected ErrDuplicateMigrationID, got %v", err)
	}
}

func TestMustRegister_Success(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	mig := buildTestMigration(t, "001", "first", "SELECT 1")
	m.MustRegister(mig)
}

func TestMustRegister_Panic(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	m, _ := newTestMigrator(t)
	m.MustRegister(nil)
}

func TestUp_NoPending(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	if err := m.Up(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUp_ApplyMigrations(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "create t", "CREATE TABLE t (id INTEGER)")
	_ = m.Register(mig)
	if err := m.Up(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatalf("query error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 applied, got %d", count)
	}
}

func TestUp_AlreadyApplied(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "create t", "CREATE TABLE t (id INTEGER)")
	_ = m.Register(mig)
	_ = m.Up(ctx)
	if err := m.Up(ctx); err != nil {
		t.Fatalf("unexpected error on second Up: %v", err)
	}
}

func TestUp_FailedQuery(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "bad", "INVALID SQL SYNTAX HERE")
	_ = m.Register(mig)
	err := m.Up(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	var migErr *MigrationError
	if !errors.As(err, &migErr) {
		t.Errorf("expected MigrationError, got %T", err)
	}
}

func TestUp_LockFailure(t *testing.T) {
	lockErr := errors.New("lock failed")
	m, _ := newTestMigrator(t, WithLockStrategy(&failingLock{lockErr: lockErr}))
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "t", "SELECT 1")
	_ = m.Register(mig)
	err := m.Up(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "lock failed") {
		t.Errorf("expected lock error, got %v", err)
	}
}

func TestUp_InitError(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	m := New(db, WithDialect(DialectSQLite))
	db.Close()
	err := m.Up(context.Background())
	if err == nil {
		t.Fatal("expected error from closed db")
	}
}

func TestUp_GetAppliedError(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	_ = m.ensureInitialized(ctx)
	_, _ = db.Exec("DROP TABLE schema_migrations")
	err := m.Up(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrFailedToGetAppliedMigrations) {
		t.Errorf("expected ErrFailedToGetAppliedMigrations, got %v", err)
	}
}

func TestDown_NoneApplied(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	err := m.Down(ctx, 1)
	if !errors.Is(err, ErrNoMigrationsToRollback) {
		t.Errorf("expected ErrNoMigrationsToRollback, got %v", err)
	}
}

func TestDown_InvalidSteps(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	err := m.Down(ctx, 0)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestDown_NegativeSteps(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	err := m.Down(ctx, -1)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestDown_Success(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	mig, _ := CreateMigration("001", "create t").
		Raw("CREATE TABLE t (id INTEGER)", "DROP TABLE t").Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	if err := m.Down(ctx, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 applied, got %d", count)
	}
}

func TestDown_Irreversible(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	mig, _ := CreateMigration("001", "create t").
		RawUp("CREATE TABLE t (id INTEGER)").
		MarkIrreversible().Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	err := m.Down(ctx, 1)
	if !errors.Is(err, ErrIrreversibleMigration) {
		t.Errorf("expected ErrIrreversibleMigration, got %v", err)
	}
}

func TestDown_IrreversibleWithForce(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	mig, _ := CreateMigration("001", "create t").
		RawUp("CREATE TABLE t (id INTEGER)").
		MarkIrreversible().Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	if err := m.Down(ctx, 1, WithForce()); err != nil {
		t.Fatalf("unexpected error with force: %v", err)
	}
}

func TestDown_LockFailure(t *testing.T) {
	lockErr := errors.New("lock failed")
	m, _ := newTestMigrator(t, WithLockStrategy(&failingLock{lockErr: lockErr}))
	mig := buildTestMigration(t, "001", "t", "SELECT 1")
	_ = m.Register(mig)
	err := m.Down(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDown_InitError(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	m := New(db, WithDialect(DialectSQLite))
	db.Close()
	err := m.Down(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error from closed db")
	}
}

func TestDown_GetAppliedError(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	_ = m.ensureInitialized(ctx)
	_, _ = db.Exec("DROP TABLE schema_migrations")
	err := m.Down(ctx, 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDownBatch_Success(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	m1, _ := CreateMigration("001", "create t1").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").Build()
	m2, _ := CreateMigration("002", "create t2").
		Raw("CREATE TABLE t2 (id INTEGER)", "DROP TABLE t2").Build()
	_ = m.Register(m1, m2)
	_ = m.Up(ctx)
	if err := m.DownBatch(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDownBatch_NoneApplied(t *testing.T) {
	m, _ := newTestMigrator(t)
	err := m.DownBatch(context.Background())
	if !errors.Is(err, ErrNoMigrationsToRollback) {
		t.Errorf("expected ErrNoMigrationsToRollback, got %v", err)
	}
}

func TestDownAll_Success(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	m1, _ := CreateMigration("001", "t1").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").Build()
	_ = m.Register(m1)
	_ = m.Up(ctx)
	if err := m.DownAll(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}

func TestDownAll_NoneApplied(t *testing.T) {
	m, _ := newTestMigrator(t)
	err := m.DownAll(context.Background())
	if !errors.Is(err, ErrNoMigrationsToRollback) {
		t.Errorf("expected ErrNoMigrationsToRollback, got %v", err)
	}
}

func TestStatus_AllStates(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	m1, _ := CreateMigration("001", "applied").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").Build()
	m2, _ := CreateMigration("002", "pending").
		RawUp("CREATE TABLE t2 (id INTEGER)").Build()
	_ = m.Register(m1, m2)
	_ = m.Up(ctx)
	m.mu.Lock()
	m.migrations = m.migrations[:1]
	m.mu.Unlock()
	m3 := buildTestMigration(t, "003", "pending2", "SELECT 1")
	_ = m.Register(m3)
	statuses, err := m.Status(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	stateMap := make(map[string]MigrationState)
	for _, s := range statuses {
		stateMap[s.ID] = s.State
	}
	if stateMap["001"] != MigrationStateApplied {
		t.Errorf("001 expected applied, got %v", stateMap["001"])
	}
	if stateMap["002"] != MigrationStateGhost {
		t.Errorf("002 expected ghost, got %v", stateMap["002"])
	}
	if stateMap["003"] != MigrationStatePending {
		t.Errorf("003 expected pending, got %v", stateMap["003"])
	}
}

func TestStatus_InitError(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	m := New(db, WithDialect(DialectSQLite))
	db.Close()
	_, err := m.Status(context.Background())
	if err == nil {
		t.Fatal("expected error from closed db")
	}
}

func TestStatus_GetAppliedError(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	_ = m.ensureInitialized(ctx)
	_, _ = db.Exec("DROP TABLE schema_migrations")
	_, err := m.Status(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPlan_ReturnsPending(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "test", "SELECT 1")
	_ = m.Register(mig)
	planned, err := m.Plan(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(planned) != 1 || planned[0].ID != "001" {
		t.Errorf("unexpected plan: %v", planned)
	}
}

func TestPlan_InitError(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	m := New(db, WithDialect(DialectSQLite))
	db.Close()
	_, err := m.Plan(context.Background())
	if err == nil {
		t.Fatal("expected error from closed db")
	}
}

func TestPlan_GetAppliedError(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	_ = m.ensureInitialized(ctx)
	_, _ = db.Exec("DROP TABLE schema_migrations")
	_, err := m.Plan(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPlanDown_ReturnsRollback(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	mig, _ := CreateMigration("001", "test").
		Raw("CREATE TABLE t (id INTEGER)", "DROP TABLE t").Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	planned, err := m.PlanDown(ctx, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(planned) != 1 || planned[0].ID != "001" {
		t.Errorf("unexpected plan: %v", planned)
	}
}

func TestPlanDown_InitError(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	m := New(db, WithDialect(DialectSQLite))
	db.Close()
	_, err := m.PlanDown(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error from closed db")
	}
}

func TestPlanDown_GetAppliedError(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	_ = m.ensureInitialized(ctx)
	_, _ = db.Exec("DROP TABLE schema_migrations")
	_, err := m.PlanDown(ctx, 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPlanDown_GhostMigration(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	_ = m.ensureInitialized(ctx)
	_, _ = db.Exec(
		"INSERT INTO schema_migrations (id, description, batch) VALUES (?, ?, ?)",
		"ghost001", "ghost", 1,
	)
	planned, err := m.PlanDown(ctx, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(planned) != 1 || len(planned[0].Queries) != 0 {
		t.Errorf("ghost should have no queries, got %v", planned)
	}
}

func TestUp_WithEmptyQuery(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	mig, _ := CreateMigration("001", "empty").
		RawUp("  ").
		RawUp("CREATE TABLE t (id INTEGER)").Build()
	_ = m.Register(mig)
	if err := m.Up(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDown_WithEmptyAndCommentQuery(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	mig, _ := CreateMigration("001", "create t").
		RawUp("CREATE TABLE t (id INTEGER)").
		RawDown("-- comment only").
		RawDown("  ").
		RawDown("DROP TABLE t").Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	if err := m.Down(ctx, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUp_WithBeforeHook_Error(t *testing.T) {
	hookErr := errors.New("before hook failed")
	m, _ := newTestMigrator(t, WithBeforeMigration(
		func(ctx context.Context, mig Migration) error { return hookErr },
	))
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "test", "SELECT 1")
	_ = m.Register(mig)
	err := m.Up(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "before hook") {
		t.Errorf("expected 'before hook' in error, got %v", err)
	}
}

func TestUp_WithAfterHook(t *testing.T) {
	var called bool
	m, _ := newTestMigrator(t, WithAfterMigration(
		func(ctx context.Context, mig Migration, d time.Duration, err error) {
			called = true
		},
	))
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "test", "CREATE TABLE t1 (id INTEGER)")
	_ = m.Register(mig)
	_ = m.Up(ctx)
	if !called {
		t.Error("expected after hook to be called")
	}
}

func TestUp_WithAfterHook_OnError(t *testing.T) {
	var hookErr error
	m, _ := newTestMigrator(t, WithAfterMigration(
		func(ctx context.Context, mig Migration, d time.Duration, err error) {
			hookErr = err
		},
	))
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "test", "INVALID SQL")
	_ = m.Register(mig)
	_ = m.Up(ctx)
	if hookErr == nil {
		t.Error("expected error passed to after hook")
	}
}

func TestUp_TxDisabled(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxDisabled))
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "test", "CREATE TABLE t1 (id INTEGER)")
	_ = m.Register(mig)
	if err := m.Up(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUp_TxPerBatch(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxPerBatch))
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "test", "CREATE TABLE t1 (id INTEGER)")
	_ = m.Register(mig)
	if err := m.Up(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUp_TxPerBatch_DisableTxMigration(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxPerBatch))
	ctx := context.Background()
	mig, _ := CreateMigration("001", "test").
		RawUp("CREATE TABLE t1 (id INTEGER)").
		DisableTransaction().Build()
	_ = m.Register(mig)
	if err := m.Up(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUp_TxPerBatch_MixedTxModes(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxPerBatch))
	ctx := context.Background()
	m1, _ := CreateMigration("001", "t1").
		RawUp("CREATE TABLE t1 (id INTEGER)").Build()
	m2, _ := CreateMigration("002", "t2").
		RawUp("CREATE TABLE t2 (id INTEGER)").
		DisableTransaction().Build()
	m3, _ := CreateMigration("003", "t3").
		RawUp("CREATE TABLE t3 (id INTEGER)").Build()
	_ = m.Register(m1, m2, m3)
	if err := m.Up(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUp_TxPerMigration_DisableTx(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxPerMigration))
	ctx := context.Background()
	mig, _ := CreateMigration("001", "t").
		RawUp("CREATE TABLE t1 (id INTEGER)").
		DisableTransaction().Build()
	_ = m.Register(mig)
	if err := m.Up(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUp_TxDisabled_FailedQuery(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxDisabled))
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "bad", "INVALID SQL")
	_ = m.Register(mig)
	err := m.Up(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestUp_TxPerBatch_FailedQuery(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxPerBatch))
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "bad", "INVALID SQL")
	_ = m.Register(mig)
	err := m.Up(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestUp_TxPerMigration_FailedQuery(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxPerMigration))
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "bad", "INVALID SQL")
	_ = m.Register(mig)
	err := m.Up(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDown_TxDisabled(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxDisabled))
	ctx := context.Background()
	mig, _ := CreateMigration("001", "t").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	if err := m.Down(ctx, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDown_TxPerBatch(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxPerBatch))
	ctx := context.Background()
	mig, _ := CreateMigration("001", "t").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	if err := m.DownAll(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDown_TxPerBatch_DisableTxMigration(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxPerBatch))
	ctx := context.Background()
	mig, _ := CreateMigration("001", "t").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").
		DisableTransaction().Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	if err := m.DownAll(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDown_TxPerBatch_MixedTxModes(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxPerBatch))
	ctx := context.Background()
	m1, _ := CreateMigration("001", "t1").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").Build()
	m2, _ := CreateMigration("002", "t2").
		Raw("CREATE TABLE t2 (id INTEGER)", "DROP TABLE t2").
		DisableTransaction().Build()
	m3, _ := CreateMigration("003", "t3").
		Raw("CREATE TABLE t3 (id INTEGER)", "DROP TABLE t3").Build()
	_ = m.Register(m1, m2, m3)
	_ = m.Up(ctx)
	if err := m.DownAll(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDown_PerMigration_DisableTx(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxPerMigration))
	ctx := context.Background()
	mig, _ := CreateMigration("001", "t").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").
		DisableTransaction().Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	if err := m.Down(ctx, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDown_GhostMigration(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	_ = m.ensureInitialized(ctx)
	_, _ = db.Exec(
		"INSERT INTO schema_migrations (id, description, batch) VALUES (?, ?, ?)",
		"ghost001", "ghost", 1,
	)
	if err := m.Down(ctx, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDown_FailedDownQuery(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	mig, _ := CreateMigration("001", "t").
		RawUp("CREATE TABLE t1 (id INTEGER)").
		RawDown("INVALID SQL FOR DOWN").Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	err := m.Down(ctx, 1)
	if err == nil {
		t.Fatal("expected error")
	}
	var migErr *MigrationError
	if !errors.As(err, &migErr) {
		t.Errorf("expected MigrationError, got %T: %v", err, err)
	}
}

func TestDown_TxDisabled_FailedDownQuery(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxDisabled))
	ctx := context.Background()
	mig, _ := CreateMigration("001", "t").
		RawUp("CREATE TABLE t1 (id INTEGER)").
		RawDown("INVALID SQL FOR DOWN").Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	err := m.Down(ctx, 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDown_TxPerBatch_FailedDownQuery(t *testing.T) {
	m, _ := newTestMigrator(t, WithTxStrategy(TxPerBatch))
	ctx := context.Background()
	mig, _ := CreateMigration("001", "t").
		RawUp("CREATE TABLE t1 (id INTEGER)").
		RawDown("INVALID SQL FOR DOWN").Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	err := m.DownAll(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDown_BeforeHook_Error(t *testing.T) {
	hookErr := errors.New("hook failed")
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	mig, _ := CreateMigration("001", "t").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	m.beforeMigration = func(ctx context.Context, mig Migration) error {
		return hookErr
	}
	err := m.Down(ctx, 1)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "before hook") {
		t.Errorf("expected 'before hook' in error, got %v", err)
	}
}

func TestDown_AfterHook_Called(t *testing.T) {
	var called bool
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	mig, _ := CreateMigration("001", "t").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").Build()
	_ = m.Register(mig)
	_ = m.Up(ctx)
	m.afterMigration = func(ctx context.Context, mig Migration, d time.Duration, err error) {
		called = true
	}
	_ = m.Down(ctx, 1)
	if !called {
		t.Error("expected after hook to be called on down")
	}
}

func TestUp_WithCustomLogger(t *testing.T) {
	logger := &recordingLogger{}
	m, _ := newTestMigrator(t, WithLogger(logger))
	ctx := context.Background()
	mig := buildTestMigration(t, "001", "t", "CREATE TABLE t1 (id INTEGER)")
	_ = m.Register(mig)
	_ = m.Up(ctx)
	if len(logger.infoMsgs) == 0 {
		t.Error("expected logger to have info messages")
	}
}

func TestNextBatchNumber(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	applied := []MigrationStatus{
		{ID: "001", Batch: 1},
		{ID: "002", Batch: 3},
		{ID: "003", Batch: 2},
	}
	got := m.nextBatchNumber(applied)
	if got != 4 {
		t.Errorf("expected 4, got %d", got)
	}
}

func TestNextBatchNumber_Empty(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	got := m.nextBatchNumber(nil)
	if got != 1 {
		t.Errorf("expected 1, got %d", got)
	}
}

func TestSortedForRollback(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	applied := []MigrationStatus{
		{ID: "001", Batch: 1},
		{ID: "003", Batch: 2},
		{ID: "002", Batch: 2},
	}
	sorted := m.sortedForRollback(applied)
	if sorted[0].ID != "003" || sorted[1].ID != "002" || sorted[2].ID != "001" {
		ids := make([]string, len(sorted))
		for i, s := range sorted {
			ids[i] = s.ID
		}
		t.Errorf("unexpected order: %v", ids)
	}
}

func TestRollbackListBySteps_MoreThanAvailable(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	applied := []MigrationStatus{{ID: "001", Batch: 1}}
	result := m.rollbackListBySteps(applied, 100)
	if len(result) != 1 {
		t.Errorf("expected 1, got %d", len(result))
	}
}

func TestRollbackListByLastBatch(t *testing.T) {
	t.Parallel()
	m, _ := newTestMigrator(t)
	applied := []MigrationStatus{
		{ID: "001", Batch: 1},
		{ID: "002", Batch: 2},
		{ID: "003", Batch: 2},
	}
	result := m.rollbackListByLastBatch(applied)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "003" {
		t.Errorf("expected 003 first, got %s", result[0].ID)
	}
}

func TestEnsureInitialized_Idempotent(t *testing.T) {
	m, _ := newTestMigrator(t)
	ctx := context.Background()
	if err := m.ensureInitialized(ctx); err != nil {
		t.Fatalf("first init: %v", err)
	}
	if err := m.ensureInitialized(ctx); err != nil {
		t.Fatalf("second init: %v", err)
	}
}

func TestEnsureInitialized_CreateTableError(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	m := New(db, WithDialect(DialectSQLite))
	db.Close()
	err := m.ensureInitialized(context.Background())
	if err == nil {
		t.Fatal("expected error from closed db")
	}
	if !errors.Is(err, ErrFailedToCreateSchemaMigrationsTable) {
		t.Errorf("expected ErrFailedToCreateSchemaMigrationsTable, got %v", err)
	}
}

func TestUp_MultipleMigrations_Batch(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	m1, _ := CreateMigration("001", "t1").
		RawUp("CREATE TABLE t1 (id INTEGER)").Build()
	m2, _ := CreateMigration("002", "t2").
		RawUp("CREATE TABLE t2 (id INTEGER)").Build()
	_ = m.Register(m1, m2)
	_ = m.Up(ctx)
	var batch int
	_ = db.QueryRow("SELECT batch FROM schema_migrations WHERE id = '002'").Scan(&batch)
	if batch != 1 {
		t.Errorf("expected batch 1, got %d", batch)
	}
}

func TestUp_TwoBatches(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	m1, _ := CreateMigration("001", "t1").
		RawUp("CREATE TABLE t1 (id INTEGER)").Build()
	_ = m.Register(m1)
	_ = m.Up(ctx)
	m2, _ := CreateMigration("002", "t2").
		RawUp("CREATE TABLE t2 (id INTEGER)").Build()
	_ = m.Register(m2)
	_ = m.Up(ctx)
	var batch int
	_ = db.QueryRow("SELECT batch FROM schema_migrations WHERE id = '002'").Scan(&batch)
	if batch != 2 {
		t.Errorf("expected batch 2, got %d", batch)
	}
}

func TestDownBatch_OnlyLastBatch(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	m1, _ := CreateMigration("001", "t1").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").Build()
	_ = m.Register(m1)
	_ = m.Up(ctx)
	m2, _ := CreateMigration("002", "t2").
		Raw("CREATE TABLE t2 (id INTEGER)", "DROP TABLE t2").Build()
	_ = m.Register(m2)
	_ = m.Up(ctx)
	_ = m.DownBatch(ctx)
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 remaining, got %d", count)
	}
}

func TestDown_MultipleSteps(t *testing.T) {
	m, db := newTestMigrator(t)
	ctx := context.Background()
	m1, _ := CreateMigration("001", "t1").
		Raw("CREATE TABLE t1 (id INTEGER)", "DROP TABLE t1").Build()
	m2, _ := CreateMigration("002", "t2").
		Raw("CREATE TABLE t2 (id INTEGER)", "DROP TABLE t2").Build()
	m3, _ := CreateMigration("003", "t3").
		Raw("CREATE TABLE t3 (id INTEGER)", "DROP TABLE t3").Build()
	_ = m.Register(m1, m2, m3)
	_ = m.Up(ctx)
	_ = m.Down(ctx, 2)
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 remaining, got %d", count)
	}
}
