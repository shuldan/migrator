package migrator

import (
	"errors"
	"strings"
	"testing"
)

func TestBaseMigration_Accessors(t *testing.T) {
	t.Parallel()
	m := &baseMigration{
		id:           "001",
		description:  "create users",
		upQueries:    []string{"CREATE TABLE users;"},
		downQueries:  []string{"DROP TABLE users;"},
		irreversible: true,
		disableTx:    true,
	}
	if m.ID() != "001" {
		t.Errorf("expected id %q, got %q", "001", m.ID())
	}
	if m.Description() != "create users" {
		t.Errorf("expected description %q, got %q", "create users", m.Description())
	}
	if len(m.Up()) != 1 || m.Up()[0] != "CREATE TABLE users;" {
		t.Errorf("unexpected Up: %v", m.Up())
	}
	if len(m.Down()) != 1 || m.Down()[0] != "DROP TABLE users;" {
		t.Errorf("unexpected Down: %v", m.Down())
	}
	if !m.Irreversible() {
		t.Error("expected irreversible true")
	}
	if !m.DisableTx() {
		t.Error("expected disableTx true")
	}
}

func TestBaseMigration_AddUp(t *testing.T) {
	t.Parallel()
	m := &baseMigration{}
	m.addUp("Q1")
	m.addUp("Q2")
	if len(m.upQueries) != 2 || m.upQueries[0] != "Q1" || m.upQueries[1] != "Q2" {
		t.Errorf("unexpected upQueries: %v", m.upQueries)
	}
}

func TestBaseMigration_AddDown(t *testing.T) {
	t.Parallel()
	m := &baseMigration{}
	m.addDown("D1")
	m.addDown("D2")
	if len(m.downQueries) != 2 || m.downQueries[0] != "D2" || m.downQueries[1] != "D1" {
		t.Errorf("expected prepend order, got %v", m.downQueries)
	}
}

func TestCreateMigration_Build_Success(t *testing.T) {
	t.Parallel()
	m, err := CreateMigration("001", "test").RawUp("SELECT 1").Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.ID() != "001" {
		t.Errorf("expected id %q, got %q", "001", m.ID())
	}
}

func TestCreateMigration_Build_EmptyID(t *testing.T) {
	t.Parallel()
	_, err := CreateMigration("", "desc").RawUp("SELECT 1").Build()
	if err == nil || !errors.Is(err, ErrEmptyMigrationID) {
		t.Errorf("expected ErrEmptyMigrationID, got %v", err)
	}
}

func TestCreateMigration_Build_EmptyDescription(t *testing.T) {
	t.Parallel()
	_, err := CreateMigration("001", "").RawUp("SELECT 1").Build()
	if err == nil || !errors.Is(err, ErrEmptyMigrationDescription) {
		t.Errorf("expected ErrEmptyMigrationDescription, got %v", err)
	}
}

func TestCreateMigration_Build_NoUpQueries(t *testing.T) {
	t.Parallel()
	_, err := CreateMigration("001", "desc").Build()
	if err == nil || !errors.Is(err, ErrNoUpQueries) {
		t.Errorf("expected ErrNoUpQueries, got %v", err)
	}
}

func TestCreateMigration_Build_MultipleErrors(t *testing.T) {
	t.Parallel()
	_, err := CreateMigration("", "").Build()
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrEmptyMigrationID) || !errors.Is(err, ErrEmptyMigrationDescription) || !errors.Is(err, ErrNoUpQueries) {
		t.Errorf("expected all three errors, got %v", err)
	}
}

func TestMustBuild_Success(t *testing.T) {
	t.Parallel()
	m := CreateMigration("001", "desc").RawUp("SELECT 1").MustBuild()
	if m.ID() != "001" {
		t.Errorf("expected id %q, got %q", "001", m.ID())
	}
}

func TestMustBuild_Panic(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "migrator:") {
			t.Errorf("unexpected panic: %v", r)
		}
	}()
	CreateMigration("", "").MustBuild()
}

func TestMigrationBuilder_MarkIrreversible(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").RawUp("Q").MarkIrreversible().Build()
	if !m.Irreversible() {
		t.Error("expected irreversible")
	}
}

func TestMigrationBuilder_DisableTransaction(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").RawUp("Q").DisableTransaction().Build()
	if !m.DisableTx() {
		t.Error("expected disableTx")
	}
}

func TestMigrationBuilder_CreateTable(t *testing.T) {
	t.Parallel()
	b := CreateMigration("001", "d").CreateTable("users", "id INT", "name TEXT")
	m, err := b.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m.Up()) != 1 || !strings.Contains(m.Up()[0], "CREATE TABLE") {
		t.Errorf("unexpected up: %v", m.Up())
	}
	if len(m.Down()) != 1 || !strings.Contains(m.Down()[0], "DROP TABLE") {
		t.Errorf("unexpected down: %v", m.Down())
	}
}

func TestMigrationBuilder_DropTable(t *testing.T) {
	t.Parallel()
	b := CreateMigration("001", "d").DropTable("users")
	m, _ := b.Build()
	if !strings.Contains(m.Up()[0], "DROP TABLE") {
		t.Errorf("expected DROP TABLE in up, got %v", m.Up())
	}
}

func TestMigrationBuilder_RenameTable(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").RenameTable("old", "new").Build()
	if !strings.Contains(m.Up()[0], "old RENAME TO new") {
		t.Errorf("unexpected up: %v", m.Up())
	}
	if !strings.Contains(m.Down()[0], "new RENAME TO old") {
		t.Errorf("unexpected down: %v", m.Down())
	}
}

func TestMigrationBuilder_AddColumn_Valid(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").AddColumn("users", "email TEXT").Build()
	if !strings.Contains(m.Up()[0], "ADD COLUMN email TEXT") {
		t.Errorf("unexpected up: %v", m.Up())
	}
	if !strings.Contains(m.Down()[0], "DROP COLUMN email") {
		t.Errorf("unexpected down: %v", m.Down())
	}
}

func TestMigrationBuilder_AddColumn_InvalidDef(t *testing.T) {
	t.Parallel()
	_, err := CreateMigration("001", "d").AddColumn("t", "onlyname").Build()
	if err == nil || !errors.Is(err, ErrInvalidColumnDefinition) {
		t.Errorf("expected ErrInvalidColumnDefinition, got %v", err)
	}
}

func TestMigrationBuilder_DropColumn(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		RawUp("filler").
		DropColumn("users", "email").Build()
	found := false
	for _, q := range m.Up() {
		if strings.Contains(q, "DROP COLUMN email") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected DROP COLUMN in up queries")
	}
}

func TestMigrationBuilder_RenameColumn(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		RenameColumn("users", "old_col", "new_col").Build()
	if !strings.Contains(m.Up()[0], "RENAME COLUMN old_col TO new_col") {
		t.Errorf("unexpected up: %v", m.Up())
	}
	if !strings.Contains(m.Down()[0], "RENAME COLUMN new_col TO old_col") {
		t.Errorf("unexpected down: %v", m.Down())
	}
}

func TestMigrationBuilder_ChangeColumn(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		ChangeColumn("users", "name", "TYPE VARCHAR(500)").Build()
	if !strings.Contains(m.Up()[0], "ALTER COLUMN name TYPE VARCHAR(500)") {
		t.Errorf("unexpected up: %v", m.Up())
	}
}

func TestMigrationBuilder_CreateIndex(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		CreateIndex("idx_email", "users", "email").Build()
	if !strings.Contains(m.Up()[0], "CREATE INDEX idx_email ON users") {
		t.Errorf("unexpected up: %v", m.Up())
	}
	if !strings.Contains(m.Down()[0], "DROP INDEX") {
		t.Errorf("unexpected down: %v", m.Down())
	}
}

func TestMigrationBuilder_CreateUniqueIndex(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		CreateUniqueIndex("idx_u", "users", "email").Build()
	if !strings.Contains(m.Up()[0], "CREATE UNIQUE INDEX") {
		t.Errorf("unexpected up: %v", m.Up())
	}
}

func TestMigrationBuilder_DropIndex(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		DropIndex("idx_email").Build()
	if !strings.Contains(m.Up()[0], "DROP INDEX") {
		t.Errorf("unexpected up: %v", m.Up())
	}
}

func TestMigrationBuilder_AddForeignKey(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		AddForeignKey("orders", "user_id", "users", "id").Build()
	if !strings.Contains(m.Up()[0], "fk_orders_user_id") {
		t.Errorf("unexpected up: %v", m.Up())
	}
}

func TestMigrationBuilder_AddForeignKeyWithName(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		AddForeignKeyWithName("orders", "my_fk", "user_id", "users", "id").Build()
	if !strings.Contains(m.Up()[0], "my_fk") {
		t.Errorf("unexpected up: %v", m.Up())
	}
	if !strings.Contains(m.Down()[0], "DROP CONSTRAINT") {
		t.Errorf("unexpected down: %v", m.Down())
	}
}

func TestMigrationBuilder_DropForeignKey(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		DropForeignKey("orders", "fk_orders_user").Build()
	if !strings.Contains(m.Up()[0], "DROP CONSTRAINT") {
		t.Errorf("unexpected up: %v", m.Up())
	}
}

func TestMigrationBuilder_AddPrimaryKey(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		AddPrimaryKey("users", "pk_users", "id").Build()
	if !strings.Contains(m.Up()[0], "PRIMARY KEY") {
		t.Errorf("unexpected up: %v", m.Up())
	}
}

func TestMigrationBuilder_AddUniqueConstraint(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		AddUniqueConstraint("users", "uq_email", "email").Build()
	if !strings.Contains(m.Up()[0], "UNIQUE") {
		t.Errorf("unexpected up: %v", m.Up())
	}
}

func TestMigrationBuilder_AddCheck(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		AddCheck("users", "ck_age", "age > 0").Build()
	if !strings.Contains(m.Up()[0], "CHECK") {
		t.Errorf("unexpected up: %v", m.Up())
	}
}

func TestMigrationBuilder_CreateExtension(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		CreateExtension("uuid-ossp").Build()
	if !strings.Contains(m.Up()[0], "CREATE EXTENSION") {
		t.Errorf("unexpected up: %v", m.Up())
	}
	if !strings.Contains(m.Down()[0], "DROP EXTENSION") {
		t.Errorf("unexpected down: %v", m.Down())
	}
}

func TestMigrationBuilder_Raw(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		Raw("UP QUERY", "DOWN QUERY").Build()
	if m.Up()[0] != "UP QUERY" {
		t.Errorf("unexpected up: %v", m.Up())
	}
	if m.Down()[0] != "DOWN QUERY" {
		t.Errorf("unexpected down: %v", m.Down())
	}
}

func TestMigrationBuilder_RawDown(t *testing.T) {
	t.Parallel()
	m, _ := CreateMigration("001", "d").
		RawUp("UP").RawDown("DOWN").Build()
	if len(m.Down()) != 1 || m.Down()[0] != "DOWN" {
		t.Errorf("unexpected down: %v", m.Down())
	}
}
