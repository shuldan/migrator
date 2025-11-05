package migrator

import (
	"testing"
)

func TestCreateMigration(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "test migration")
	if builder == nil {
		t.Fatal("expected non-nil builder")
	}
	if builder.migration == nil {
		t.Fatal("expected non-nil migration")
	}
	if builder.migration.ID() != "1" {
		t.Errorf("expected ID '1', got '%s'", builder.migration.ID())
	}
	if builder.migration.Description() != "test migration" {
		t.Errorf("expected description 'test migration', got '%s'", builder.migration.Description())
	}
}

func TestMigrationBuilder_CreateTable(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "create users table")
	migration := builder.CreateTable("users", "id INTEGER PRIMARY KEY", "name TEXT").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "CREATE TABLE IF NOT EXISTS users (\n    id INTEGER PRIMARY KEY,\n    name TEXT\n);"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "DROP TABLE IF EXISTS users;"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_DropTable(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "drop users table")
	migration := builder.DropTable("users").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "DROP TABLE IF EXISTS users;"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "-- Cannot restore dropped table users"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_AddColumn(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "add email column")
	migration := builder.AddColumn("users", "email VARCHAR(255)").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "ALTER TABLE users ADD COLUMN email VARCHAR(255);"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "ALTER TABLE users DROP COLUMN email;"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_DropColumn(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "drop email column")
	migration := builder.DropColumn("users", "email").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "ALTER TABLE users DROP COLUMN email;"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "-- Cannot restore dropped column users.email without definition"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_RenameColumn(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "rename email column")
	migration := builder.RenameColumn("users", "email", "email_address").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "ALTER TABLE users RENAME COLUMN email TO email_address;"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "ALTER TABLE users RENAME COLUMN email_address TO email;"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_ChangeColumn(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "change email column")
	migration := builder.ChangeColumn("users", "email", "TYPE VARCHAR(500)").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "ALTER TABLE users ALTER COLUMN email TYPE VARCHAR(500);"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "-- Cannot reverse column change for users.email"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_CreateIndex(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "create index on users")
	migration := builder.CreateIndex("idx_users_name", "users", "name").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "CREATE INDEX idx_users_name ON users (name);"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "DROP INDEX IF EXISTS idx_users_name;"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_CreateUniqueIndex(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "create unique index on users")
	migration := builder.CreateUniqueIndex("idx_users_email", "users", "email").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "CREATE UNIQUE INDEX idx_users_email ON users (email);"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "DROP INDEX IF EXISTS idx_users_email;"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_DropIndex(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "drop index")
	migration := builder.DropIndex("idx_users_name").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "DROP INDEX IF EXISTS idx_users_name;"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "-- Cannot restore dropped index idx_users_name without definition"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_AddForeignKey(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "add foreign key")
	migration := builder.AddForeignKey("posts", "user_id", "users", "id").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "ALTER TABLE posts ADD CONSTRAINT fk_posts_user_id FOREIGN KEY (user_id) REFERENCES users(id);"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "ALTER TABLE posts DROP CONSTRAINT IF EXISTS fk_posts_user_id;"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_AddForeignKeyWithName(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "add named foreign key")
	migration := builder.AddForeignKeyWithName("posts", "fk_user_id", "user_id", "users", "id").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "ALTER TABLE posts ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id);"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "ALTER TABLE posts DROP CONSTRAINT IF EXISTS fk_user_id;"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_DropForeignKey(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "drop foreign key")
	migration := builder.DropForeignKey("posts", "fk_user_id").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "ALTER TABLE posts DROP CONSTRAINT IF EXISTS fk_user_id;"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "-- Cannot restore dropped foreign key fk_user_id"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_AddPrimaryKey(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "add primary key")
	migration := builder.AddPrimaryKey("users", "pk_users", "id").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id);"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "ALTER TABLE users DROP CONSTRAINT IF EXISTS pk_users;"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_AddCheck(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "add check constraint")
	migration := builder.AddCheck("users", "chk_email", "email LIKE '%@%'").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	expectedUp := "ALTER TABLE users ADD CONSTRAINT chk_email CHECK (email LIKE '%@%');"
	if migration.Up()[0] != expectedUp {
		t.Errorf("expected up query '%s', got '%s'", expectedUp, migration.Up()[0])
	}

	expectedDown := "ALTER TABLE users DROP CONSTRAINT IF EXISTS chk_email;"
	if migration.Down()[0] != expectedDown {
		t.Errorf("expected down query '%s', got '%s'", expectedDown, migration.Down()[0])
	}
}

func TestMigrationBuilder_Raw(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "raw queries")
	migration := builder.Raw("SELECT 1;", "SELECT 2;").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	if migration.Up()[0] != "SELECT 1;" {
		t.Errorf("expected up query 'SELECT 1;', got '%s'", migration.Up()[0])
	}

	if migration.Down()[0] != "SELECT 2;" {
		t.Errorf("expected down query 'SELECT 2;', got '%s'", migration.Down()[0])
	}
}

func TestMigrationBuilder_RawUp(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "raw up query")
	migration := builder.RawUp("SELECT 1;").Build()

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 0 {
		t.Errorf("expected 0 down queries, got %d", len(migration.Down()))
	}

	if migration.Up()[0] != "SELECT 1;" {
		t.Errorf("expected up query 'SELECT 1;', got '%s'", migration.Up()[0])
	}
}

func TestMigrationBuilder_RawDown(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "raw down query")
	migration := builder.RawDown("SELECT 2;").Build()

	if len(migration.Up()) != 0 {
		t.Errorf("expected 0 up queries, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	if migration.Down()[0] != "SELECT 2;" {
		t.Errorf("expected down query 'SELECT 2;', got '%s'", migration.Down()[0])
	}
}

func TestMigrationBuilder_Chaining(t *testing.T) {
	t.Parallel()

	builder := CreateMigration("1", "chained operations")
	migration := builder.
		CreateTable("users", "id INTEGER PRIMARY KEY").
		AddColumn("users", "name TEXT").
		CreateIndex("idx_users_name", "users", "name").
		Build()

	if len(migration.Up()) != 3 {
		t.Errorf("expected 3 up queries, got %d", len(migration.Up()))
	}
	if len(migration.Down()) != 3 {
		t.Errorf("expected 3 down queries, got %d", len(migration.Down()))
	}

	expectedUp := []string{
		"CREATE TABLE IF NOT EXISTS users (\n    id INTEGER PRIMARY KEY\n);",
		"ALTER TABLE users ADD COLUMN name TEXT;",
		"CREATE INDEX idx_users_name ON users (name);",
	}

	expectedDown := []string{
		"DROP INDEX IF EXISTS idx_users_name;",
		"ALTER TABLE users DROP COLUMN name;",
		"DROP TABLE IF EXISTS users;",
	}

	for i, query := range expectedUp {
		if migration.Up()[i] != query {
			t.Errorf("expected up query '%s' at index %d, got '%s'", query, i, migration.Up()[i])
		}
	}

	for i, query := range expectedDown {
		if migration.Down()[i] != query {
			t.Errorf("expected down query '%s' at index %d, got '%s'", query, i, migration.Down()[i])
		}
	}
}

func TestBaseMigration_AddUp(t *testing.T) {
	t.Parallel()

	migration := &baseMigration{
		upQueries: make([]string, 0),
	}

	result := migration.AddUp("SELECT 1;")
	if result != migration {
		t.Error("expected AddUp to return the same migration instance")
	}

	if len(migration.Up()) != 1 {
		t.Errorf("expected 1 up query, got %d", len(migration.Up()))
	}

	if migration.Up()[0] != "SELECT 1;" {
		t.Errorf("expected up query 'SELECT 1;', got '%s'", migration.Up()[0])
	}
}

func TestBaseMigration_AddDown(t *testing.T) {
	t.Parallel()

	migration := &baseMigration{
		downQueries: make([]string, 0),
	}

	result := migration.AddDown("SELECT 1;")
	if result != migration {
		t.Error("expected AddDown to return the same migration instance")
	}

	if len(migration.Down()) != 1 {
		t.Errorf("expected 1 down query, got %d", len(migration.Down()))
	}

	if migration.Down()[0] != "SELECT 1;" {
		t.Errorf("expected down query 'SELECT 1;', got '%s'", migration.Down()[0])
	}
}
