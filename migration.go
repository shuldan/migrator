package migrator

import (
	"errors"
	"fmt"
	"strings"
)

type Migration interface {
	ID() string
	Description() string
	Up() []string
	Down() []string
	Irreversible() bool
	DisableTx() bool
}

type baseMigration struct {
	id           string
	description  string
	upQueries    []string
	downQueries  []string
	irreversible bool
	disableTx    bool
}

func (m *baseMigration) ID() string          { return m.id }
func (m *baseMigration) Description() string { return m.description }
func (m *baseMigration) Up() []string        { return m.upQueries }
func (m *baseMigration) Down() []string      { return m.downQueries }
func (m *baseMigration) Irreversible() bool  { return m.irreversible }
func (m *baseMigration) DisableTx() bool     { return m.disableTx }

func (m *baseMigration) addUp(query string) {
	m.upQueries = append(m.upQueries, query)
}

func (m *baseMigration) addDown(query string) {
	m.downQueries = append([]string{query}, m.downQueries...)
}

type MigrationBuilder struct {
	migration *baseMigration
	errors    []error
}

func CreateMigration(id, description string) *MigrationBuilder {
	return &MigrationBuilder{
		migration: &baseMigration{
			id:          id,
			description: description,
			upQueries:   make([]string, 0),
			downQueries: make([]string, 0),
		},
	}
}

func (b *MigrationBuilder) Build() (Migration, error) {
	if b.migration.id == "" {
		b.errors = append(b.errors, ErrEmptyMigrationID)
	}
	if b.migration.description == "" {
		b.errors = append(b.errors, ErrEmptyMigrationDescription)
	}
	if len(b.migration.upQueries) == 0 {
		b.errors = append(b.errors, ErrNoUpQueries)
	}
	if len(b.errors) > 0 {
		return nil, errors.Join(b.errors...)
	}
	return b.migration, nil
}

func (b *MigrationBuilder) MustBuild() Migration {
	m, err := b.Build()
	if err != nil {
		panic(fmt.Sprintf("migrator: invalid migration: %v", err))
	}
	return m
}

func (b *MigrationBuilder) MarkIrreversible() *MigrationBuilder {
	b.migration.irreversible = true
	return b
}

func (b *MigrationBuilder) DisableTransaction() *MigrationBuilder {
	b.migration.disableTx = true
	return b
}

func (b *MigrationBuilder) CreateTable(tableName string, columns ...string) *MigrationBuilder {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s\n);",
		tableName, strings.Join(columns, ",\n    "))
	b.migration.addUp(query)
	b.migration.addDown(fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	return b
}

func (b *MigrationBuilder) DropTable(tableName string) *MigrationBuilder {
	b.migration.addUp(fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	return b
}

func (b *MigrationBuilder) RenameTable(oldName, newName string) *MigrationBuilder {
	b.migration.addUp(fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", oldName, newName))
	b.migration.addDown(fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", newName, oldName))
	return b
}

func (b *MigrationBuilder) AddColumn(tableName, columnDef string) *MigrationBuilder {
	fields := strings.Fields(columnDef)
	if len(fields) < 2 {
		b.errors = append(b.errors, fmt.Errorf("%w: %q", ErrInvalidColumnDefinition, columnDef))
		return b
	}

	b.migration.addUp(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", tableName, columnDef))
	b.migration.addDown(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", tableName, fields[0]))
	return b
}

func (b *MigrationBuilder) DropColumn(tableName, columnName string) *MigrationBuilder {
	b.migration.addUp(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", tableName, columnName))
	return b
}

func (b *MigrationBuilder) RenameColumn(tableName, oldName, newName string) *MigrationBuilder {
	b.migration.addUp(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", tableName, oldName, newName))
	b.migration.addDown(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", tableName, newName, oldName))
	return b
}

func (b *MigrationBuilder) ChangeColumn(tableName, columnName, newDefinition string) *MigrationBuilder {
	b.migration.addUp(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s;", tableName, columnName, newDefinition))
	return b
}

func (b *MigrationBuilder) CreateIndex(indexName, tableName string, columns ...string) *MigrationBuilder {
	query := fmt.Sprintf("CREATE INDEX %s ON %s (%s);",
		indexName, tableName, strings.Join(columns, ", "))
	b.migration.addUp(query)
	b.migration.addDown(fmt.Sprintf("DROP INDEX IF EXISTS %s;", indexName))
	return b
}

func (b *MigrationBuilder) CreateUniqueIndex(indexName, tableName string, columns ...string) *MigrationBuilder {
	query := fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s);",
		indexName, tableName, strings.Join(columns, ", "))
	b.migration.addUp(query)
	b.migration.addDown(fmt.Sprintf("DROP INDEX IF EXISTS %s;", indexName))
	return b
}

func (b *MigrationBuilder) DropIndex(indexName string) *MigrationBuilder {
	b.migration.addUp(fmt.Sprintf("DROP INDEX IF EXISTS %s;", indexName))
	return b
}

func (b *MigrationBuilder) AddForeignKey(tableName, columnName, refTable, refColumn string) *MigrationBuilder {
	constraintName := fmt.Sprintf("fk_%s_%s", tableName, columnName)
	return b.AddForeignKeyWithName(tableName, constraintName, columnName, refTable, refColumn)
}

func (b *MigrationBuilder) AddForeignKeyWithName(tableName, constraintName, columnName, refTable, refColumn string) *MigrationBuilder {
	query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s);",
		tableName, constraintName, columnName, refTable, refColumn)
	b.migration.addUp(query)
	b.migration.addDown(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", tableName, constraintName))
	return b
}

func (b *MigrationBuilder) DropForeignKey(tableName, constraintName string) *MigrationBuilder {
	b.migration.addUp(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", tableName, constraintName))
	return b
}

func (b *MigrationBuilder) AddPrimaryKey(tableName, constraintName string, columns ...string) *MigrationBuilder {
	query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s);",
		tableName, constraintName, strings.Join(columns, ", "))
	b.migration.addUp(query)
	b.migration.addDown(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", tableName, constraintName))
	return b
}

func (b *MigrationBuilder) AddUniqueConstraint(tableName, constraintName string, columns ...string) *MigrationBuilder {
	query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s);",
		tableName, constraintName, strings.Join(columns, ", "))
	b.migration.addUp(query)
	b.migration.addDown(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", tableName, constraintName))
	return b
}

func (b *MigrationBuilder) AddCheck(tableName, constraintName, condition string) *MigrationBuilder {
	query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);",
		tableName, constraintName, condition)
	b.migration.addUp(query)
	b.migration.addDown(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", tableName, constraintName))
	return b
}

func (b *MigrationBuilder) CreateExtension(name string) *MigrationBuilder {
	b.migration.addUp(fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %q;", name))
	b.migration.addDown(fmt.Sprintf("DROP EXTENSION IF EXISTS %q;", name))
	return b
}

func (b *MigrationBuilder) RawUp(query string) *MigrationBuilder {
	b.migration.addUp(query)
	return b
}

func (b *MigrationBuilder) RawDown(query string) *MigrationBuilder {
	b.migration.addDown(query)
	return b
}

func (b *MigrationBuilder) Raw(upQuery, downQuery string) *MigrationBuilder {
	b.migration.addUp(upQuery)
	b.migration.addDown(downQuery)
	return b
}
