package migrator

import (
	"fmt"
	"strings"
	"time"
)

type Migration interface {
	ID() string
	Description() string
	Up() []string
	Down() []string
}

type MigrationStatus struct {
	ID          string
	Description string
	AppliedAt   *time.Time
	Batch       int
}

type baseMigration struct {
	id          string
	description string
	upQueries   []string
	downQueries []string
}

func (m *baseMigration) ID() string {
	return m.id
}

func (m *baseMigration) Description() string {
	return m.description
}

func (m *baseMigration) Up() []string {
	return m.upQueries
}

func (m *baseMigration) Down() []string {
	return m.downQueries
}

func (m *baseMigration) AddUp(query string) *baseMigration {
	m.upQueries = append(m.upQueries, query)
	return m
}

func (m *baseMigration) AddDown(query string) *baseMigration {
	m.downQueries = append([]string{query}, m.downQueries...)
	return m
}

type MigrationBuilder struct {
	migration *baseMigration
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

func (b *MigrationBuilder) CreateTable(tableName string, columns ...string) *MigrationBuilder {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s\n);",
		tableName, strings.Join(columns, ",\n    "))
	b.migration.AddUp(query)
	b.migration.AddDown(fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	return b
}

func (b *MigrationBuilder) DropTable(tableName string) *MigrationBuilder {
	b.migration.AddUp(fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	b.migration.AddDown(fmt.Sprintf("-- Cannot restore dropped table %s", tableName))
	return b
}

func (b *MigrationBuilder) AddColumn(tableName, columnDef string) *MigrationBuilder {
	b.migration.AddUp(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", tableName, columnDef))

	columnName := strings.Fields(columnDef)[0]
	b.migration.AddDown(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", tableName, columnName))
	return b
}

func (b *MigrationBuilder) DropColumn(tableName, columnName string) *MigrationBuilder {
	b.migration.AddUp(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", tableName, columnName))
	b.migration.AddDown(fmt.Sprintf("-- Cannot restore dropped column %s.%s without definition", tableName, columnName))
	return b
}

func (b *MigrationBuilder) RenameColumn(tableName, oldName, newName string) *MigrationBuilder {
	b.migration.AddUp(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", tableName, oldName, newName))
	b.migration.AddDown(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", tableName, newName, oldName))
	return b
}

func (b *MigrationBuilder) ChangeColumn(tableName, columnName, newDefinition string) *MigrationBuilder {
	b.migration.AddUp(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s;", tableName, columnName, newDefinition))
	b.migration.AddDown(fmt.Sprintf("-- Cannot reverse column change for %s.%s", tableName, columnName))
	return b
}

func (b *MigrationBuilder) CreateIndex(indexName, tableName string, columns ...string) *MigrationBuilder {
	query := fmt.Sprintf("CREATE INDEX %s ON %s (%s);",
		indexName, tableName, strings.Join(columns, ", "))
	b.migration.AddUp(query)
	b.migration.AddDown(fmt.Sprintf("DROP INDEX IF EXISTS %s;", indexName))
	return b
}

func (b *MigrationBuilder) CreateUniqueIndex(indexName, tableName string, columns ...string) *MigrationBuilder {
	query := fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s);",
		indexName, tableName, strings.Join(columns, ", "))
	b.migration.AddUp(query)
	b.migration.AddDown(fmt.Sprintf("DROP INDEX IF EXISTS %s;", indexName))
	return b
}

func (b *MigrationBuilder) DropIndex(indexName string) *MigrationBuilder {
	b.migration.AddUp(fmt.Sprintf("DROP INDEX IF EXISTS %s;", indexName))
	b.migration.AddDown(fmt.Sprintf("-- Cannot restore dropped index %s without definition", indexName))
	return b
}

func (b *MigrationBuilder) AddForeignKey(tableName, columnName, refTable, refColumn string) *MigrationBuilder {
	constraintName := fmt.Sprintf("fk_%s_%s", tableName, columnName)
	query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s);",
		tableName, constraintName, columnName, refTable, refColumn)
	b.migration.AddUp(query)
	b.migration.AddDown(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", tableName, constraintName))
	return b
}

func (b *MigrationBuilder) AddForeignKeyWithName(tableName, constraintName, columnName, refTable, refColumn string) *MigrationBuilder {
	query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s);",
		tableName, constraintName, columnName, refTable, refColumn)
	b.migration.AddUp(query)
	b.migration.AddDown(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", tableName, constraintName))
	return b
}

func (b *MigrationBuilder) DropForeignKey(tableName, constraintName string) *MigrationBuilder {
	b.migration.AddUp(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", tableName, constraintName))
	b.migration.AddDown(fmt.Sprintf("-- Cannot restore dropped foreign key %s", constraintName))
	return b
}

func (b *MigrationBuilder) AddPrimaryKey(tableName, constraintName string, columns ...string) *MigrationBuilder {
	query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s);",
		tableName, constraintName, strings.Join(columns, ", "))
	b.migration.AddUp(query)
	b.migration.AddDown(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", tableName, constraintName))
	return b
}

func (b *MigrationBuilder) AddCheck(tableName, constraintName, condition string) *MigrationBuilder {
	query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);",
		tableName, constraintName, condition)
	b.migration.AddUp(query)
	b.migration.AddDown(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", tableName, constraintName))
	return b
}

func (b *MigrationBuilder) RawUp(query string) *MigrationBuilder {
	b.migration.AddUp(query)
	return b
}

func (b *MigrationBuilder) RawDown(query string) *MigrationBuilder {
	b.migration.AddDown(query)
	return b
}

func (b *MigrationBuilder) Raw(upQuery, downQuery string) *MigrationBuilder {
	b.migration.AddUp(upQuery)
	b.migration.AddDown(downQuery)
	return b
}

func (b *MigrationBuilder) Build() Migration {
	return b.migration
}
