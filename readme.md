# `migrator` — Типобезопасный инструмент для управления миграциями БД на Go

[![Go CI](https://github.com/shuldan/migrator/workflows/Go%20CI/badge.svg)](https://github.com/shuldan/migrator/actions)
[![codecov](https://codecov.io/gh/shuldan/migrator/branch/main/graph/badge.svg)](https://codecov.io/gh/shuldan/migrator)
[![Go Report Card](https://goreportcard.com/badge/github.com/shuldan/migrator)](https://goreportcard.com/report/github.com/shuldan/migrator)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Пакет `migrator` предоставляет типобезопасный и расширяемый способ управления миграциями базы данных в Go-приложениях, построенных по принципам DDD. Поддерживает батчевое выполнение, несколько стратегий транзакций, advisory-блокировки для распределённых сред, dry-run, хуки жизненного цикла и полную диагностику состояния миграций.

---

## Содержание

- [Основные возможности](#-основные-возможности)
- [Установка](#-установка)
- [Быстрый старт](#-быстрый-старт)
- [Архитектура](#-архитектура)
  - [Migration](#migration)
  - [MigrationBuilder](#migrationbuilder)
  - [Migrator](#migrator)
- [Конфигурация](#-конфигурация)
  - [Функциональные опции](#функциональные-опции)
  - [Диалекты](#диалекты)
  - [Стратегии транзакций](#стратегии-транзакций)
  - [Advisory Lock](#advisory-lock)
  - [Логирование](#логирование)
  - [Хуки жизненного цикла](#хуки-жизненного-цикла)
- [Применение миграций](#-применение-миграций)
- [Откат миграций](#-откат-миграций)
  - [Откат по количеству](#откат-по-количеству)
  - [Откат последнего батча](#откат-последнего-батча)
  - [Откат всех миграций](#откат-всех-миграций)
  - [Необратимые миграции](#необратимые-миграции)
- [Dry Run — просмотр плана](#-dry-run--просмотр-плана)
- [Статус миграций](#-статус-миграций)
- [Операции MigrationBuilder](#-операции-migrationbuilder)
  - [Таблицы](#таблицы)
  - [Колонки](#колонки)
  - [Индексы](#индексы)
  - [Ограничения](#ограничения)
  - [Расширения PostgreSQL](#расширения-postgresql)
  - [Произвольный SQL](#произвольный-sql)
  - [Флаги миграции](#флаги-миграции)
- [Валидация](#-валидация)
- [Обработка ошибок](#-обработка-ошибок)
- [DDD и мультимодульные проекты](#-ddd-и-мультимодульные-проекты)
- [Полный пример](#-полный-пример)
- [Работа с проектом](#-работа-с-проектом)
- [Лицензия](#-лицензия)

---

## 🚀 Основные возможности

| Возможность | Описание |
|---|---|
| **Декларативный builder** | Типобезопасное построение миграций через цепочку вызовов |
| **Батчевое выполнение** | Миграции группируются в батчи для атомарного применения и отката |
| **3 стратегии транзакций** | `TxPerBatch`, `TxPerMigration`, `TxDisabled` — с автовыбором по диалекту |
| **Advisory-блокировки** | Защита от параллельного запуска в распределённых средах (PostgreSQL, MySQL) |
| **Необратимые миграции** | Явная пометка `MarkIrreversible()` с защитой от случайного отката |
| **Dry Run** | `Plan()` / `PlanDown()` — просмотр SQL без выполнения |
| **Полная диагностика** | `Status()` возвращает Applied, Pending и Ghost-миграции |
| **Хуки жизненного цикла** | `BeforeMigration` / `AfterMigration` для логирования, метрик, нотификаций |
| **Структурные ошибки** | `MigrationError` с ID миграции, фазой, текстом запроса |
| **Мультидиалектность** | PostgreSQL, MySQL, SQLite — с автоопределением или явным указанием |
| **Тестовые утилиты** | Подпакет `migratortest` с assertion-хелперами |

---

## 📦 Установка

```sh
go get github.com/shuldan/migrator
```

Требуется Go **1.24+**.

---

## 🏁 Быстрый старт

```go
package main

import (
    "context"
    "database/sql"
    "log"

    _ "github.com/mattn/go-sqlite3"
    "github.com/shuldan/migrator"
)

func main() {
    db, err := sql.Open("sqlite3", "./app.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    m := migrator.New(db)

    m.MustRegister(
        migrator.CreateMigration("20240101120000_create_users", "Create users table").
            CreateTable("users",
                "id INTEGER PRIMARY KEY",
                "name TEXT NOT NULL",
                "email VARCHAR(255) UNIQUE",
            ).
            MustBuild(),
    )

    ctx := context.Background()

    if err := m.Up(ctx); err != nil {
        log.Fatal(err)
    }
}
```

---

## 🧱 Архитектура

### Migration

Интерфейс единицы миграции:

```go
type Migration interface {
    ID() string           // Уникальный идентификатор (рекомендуется формат YYYYMMDDHHMMSS_name)
    Description() string  // Человекочитаемое описание
    Up() []string         // SQL-запросы для применения
    Down() []string       // SQL-запросы для отката
    Irreversible() bool   // Помечена ли миграция как необратимая
    DisableTx() bool      // Выполнять ли вне транзакции
}
```

### MigrationBuilder

Построитель миграций с цепочкой вызовов и отложенной валидацией:

```go
migration, err := migrator.CreateMigration("20240101120000_create_users", "Create users table").
    CreateTable("users",
        "id BIGSERIAL PRIMARY KEY",
        "email VARCHAR(255) NOT NULL",
    ).
    CreateUniqueIndex("idx_users_email", "users", "email").
    Build()  // валидация и возврат ошибки
```

Или с паникой при ошибке:

```go
migration := migrator.CreateMigration("20240101120000_create_users", "Create users table").
    CreateTable("users", "id BIGSERIAL PRIMARY KEY", "email VARCHAR(255) NOT NULL").
    CreateUniqueIndex("idx_users_email", "users", "email").
    MustBuild()  // паника при невалидной миграции
```

### Migrator

Центральный объект управления миграциями:

```go
m := migrator.New(db, ...options)

m.MustRegister(migration1, migration2)  // регистрация
err := m.Up(ctx)                        // применить pending-миграции
err := m.Down(ctx, 2)                   // откатить последние 2
err := m.DownBatch(ctx)                 // откатить последний батч
err := m.DownAll(ctx)                   // откатить всё
statuses, err := m.Status(ctx)          // полный статус
planned, err := m.Plan(ctx)             // dry run
```

---

## ⚙ Конфигурация

### Функциональные опции

Все параметры настраиваются через `Option` при создании `Migrator`:

```go
m := migrator.New(db,
    migrator.WithDialect(migrator.DialectPostgreSQL),
    migrator.WithTableName("my_migrations"),
    migrator.WithSchema("myapp"),
    migrator.WithLogger(logger),
    migrator.WithAdvisoryLock(),
    migrator.WithTxStrategy(migrator.TxPerBatch),
    migrator.WithBeforeMigration(beforeHook),
    migrator.WithAfterMigration(afterHook),
)
```

| Опция | По умолчанию | Описание |
|---|---|---|
| `WithDialect(d)` | Автоопределение | Явное указание диалекта СУБД |
| `WithTableName(name)` | `"schema_migrations"` | Имя мета-таблицы миграций |
| `WithSchema(schema)` | `""` | Схема/namespace (генерирует `schema.table`) |
| `WithLogger(l)` | `noopLogger` | Логгер, реализующий интерфейс `Logger` |
| `WithAdvisoryLock()` | Отключено | Advisory lock, соответствующий диалекту |
| `WithLockStrategy(s)` | `NoopLock` | Пользовательская стратегия блокировки |
| `WithTxStrategy(s)` | Автовыбор | Стратегия транзакций |
| `WithBeforeMigration(fn)` | `nil` | Хук перед каждой миграцией |
| `WithAfterMigration(fn)` | `nil` | Хук после каждой миграции |

### Диалекты

```go
migrator.DialectPostgreSQL  // PostgreSQL (pgx, pq, lib/pq)
migrator.DialectMySQL       // MySQL
migrator.DialectSQLite      // SQLite
```

Диалекты определяют:
- Формат плейсхолдеров (`?` → `$1, $2, ...` для PostgreSQL)
- Формат квотирования идентификаторов (`"name"` vs `` `name` ``)
- Стратегию транзакций по умолчанию
- Тип advisory lock

**Автоопределение** работает через анализ имени драйвера `*sql.DB`. Для надёжности рекомендуется указывать явно через `WithDialect()`.

```go
// Метод для квотирования идентификаторов в Raw-запросах:
quoted := migrator.DialectPostgreSQL.QuoteIdentifier("table_name")
// PostgreSQL: "table_name"
// MySQL:      `table_name`
```

### Стратегии транзакций

```go
migrator.TxDefault        // Автовыбор по диалекту
migrator.TxPerBatch       // Весь батч в одной транзакции
migrator.TxPerMigration   // Каждая миграция — отдельная транзакция
migrator.TxDisabled       // Без транзакций
```

| Диалект | Стратегия по умолчанию | Причина |
|---|---|---|
| PostgreSQL | `TxPerBatch` | Транзакционный DDL |
| MySQL | `TxPerMigration` | DDL вызывает implicit commit |
| SQLite | `TxPerMigration` | Ограничения на параллельные транзакции |

Миграции с `DisableTransaction()` (например, `CREATE INDEX CONCURRENTLY`) автоматически выполняются вне транзакции, даже при `TxPerBatch`:

```go
migrator.CreateMigration("20240601_create_index_concurrently", "Add index concurrently").
    RawUp("CREATE INDEX CONCURRENTLY idx_orders_status ON orders(status);").
    RawDown("DROP INDEX IF EXISTS idx_orders_status;").
    DisableTransaction().
    MustBuild()
```

### Advisory Lock

Защита от одновременного запуска миграций несколькими инстансами:

```go
// Автоматический выбор по диалекту:
m := migrator.New(db, migrator.WithAdvisoryLock())

// Или пользовательская стратегия:
m := migrator.New(db, migrator.WithLockStrategy(customLock))
```

| Диалект | Механизм | Детали |
|---|---|---|
| PostgreSQL | `pg_advisory_lock` / `pg_advisory_unlock` | Ключ — FNV-хеш имени таблицы |
| MySQL | `GET_LOCK` / `RELEASE_LOCK` | Таймаут 10 сек |
| SQLite | Нет (no-op) | Встроенная защита через file lock |

Интерфейс для пользовательских реализаций:

```go
type LockStrategy interface {
    Lock(ctx context.Context) error
    Unlock(ctx context.Context) error
}
```

### Логирование

Минимальный интерфейс, совместимый со стилем `log/slog`:

```go
type Logger interface {
    Info(msg string, args ...any)
    Error(msg string, args ...any)
}
```

Адаптер для `*slog.Logger`:

```go
type slogAdapter struct {
    logger *slog.Logger
}

func (a *slogAdapter) Info(msg string, args ...any)  { a.logger.Info(msg, args...) }
func (a *slogAdapter) Error(msg string, args ...any) { a.logger.Error(msg, args...) }

m := migrator.New(db, migrator.WithLogger(&slogAdapter{logger: slog.Default()}))
```

Мигратор логирует:
- Количество pending-миграций и номер батча перед применением
- ID и длительность каждой применённой/откаченной миграции
- Ошибки закрытия курсора БД

### Хуки жизненного цикла

```go
m := migrator.New(db,
    migrator.WithBeforeMigration(func(ctx context.Context, mig migrator.Migration) error {
        log.Printf("→ starting: %s", mig.ID())
        // Возврат ошибки прерывает процесс
        return nil
    }),
    migrator.WithAfterMigration(func(ctx context.Context, mig migrator.Migration, d time.Duration, err error) {
        if err != nil {
            metrics.MigrationFailed.Inc()
        } else {
            metrics.MigrationDuration.Observe(d.Seconds())
            log.Printf("✓ completed: %s (%s)", mig.ID(), d)
        }
    }),
)
```

---

## ▶ Применение миграций

```go
ctx := context.Background()

// Применить все pending-миграции одним батчем:
if err := m.Up(ctx); err != nil {
    log.Fatal(err)
}
```

Метод `Up`:
1. Захватывает advisory lock (если настроен)
2. Создаёт мета-таблицу (при первом вызове)
3. Определяет pending-миграции (зарегистрированные, но не в БД)
4. Назначает номер батча (макс. существующий + 1)
5. Выполняет миграции согласно стратегии транзакций
6. Записывает каждую миграцию в мета-таблицу
7. Освобождает advisory lock

---

## ◀ Откат миграций

### Откат по количеству

```go
// Откатить последние 3 миграции:
err := m.Down(ctx, 3)
```

### Откат последнего батча

```go
// Откатить все миграции, применённые в последнем батче:
err := m.DownBatch(ctx)
```

### Откат всех миграций

```go
// Откатить всё:
err := m.DownAll(ctx)
```

### Необратимые миграции

Миграции, помеченные как `MarkIrreversible()`, защищены от случайного отката:

```go
migrator.CreateMigration("20240501_drop_legacy", "Drop legacy tables").
    DropTable("legacy_data").
    MarkIrreversible().
    MustBuild()
```

Попытка отката вернёт ошибку `ErrIrreversibleMigration`. Для принудительного отката используйте `WithForce()` — запись удалится из мета-таблицы, down-запросы не выполнятся:

```go
err := m.DownAll(ctx, migrator.WithForce())
```

---

## 🔍 Dry Run — просмотр плана

Просмотр запросов без выполнения:

```go
// Какие миграции будут применены:
planned, err := m.Plan(ctx)
for _, p := range planned {
    fmt.Printf("Migration: %s (%s)\n", p.ID, p.Description)
    for _, q := range p.Queries {
        fmt.Printf("  %s\n", q)
    }
}

// Какие миграции будут откачены:
rollback, err := m.PlanDown(ctx, 2)
for _, p := range rollback {
    fmt.Printf("Rollback: %s\n", p.ID)
}
```

---

## 📊 Статус миграций

```go
statuses, err := m.Status(ctx)
for _, s := range statuses {
    fmt.Printf("%-40s  %-8s  batch=%d\n", s.ID, s.State, s.Batch)
}
```

Каждая миграция имеет одно из трёх состояний:

| Состояние | Описание |
|---|---|
| `MigrationStatePending` | Зарегистрирована в коде, но не применена |
| `MigrationStateApplied` | Применена и зарегистрирована в коде |
| `MigrationStateGhost` | Есть в БД, но не зарегистрирована в коде (удалена из кода после применения) |

Ghost-миграции — сигнал о проблеме: кто-то удалил миграцию из кода после того, как она была применена.

---

## 🔧 Операции MigrationBuilder

Builder генерирует ANSI SQL с ориентацией на PostgreSQL. Для диалект-специфичного SQL используйте `Raw()` / `RawUp()` / `RawDown()`.

### Таблицы

```go
// Создание таблицы (автоматически генерирует DROP TABLE для Down):
.CreateTable("users",
    "id BIGSERIAL PRIMARY KEY",
    "email VARCHAR(255) NOT NULL",
    "created_at TIMESTAMP NOT NULL DEFAULT NOW()",
)

// Удаление таблицы (необратимая операция — Down не генерируется):
.DropTable("legacy_data")

// Переименование таблицы (Down — обратное переименование):
.RenameTable("users", "accounts")
```

### Колонки

```go
// Добавление колонки (Down — DROP COLUMN):
.AddColumn("users", "phone VARCHAR(20) NOT NULL DEFAULT ''")

// Удаление колонки (необратимая операция):
.DropColumn("users", "phone")

// Переименование колонки (Down — обратное переименование):
.RenameColumn("users", "name", "full_name")

// Изменение типа/свойств колонки (необратимая операция):
.ChangeColumn("users", "email", "TYPE TEXT")
```

### Индексы

```go
// Обычный индекс (Down — DROP INDEX):
.CreateIndex("idx_users_email", "users", "email")

// Составной индекс:
.CreateIndex("idx_orders_user_date", "orders", "user_id", "created_at")

// Уникальный индекс:
.CreateUniqueIndex("idx_users_email_unique", "users", "email")

// Удаление индекса:
.DropIndex("idx_users_email")
```

### Ограничения

```go
// Foreign key (имя генерируется автоматически: fk_orders_user_id):
.AddForeignKey("orders", "user_id", "users", "id")

// Foreign key с явным именем:
.AddForeignKeyWithName("orders", "fk_orders_user", "user_id", "users", "id")

// Primary key:
.AddPrimaryKey("order_items", "pk_order_items", "order_id", "product_id")

// Unique constraint (отличается от уникального индекса семантически):
.AddUniqueConstraint("users", "uq_users_email", "email")

// Check constraint:
.AddCheck("orders", "chk_orders_total", "total >= 0")

// Удаление foreign key:
.DropForeignKey("orders", "fk_orders_user")
```

### Расширения PostgreSQL

```go
// Создание расширения (Down — DROP EXTENSION):
.CreateExtension("uuid-ossp")
.CreateExtension("pgcrypto")
```

### Произвольный SQL

```go
// Up + Down:
.Raw(
    "INSERT INTO settings (key, value) VALUES ('version', '1.0');",
    "DELETE FROM settings WHERE key = 'version';",
)

// Только Up:
.RawUp("CREATE MATERIALIZED VIEW user_stats AS SELECT ...")

// Только Down:
.RawDown("DROP MATERIALIZED VIEW IF EXISTS user_stats;")
```

### Флаги миграции

```go
// Необратимая миграция (откат заблокирован без WithForce):
.MarkIrreversible()

// Отключить транзакцию (для CREATE INDEX CONCURRENTLY и т.п.):
.DisableTransaction()
```

---

## ✅ Валидация

`Build()` выполняет валидацию и возвращает ошибку:

```go
migration, err := migrator.CreateMigration("", "").Build()
// err: "migration id must not be empty\nmigration description must not be empty\nmigration must have at least one up query"
```

Проверки при построении:
- ID не пустой
- Описание не пустое
- Есть хотя бы один Up-запрос
- `AddColumn` — определение колонки содержит имя и тип (минимум 2 слова)

Проверки при регистрации (`Register`):
- Миграция не `nil`
- Нет дубликатов ID

---

## ⚠ Обработка ошибок

### Сигнальные ошибки

Проверяются через `errors.Is`:

```go
if errors.Is(err, migrator.ErrIrreversibleMigration) {
    log.Println("Миграция необратима, используйте WithForce()")
}

if errors.Is(err, migrator.ErrNoMigrationsToRollback) {
    log.Println("Нечего откатывать")
}
```

Полный список:

| Ошибка | Описание |
|---|---|
| `ErrMigrationFailed` | Общая ошибка выполнения миграции |
| `ErrIrreversibleMigration` | Попытка отката необратимой миграции |
| `ErrNoMigrationsToRollback` | Нет применённых миграций для отката |
| `ErrDuplicateMigrationID` | Дубликат ID при регистрации |
| `ErrNilMigration` | `nil` передан в `Register` |
| `ErrEmptyMigrationID` | Пустой ID при `Build()` |
| `ErrEmptyMigrationDescription` | Пустое описание при `Build()` |
| `ErrNoUpQueries` | Нет Up-запросов при `Build()` |
| `ErrInvalidColumnDefinition` | Некорректное определение колонки |
| `ErrInvalidArgument` | Некорректный аргумент метода |
| `ErrFailedToBeginTransaction` | Не удалось начать транзакцию |
| `ErrFailedToExecuteQuery` | Не удалось выполнить SQL-запрос |
| `ErrFailedToGetAppliedMigrations` | Не удалось прочитать мета-таблицу |
| `ErrFailedToCreateSchemaMigrationsTable` | Не удалось создать мета-таблицу |
| `ErrFailedToCreateSchemaMigrationsIndex` | Не удалось создать индекс мета-таблицы |
| `ErrFailedToAcquireLock` | Не удалось захватить advisory lock |
| `ErrFailedToReleaseLock` | Не удалось освободить advisory lock |

### Структурная ошибка MigrationError

Извлекается через `errors.As` для диагностики:

```go
var migErr *migrator.MigrationError
if errors.As(err, &migErr) {
    fmt.Printf("Migration: %s\n", migErr.MigrationID)
    fmt.Printf("Phase:     %s\n", migErr.Phase)  // "up" или "down"
    fmt.Printf("Query:     %s\n", migErr.Query)
    fmt.Printf("Error:     %v\n", migErr.Err)
}
```

---

## 🏗 DDD и мультимодульные проекты

Пакет спроектирован для DDD-проектов, где каждый bounded context может управлять своими миграциями:

```go
// bounded context "users"
usersDB, _ := sql.Open("postgres", usersConnStr)
usersMigrator := migrator.New(usersDB,
    migrator.WithSchema("users"),
    migrator.WithTableName("migrations"),
    migrator.WithAdvisoryLock(),
)
usersMigrator.MustRegister(usersMigrations()...)

// bounded context "orders"
ordersDB, _ := sql.Open("postgres", ordersConnStr)
ordersMigrator := migrator.New(ordersDB,
    migrator.WithSchema("orders"),
    migrator.WithTableName("migrations"),
    migrator.WithAdvisoryLock(),
)
ordersMigrator.MustRegister(ordersMigrations()...)

// Каждый контекст мигрирует независимо
_ = usersMigrator.Up(ctx)
_ = ordersMigrator.Up(ctx)
```

Использование `WithSchema` + `WithTableName` гарантирует изоляцию мета-таблиц: `users.migrations` и `orders.migrations`.

---

## 📋 Полный пример

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "log/slog"
    "os"
    "time"

    _ "github.com/lib/pq"
    "github.com/shuldan/migrator"
)

type slogAdapter struct{ l *slog.Logger }

func (a *slogAdapter) Info(msg string, args ...any)  { a.l.Info(msg, args...) }
func (a *slogAdapter) Error(msg string, args ...any) { a.l.Error(msg, args...) }

func main() {
    db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ctx := context.Background()
    logger := &slogAdapter{l: slog.Default()}

    m := migrator.New(db,
        migrator.WithDialect(migrator.DialectPostgreSQL),
        migrator.WithSchema("myapp"),
        migrator.WithLogger(logger),
        migrator.WithAdvisoryLock(),
        migrator.WithAfterMigration(func(_ context.Context, mig migrator.Migration, d time.Duration, err error) {
            if err == nil {
                logger.Info("✓ migration applied", "id", mig.ID(), "duration", d.String())
            }
        }),
    )

    m.MustRegister(
        migrator.CreateMigration("20240101120000_create_users", "Create users table").
            CreateTable("users",
                "id BIGSERIAL PRIMARY KEY",
                "email VARCHAR(255) NOT NULL",
                "name VARCHAR(255) NOT NULL",
                "created_at TIMESTAMP NOT NULL DEFAULT NOW()",
            ).
            CreateUniqueIndex("idx_users_email", "users", "email").
            MustBuild(),

        migrator.CreateMigration("20240102120000_create_orders", "Create orders table").
            CreateTable("orders",
                "id BIGSERIAL PRIMARY KEY",
                "user_id BIGINT NOT NULL",
                "total DECIMAL(10,2) NOT NULL",
                "created_at TIMESTAMP NOT NULL DEFAULT NOW()",
            ).
            AddForeignKey("orders", "user_id", "users", "id").
            AddCheck("orders", "chk_orders_total", "total >= 0").
            MustBuild(),

        migrator.CreateMigration("20240201120000_add_status", "Add status to orders").
            AddColumn("orders", "status VARCHAR(50) NOT NULL DEFAULT 'pending'").
            CreateIndex("idx_orders_status", "orders", "status").
            MustBuild(),

        migrator.CreateMigration("20240301120000_drop_legacy", "Remove legacy tables").
            DropTable("legacy_data").
            MarkIrreversible().
            MustBuild(),
    )

    // Просмотр плана
    planned, err := m.Plan(ctx)
    if err != nil {
        log.Fatal(err)
    }
    for _, p := range planned {
        logger.Info("pending", "id", p.ID, "queries", len(p.Queries))
    }

    // Применить
    if err := m.Up(ctx); err != nil {
        log.Fatal(err)
    }

    // Проверить статус
    statuses, err := m.Status(ctx)
    if err != nil {
        log.Fatal(err)
    }
    for _, s := range statuses {
        logger.Info("status", "id", s.ID, "state", s.State.String(), "batch", s.Batch)
    }
}
```

---

## 🛠 Работа с проектом

### Установка инструментов

```sh
make install-tools
```

Установит `golangci-lint` (v2.4.0), `goimports`, `gosec`.

### Полная проверка

```sh
make all       # fmt + lint + gosec + test
make ci        # валидация, vet, lint, тесты с покрытием (≥70%)
```

### Форматирование

```sh
make fmt        # форматирование + сортировка импортов
make fmt-check  # проверка без изменений
```

### Тесты

```sh
make test
make test-coverage
```

---

## 📄 Лицензия

Проект распространяется под лицензией [MIT](LICENSE).

---

## 🤝 Вклад в проект

PR и issue приветствуются. Перед отправкой:

1. `make fmt` — форматирование кода
2. `make all` — полная проверка
3. Покройте новый код тестами

---

> **Автор**: MSeytumerov
> **Репозиторий**: `github.com/shuldan/migrator`
> **Go**: `1.24+`
