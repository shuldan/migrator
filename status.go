package migrator

import "time"

type MigrationState int

const (
	MigrationStatePending MigrationState = iota
	MigrationStateApplied
	MigrationStateGhost
)

func (s MigrationState) String() string {
	switch s {
	case MigrationStatePending:
		return "pending"
	case MigrationStateApplied:
		return "applied"
	case MigrationStateGhost:
		return "ghost"
	default:
		return "unknown"
	}
}

type MigrationStatus struct {
	ID          string
	Description string
	State       MigrationState
	AppliedAt   *time.Time
	Batch       int
}

type PlannedMigration struct {
	ID          string
	Description string
	Queries     []string
}
