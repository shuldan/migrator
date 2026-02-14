package migrator

import "testing"

func TestMigrationState_String_AllCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		state    MigrationState
		expected string
	}{
		{"Pending", MigrationStatePending, "pending"},
		{"Applied", MigrationStateApplied, "applied"},
		{"Ghost", MigrationStateGhost, "ghost"},
		{"Unknown", MigrationState(99), "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.state.String()
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}
