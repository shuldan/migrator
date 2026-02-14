package migrator

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type noopLogger struct{}

func (n *noopLogger) Info(string, ...any)  {}
func (n *noopLogger) Error(string, ...any) {}
