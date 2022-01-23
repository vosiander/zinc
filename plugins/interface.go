package plugins

type (
	Plugin interface {
		Boot(conf interface{}, dependencies ...interface{}) Plugin
		Start() error
		Name() string
		Close() error
		IsEnabled() bool
	}
)
