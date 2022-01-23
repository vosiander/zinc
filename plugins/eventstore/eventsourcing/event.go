package eventsourcing

type (
	EventID string

	Event interface {
		Meta() Meta
	}
)
