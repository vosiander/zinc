package eventsourcing

type (
	AggregateID string

	Aggregate interface {
		AggregateID() AggregateID
		On(e Event) error
		OnRaw(name string, version string, e []byte) error
		Events() []Event
	}
)
