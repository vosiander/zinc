package aggregate

import (
	"encoding/json"
	"errors"

	"github.com/siklol/zinc/plugins/eventstore/eventsourcing"
)

type (
	ExampleAggregate struct {
		id     eventsourcing.AggregateID
		events []eventsourcing.Event

		First  string
		Second string
	}
)

func NewExampleAggregate(id eventsourcing.AggregateID) *ExampleAggregate {
	return &ExampleAggregate{
		id: id,
	}
}

func (a *ExampleAggregate) AggregateID() eventsourcing.AggregateID {
	return a.id
}

func (a *ExampleAggregate) On(e eventsourcing.Event) error {
	switch event := e.(type) {
	case *BlaBlubEvent:
		a.First = event.FirstValue
		a.Second = event.SecondValue
	}
	a.events = append(a.events, e)
	return nil
}

func (a *ExampleAggregate) OnRaw(name string, version string, e []byte) error {
	switch true {
	case name == "blablub" && version == "1.0":
		var event *BlaBlubEvent
		json.Unmarshal(e, &event)
		return a.On(event)
	}
	return errors.New("unknown event type and version")
}

func (a *ExampleAggregate) Events() []eventsourcing.Event {
	return a.events
}
