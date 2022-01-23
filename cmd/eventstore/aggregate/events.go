package aggregate

import "github.com/siklol/zinc/plugins/eventstore/eventsourcing"

type (
	BlaBlubEvent struct {
		M           eventsourcing.Meta `json:"meta"`
		FirstValue  string             `json:"first_value"`
		SecondValue string             `json:"second_value"`
	}
)

func (e *BlaBlubEvent) Meta() eventsourcing.Meta {
	return e.M
}
