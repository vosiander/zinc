package eventsourcing

import (
	"errors"

	"github.com/sirupsen/logrus"
)

type (
	EventStore struct {
		logger *logrus.Entry
		s      map[string]Storage
	}

	Storage interface {
		Name() string
		Upsert(a Aggregate) error
		Load(a Aggregate) error
		Close() error
	}
)

var (
	ErrStorageNotAvailable = errors.New("storage not available")
	ErrNoAggregateID       = errors.New("no aggregate id")
)

func NewEventStore(l logrus.FieldLogger) *EventStore {
	return &EventStore{
		logger: l.WithField("component", "eventstore"),
		s:      map[string]Storage{},
	}
}

func (es *EventStore) Persist(storage string, a Aggregate) error {
	if _, isOk := es.s[storage]; !isOk {
		es.logger.WithField("storage", storage).Error("storage not available")
		return ErrStorageNotAvailable
	}
	return es.s[storage].Upsert(a)
}

func (es *EventStore) Load(storage string, a Aggregate) error {
	if _, isOk := es.s[storage]; !isOk {
		es.logger.WithField("storage", storage).Error("storage not available")
		return ErrStorageNotAvailable
	}

	if a.AggregateID() == "" {
		es.logger.WithField("storage", storage).Error("aggregate id not available")
		return ErrNoAggregateID
	}

	return es.s[storage].Load(a)
}

func (es *EventStore) AddStorage(s Storage) {
	es.s[s.Name()] = s
}
