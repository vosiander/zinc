package eventsourcing

import (
	"time"

	"github.com/google/uuid"
)

type (
	Meta struct {
		ID        EventID   `json:"id" yaml:"id"`
		Name      string    `json:"name" yaml:"name"`
		CreatedAt time.Time `json:"created_at" yaml:"created_at"`
		Version   string    `json:"version" yaml:"version"`
	}
)

func NewMeta(name string, version string) Meta {
	return Meta{
		ID:        EventID(uuid.New().String()),
		Name:      name,
		CreatedAt: time.Now(),
		Version:   version,
	}
}

func NewMetaWithID(id EventID, name string, version string) Meta {
	return Meta{
		ID:        id,
		Name:      name,
		CreatedAt: time.Now(),
		Version:   version,
	}
}
