package kafka

import (
	"time"

	"github.com/google/uuid"
)

type (
	Meta struct {
		ID        string    `json:"id" yaml:"id"`
		Name      string    `json:"name" yaml:"name"`
		CreatedAt time.Time `json:"created_at" yaml:"created_at"`
		Version   string    `json:"version" yaml:"version"`
	}

	Message struct {
		Topic         string
		Partition     int
		Offset        int64
		HighWaterMark int64
		Key           []byte
		Value         []byte
		Time          time.Time
	}
)

func NewMeta(name string, version string) Meta {
	if version == "" {
		version = "1"
	}
	uid, _ := uuid.NewRandom()
	return Meta{
		ID:        uid.String(),
		Name:      name,
		CreatedAt: time.Now(),
		Version:   version,
	}
}
