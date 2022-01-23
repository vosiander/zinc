package boltdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/siklol/zinc/plugins/eventstore/eventsourcing"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

type (
	Storage struct {
		l   *logrus.Entry
		db  *bbolt.DB
		cfg Config
	}

	Config struct {
		Enable bool   `env:"BOLTDB_STORAGE_ENABLE" yaml:"enable"`
		File   string `env:"BOLTDB_STORAGE_FILE" default:"eventstore.db" yaml:"file"`
	}

	iEvent struct {
		Meta eventsourcing.Meta `json:"meta"`
	}
)

func NewBoltDBEventStorage(l *logrus.Entry, cfg Config) *Storage {
	l.WithField("config", cfg).Trace("storage boltdb config")

	db, err := bbolt.Open(cfg.File, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		l.WithError(err).Fatal("error starting bolt db")
	}

	return &Storage{
		db: db,
		l:  l,
	}
}

func (p *Storage) Name() string {
	return "boltdb"
}

func (p *Storage) Upsert(a eventsourcing.Aggregate) error {
	l := p.l.WithField("module", "upsert")
	return p.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName(a))
		if err != nil {
			l.WithError(err).Error("could not create bucket")
			return fmt.Errorf("create bucket: %s", err)
		}

		events := a.Events()
		l.Tracef("%#v", a)

		if len(events) == 0 {
			return errors.New("no events to persist")
		}

		for _, e := range events {
			jEvent, err := json.Marshal(e)
			if err != nil {
				l.WithError(err).WithField("meta", e.Meta()).Error("could not marshal event")
				return err
			}
			id := eventID(a, e)
			if err := b.Put(id, jEvent); err != nil {
				l.WithError(err).WithField("meta", e.Meta()).Error("could not persist event")
				return err
			}
			l.WithField("meta", e.Meta()).WithField("id", string(id)).Trace("event written")
		}

		l.Debug("done persisting aggregate")

		return nil
	})
}

func (p *Storage) Load(a eventsourcing.Aggregate) error {
	l := p.l.WithField("module", "load")

	return p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName(a))

		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var e iEvent
			if err := json.Unmarshal(v, &e); err != nil {
				l.WithError(err).WithField("id", string(k)).Error("could not unmarshal event")
				return err
			}
			if err := a.OnRaw(e.Meta.Name, e.Meta.Version, v); err != nil {
				l.WithError(err).WithField("id", e.Meta.ID).Error("could not load event")
				return err
			}
		}

		return nil
	})
}

func (p *Storage) Close() error {
	return p.db.Close()
}

func bucketName(a eventsourcing.Aggregate) []byte {
	return []byte(fmt.Sprintf("aggregate-%s", a.AggregateID()))
}

func eventID(a eventsourcing.Aggregate, e eventsourcing.Event) []byte {
	return []byte(fmt.Sprintf("%s-%s", a.AggregateID(), e.Meta().ID))
}
