package eventstore

import (
	"github.com/siklol/zinc/plugins"
	"github.com/siklol/zinc/plugins/eventstore/eventsourcing"
	"github.com/siklol/zinc/plugins/eventstore/storage/boltdb"
	"github.com/siklol/zinc/plugins/eventstore/storage/postgres"
	"github.com/sirupsen/logrus"
)

type (
	Plugin struct {
		logger   *logrus.Entry
		conf     Config
		storages map[string]eventsourcing.Storage
		es       *eventsourcing.EventStore
	}

	Config struct {
		Enable   bool `env:"EVENTSTORE_ENABLE" default:"false" yaml:"enable"`
		Storages struct {
			BoltDB   boltdb.Config   `env:"EVENTSTORE_STORAGES_BOLTDB" yaml:"boltdb" json:"boltdb"`
			Postgres postgres.Config `env:"EVENTSTORE_STORAGES_POSTGRES" yaml:"postgres" json:"postgres"`
		} `env:"EVENTSTORE_STORAGES" yaml:"storages" json:"storages"`
	}
)

const Name = "eventstore"

func New() *Plugin {
	return &Plugin{
		storages: map[string]eventsourcing.Storage{},
	}
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	for _, d := range dependencies {
		switch dp := d.(type) {
		case *logrus.Entry:
			p.logger = dp.WithField("component", "eventstore")
		}
	}
	if p.logger == nil {
		p.logger = logrus.WithField("component", "eventstore")
	}

	l := p.logger
	p.conf = conf.(Config)

	if !p.conf.Enable {
		l.Debug("plugin is not enabled. nothing to init...")
		return p
	}

	p.es = eventsourcing.NewEventStore(p.logger.WithField("module", "event-store"))

	return p
}

func (p *Plugin) BootStorage(storages ...eventsourcing.Storage) {
	l := p.logger

	for _, s := range storages {
		l.WithField("boot-storage", s.Name()).Debug("booting eventsourcing storage plugin")
		p.es.AddStorage(s)
	}
}

func (p *Plugin) EventStore() *eventsourcing.EventStore {
	return p.es
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Close() error {
	if !p.conf.Enable {
		return nil
	}

	for name, s := range p.storages {
		if err := s.Close(); err != nil {
			p.logger.WithField("component", "close eventsourcing storage").
				WithField("storage", name).
				WithError(err).
				Error("failing to close storage")
		}
	}

	return nil
}
