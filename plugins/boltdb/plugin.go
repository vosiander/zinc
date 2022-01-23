package boltdb

import (
	"time"

	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

type (
	Plugin struct {
		logger *logrus.Entry
		conf   Config
		db     *bolt.DB
	}

	Config struct {
		Enable bool   `env:"BOLTDB_ENABLE" default:"false" yaml:"enable"`
		File   string `env:"BOLTDB_FILE" default:"db.db" yaml:"file"`
	}
)

const Name = "boltdb"

func New() *Plugin {
	return &Plugin{}
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	for _, d := range dependencies {
		if dp, isOk := d.(*logrus.Entry); isOk {
			p.logger = dp.WithField("component", "boltdb")
		}
	}
	if p.logger == nil {
		p.logger = logrus.WithField("component", "boltdb")
	}

	l := p.logger
	p.conf = conf.(Config)

	if !p.conf.Enable {
		l.Debug("boltdb is not enabled. nothing to init...")
		return p
	}

	p.db = p.Open(p.conf.File)

	return p
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) Open(file string) *bolt.DB {
	if !p.conf.Enable {
		p.logger.Fatal("cannot open boltdb. plugin is not enabled")
	}

	l := p.logger
	boltDB, err := bolt.Open(file, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		l.WithError(err).Fatal("error starting bolt db")
	}
	return boltDB
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Close() error {
	if !p.conf.Enable {
		return nil
	}
	return p.db.Close()
}

func (p *Plugin) DB() *bolt.DB {
	if !p.conf.Enable {
		p.logger.Fatal("cannot use boltdb. plugin is not enabled")
	}
	return p.db
}
