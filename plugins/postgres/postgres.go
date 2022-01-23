package postgres

import (
	"database/sql"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
)

type (
	Plugin struct {
		logger *logrus.Entry
		conf   Config
		db     *sqlx.DB
		m      Migrator
	}

	Config struct {
		Enable         bool   `env:"POSTGRES_ENABLE" default:"false" yaml:"enable"`
		Uri            string `env:"POSTGRES_URI" default:"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" yaml:"uri"`
		DatabaseName   string `env:"POSTGRES_DATABASE_NAME" default:"" yaml:"databaseName"`
		MigrationsPath string `env:"POSTGRES_MIGRATIONS_PATH" default:"/migrations" yaml:"migrationsPath"`
	}

	Migrator interface {
		PerformMigrations(db *sql.DB, path string, databaseName string) error
	}
)

const Name = "postgres"

func New() *Plugin {
	return &Plugin{}
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	for _, d := range dependencies {
		switch dp := d.(type) {
		case *logrus.Entry:
			p.logger = dp.WithField("component", "postgres")
		case Migrator:
			p.m = dp
		}
	}
	if p.logger == nil {
		p.logger = logrus.WithField("component", "postgres")
	}
	l := p.logger
	p.conf = conf.(Config)

	if !p.conf.Enable {
		l.Debug("postgres is not enabled. nothing to init...")
		return p
	}
	if p.m == nil {
		p.m = NewGolangMigrator(l, "")
	}

	l.Debug("opening db connection")
	dbConn, err := sqlx.Open("postgres", p.conf.Uri)
	if err != nil {
		l.WithError(err).Fatal("Database could not be opened")
	}
	l.Debug("finished starting postgres")
	p.db = dbConn

	if err := p.m.PerformMigrations(p.db.DB, "file://"+p.conf.MigrationsPath, p.conf.DatabaseName); err != nil {
		l.WithError(err).Fatal("error running migrations")
	}

	return p
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

func (p *Plugin) DB() *sqlx.DB {
	if !p.conf.Enable {
		p.logger.Fatal("postgres db not enabled. failing")
	}
	return p.db
}
