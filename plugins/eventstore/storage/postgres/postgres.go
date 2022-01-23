package postgres

import (
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/jmoiron/sqlx"
	"github.com/siklol/zinc/plugins/eventstore/eventsourcing"
	"github.com/sirupsen/logrus"
)

type (
	Storage struct {
		l   *logrus.Entry
		cfg Config
		db  *sqlx.DB
		m   Migrator
	}

	Config struct {
		Enable          bool   `env:"POSTGRES_STORAGE_ENABLE" yaml:"enable"`
		Uri             string `env:"POSTGRES_STORAGE_URI" default:"" yaml:"uri"`
		DatabaseName    string `env:"POSTGRES_STORAGE_DATABASE_NAME" default:"" yaml:"databaseName"`
		MigrationsPath  string `env:"POSTGRES_STORAGE_MIGRATIONS_PATH" default:"/migrations" yaml:"migrationsPath"`
		MigrationsTable string `env:"POSTGRES_STORAGE_MIGRATIONS_TABLE" default:"" yaml:"migrationsTable"`
	}

	Migrator interface {
		PerformMigrations(db *sql.DB, path string, databaseName string) error
	}
)

var (
	ErrNoAggregateID = errors.New("aggregate id missing. cannot load aggregate")
)

func NewPostgresEventStorage(l *logrus.Entry, cfg Config, m Migrator) *Storage {
	l.WithField("config", cfg).Trace("storage postgres config")

	db, err := sqlx.Open("postgres", cfg.Uri)
	if err != nil {
		l.WithError(err).Fatal("Database could not be opened")
	}

	l.Trace(cfg.MigrationsPath)

	if err := m.PerformMigrations(db.DB, "file://"+cfg.MigrationsPath, cfg.DatabaseName); err != nil {
		l.WithError(err).Fatal("error running migrations")
	}

	return &Storage{
		l:   l,
		cfg: cfg,
		db:  db,
		m:   m,
	}
}

func (p *Storage) Upsert(a eventsourcing.Aggregate) error {
	events := a.Events()

	tx, err := p.db.Beginx()
	if err != nil {
		return err
	}

	query := `
		INSERT INTO events (id, aggregate_id, name, version, created_at, data) 
    	VALUES (:id, :aggregateId, :name, :version, :createdAt, :data)
    	ON CONFLICT (id) DO UPDATE 
		  SET id = :id, 
		      aggregate_id = :aggregateId,
			  name = :name,
		      version = :version,
		      created_at = :createdAt,
		      data = :data;
    	`
	for _, e := range events {
		m := e.Meta()
		data, err := json.Marshal(e)

		if err != nil {
			tx.Rollback()
			return err
		}

		_, err = tx.NamedExec(query, map[string]interface{}{
			"id":          m.ID,
			"aggregateId": a.AggregateID(),
			"name":        m.Name,
			"version":     m.Version,
			"createdAt":   m.CreatedAt,
			"data":        data,
		})
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (p *Storage) Load(a eventsourcing.Aggregate) error {
	if a.AggregateID() == "" {
		return ErrNoAggregateID
	}

	rows, err := p.db.NamedQuery("SELECT data, name, version FROM events WHERE aggregate_id = :id ORDER BY created_at ASC", map[string]interface{}{
		"id": a.AggregateID(),
	})
	if err != nil {
		p.l.WithError(err).Error("could not execute aggregate loading query")
		return err
	}

	for rows.Next() {
		var data []byte
		var name string
		var version string
		if err = rows.Scan(&data, &name, &version); err != nil {
			p.l.WithError(err).Error("could not execute aggregate loading query")
			return err
		}
		if err := a.OnRaw(name, version, data); err != nil {
			p.l.WithError(err).Error("could not rebuild aggregate")
		}
	}

	return nil
}

func (p *Storage) Name() string {
	return "postgres"
}

func (p *Storage) Close() error {
	return nil
}
