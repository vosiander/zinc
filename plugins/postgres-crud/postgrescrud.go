package postgres_crud

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/siklol/zinc/plugins"
	"github.com/siklol/zinc/plugins/postgres"
	"github.com/sirupsen/logrus"
)

type (
	Plugin struct {
		logger *logrus.Entry
		conf   Config
		db     *sqlx.DB
	}

	Transaction struct {
		table string
		db    *sqlx.DB
	}

	Config struct {
		Enable bool `env:"POSTGRES_ENABLE" default:"false" yaml:"enable"`
	}
)

const Name = "postgres-crud"

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
	p.logger = logrus.WithField("component", "postgres-crud")
	l := p.logger
	p.conf = conf.(Config)

	for _, d := range dependencies {
		switch dp := d.(type) {
		case *logrus.Entry:
			p.logger = dp.WithField("component", "postgres-crud")
		case *postgres.Plugin:
			if !dp.IsEnabled() {
				return p
			}
			p.db = dp.DB()
		}
	}

	if !p.conf.Enable {
		l.Debug("postgres is not enabled. nothing to init...")
		return p
	}

	return p
}

func (p *Plugin) CreateTable(name string) (*Transaction, error) {
	if !p.conf.Enable {
		return nil, errors.New("postgres crud not enabled")
	}
	name = strings.ReplaceAll(name, "-", "_")

	schema := `create table if not exists %s
(
    id text constraint id_%s_pk primary key,
    data jsonb not null,
	created_at timestamp not null,
	updated_at timestamp no null
);;` //

	p.logger.WithField("name", name).Debug("trying to create table")

	_, err := p.db.Exec(fmt.Sprintf(schema, name, name))

	if err != nil {
		return nil, err
	}

	return p.NewTransaction(name), nil
}

func (p *Plugin) NewTransaction(name string) *Transaction {
	if !p.IsEnabled() {
		p.logger.Fatal("cannot create transaction. postgres-crud is not enabled")
	}

	return &Transaction{
		table: name,
		db:    p.db,
	}
}

func (tx *Transaction) Upsert(id string, data interface{}) error {
	jData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	schema := `INSERT INTO %s (id, data, created_at, updated_at) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET id = $1, data = $2, updated_at = $4;`
	_, err = tx.db.Exec(fmt.Sprintf(schema, tx.table), id, jData, time.Now(), time.Now())

	return err
}

func (tx *Transaction) Find(id string, target interface{}) error {
	row := tx.db.QueryRowx(fmt.Sprintf("SELECT data from %s where id = $1", tx.table), id)
	err := row.Err()
	if err != nil {
		return err
	}

	var data []byte
	err = row.Scan(&data)
	if err == sql.ErrNoRows {
		return nil
	} else if err != nil {
		return err
	}

	if err := json.Unmarshal(data, target); err != nil {
		return err
	}

	return nil
}

func (tx *Transaction) Delete(id string) error {
	schema := `DELETE FROM  %s WHERE id = $1;`
	_, err := tx.db.Exec(fmt.Sprintf(schema, tx.table), id)

	return err
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Close() error {
	if !p.conf.Enable {
		return nil
	}
	return nil
}
