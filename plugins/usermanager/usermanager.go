package usermanager

import (
	"database/sql"
	"fmt"

	"github.com/siklol/zinc/plugins/postgres"

	"github.com/jmoiron/sqlx"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const Name = "usermanager"

type (
	Plugin struct {
		logger *log.Entry
		conf   Config
		db     *sqlx.DB
	}

	Config struct {
		Enable bool   `env:"USERMANAGER_ENABLE" default:"false" yaml:"enable"`
		Table  string `env:"USERMANAGER_TABLE" default:"um_users" yaml:"table"`
	}

	User struct {
		ID        string `json:"id"`
		Firstname string `json:"firstname"`
		Lastname  string `json:"lastname"`
		Username  string `json:"username"`
	}
)

func New() *Plugin {
	return &Plugin{}
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	p.logger = logrus.WithField("component", "usermanager")
	p.conf = conf.(Config)

	for _, d := range dependencies {
		switch dp := d.(type) {
		case *postgres.Plugin:
			if !dp.IsEnabled() {
				return p
			}
			p.db = dp.DB()
		case *logrus.Entry:
			p.logger = dp
		}
	}

	p.logger = p.logger.WithField("component", "usermanager")

	if !p.IsEnabled() {
		p.logger.Debug("usermanager is not enabled. nothing to init...")
		return p
	}

	if err := p.CreateTable(p.conf.Table); err != nil {
		p.logger.WithError(err).Fatalf("could not create table %s.", p.conf.Table)
	}

	p.logger.Debug("usermanager initialized")

	return p
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Close() error {
	return nil
}

func (p *Plugin) CreateTable(name string) error {
	sql := `CREATE TABLE IF NOT EXISTS %s
(
    id text not null primary key ,
    username varchar(255) not null,
    firstname varchar(255) not null,
    lastname varchar(255) not null
)`

	_, err := p.db.Exec(fmt.Sprintf(sql, p.conf.Table))
	return err
}

func (p *Plugin) RegisterUser(id string, firstname string, lastname string, username string) {
	l := p.logger
	u, err := p.FindByID(id)
	if err != nil {
		l.WithError(err).WithField("id", id).Warn("find by id failed for id")
		return
	}
	if u != nil {
		return
	}
	u = &User{ID: id, Firstname: firstname, Lastname: lastname, Username: username}
	if err := p.Insert(*u); err != nil {
		l.WithError(err).Warn("error writing telegram user to db")
	}
}

func (p *Plugin) Insert(u User) error {
	sql := `INSERT INTO %s (id, firstname, lastname, username) VALUES (:id, :firstname, :lastname, :username)`
	_, err := p.db.NamedExec(fmt.Sprintf(sql, p.conf.Table), map[string]interface{}{
		"id":        u.ID,
		"firstname": u.Firstname,
		"lastname":  u.Lastname,
		"username":  u.Username,
	})
	return err
}

func (p *Plugin) Delete(id string) error {
	_, err := p.db.NamedExec(fmt.Sprintf("DELETE FROM %s WHERE id = :id", p.conf.Table), map[string]interface{}{"id": id})
	return err
}

func (p *Plugin) FindByID(id string) (*User, error) {
	var u User
	err := p.db.Get(&u, fmt.Sprintf("SELECT * FROM %s WHERE id = $1", p.conf.Table), id)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &u, nil
}

func (p *Plugin) Exists(id string) (bool, error) {
	var u User
	err := p.db.Get(&u, fmt.Sprintf("SELECT id FROM %s WHERE id = $1", p.conf.Table), id)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (p *Plugin) FindAll() ([]*User, error) {
	rows, err := p.db.Queryx(fmt.Sprintf("SELECT * FROM %s", p.conf.Table))
	if err != nil {
		return nil, err
	}

	users := []*User{}
	for rows.Next() {
		var u User
		err = rows.StructScan(&u)

		if err != nil {
			return nil, err
		}

		users = append(users, &u)
	}

	return users, nil
}
