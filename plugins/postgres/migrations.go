package postgres

import (
	"database/sql"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/sirupsen/logrus"
)

type (
	GolangMigrator struct {
		logger          *logrus.Entry
		migrationsTable string
	}
)

func NewGolangMigrator(logger *logrus.Entry, migrationsTable string) *GolangMigrator {
	return &GolangMigrator{
		logger:          logger,
		migrationsTable: migrationsTable,
	}
}

func (m *GolangMigrator) PerformMigrations(db *sql.DB, path string, databaseName string) error {
	l := m.logger.WithField("component", "postgres-storage-migrations")

	l.Debug("executing migrations")
	driver, err := postgres.WithInstance(db, &postgres.Config{
		MigrationsTable: m.migrationsTable,
	})
	if err != nil {
		return err
	}
	ma, err := migrate.NewWithDatabaseInstance(path, databaseName, driver)
	if err != nil {
		return err
	}
	ma.Log = newGolangLogger(l.WithField("module", "migrator"))
	if err := ma.Up(); err != nil {
		if err != migrate.ErrNoChange {
			return err
		}
		l.Debug("no migrations applied")
	}
	l.Debug("finished migrations")

	return nil
}

type golangLogger struct {
	l *logrus.Entry
}

func newGolangLogger(l *logrus.Entry) *golangLogger {
	return &golangLogger{
		l: l,
	}
}

func (l *golangLogger) Printf(format string, v ...interface{}) {
	l.l.Infof(format, v...)
}

func (l *golangLogger) Verbose() bool {
	return true
}
