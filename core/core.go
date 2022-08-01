package core

import (
	"github.com/denisbrodbeck/machineid"
	"github.com/siklol/zinc/plugins"
	"github.com/siklol/zinc/plugins/boltdb"
	"github.com/siklol/zinc/plugins/boot"
	"github.com/siklol/zinc/plugins/clidaemon"
	"github.com/siklol/zinc/plugins/configurator"
	"github.com/siklol/zinc/plugins/etcd"
	"github.com/siklol/zinc/plugins/eventstore"
	"github.com/siklol/zinc/plugins/eventstore/eventsourcing"
	boltdb2 "github.com/siklol/zinc/plugins/eventstore/storage/boltdb"
	postgres2 "github.com/siklol/zinc/plugins/eventstore/storage/postgres"
	"github.com/siklol/zinc/plugins/githelper"
	heartbeat_consumer "github.com/siklol/zinc/plugins/heartbeat-consumer"
	heartbeat_publisher "github.com/siklol/zinc/plugins/heartbeat-publisher"
	"github.com/siklol/zinc/plugins/kafka"
	"github.com/siklol/zinc/plugins/loglevel"
	"github.com/siklol/zinc/plugins/nats"
	"github.com/siklol/zinc/plugins/postgres"
	postgres_crud "github.com/siklol/zinc/plugins/postgres-crud"
	"github.com/siklol/zinc/plugins/prometheus"
	"github.com/siklol/zinc/plugins/rest"
	"github.com/siklol/zinc/plugins/restjwt"
	"github.com/siklol/zinc/plugins/s3file"
	"github.com/siklol/zinc/plugins/telegram"
	"github.com/siklol/zinc/plugins/usermanager"
	"github.com/siklol/zinc/plugins/yamlloader"
	"github.com/sirupsen/logrus"
	"os"
	"sync"
	"syscall"
)

type (
	Core struct {
		logger  *logrus.Entry
		plugins map[string]plugins.Plugin
		bp      *boot.Plugin
		yl      *yamlloader.Plugin
		cl      *configurator.Plugin
		cliD    *clidaemon.Plugin
	}

	MinimalPluginConfig struct {
		Home      boot.Config      `yaml:"home" json:"home"`
		CLIDaemon clidaemon.Config `yaml:"cliDaemon" json:"cliDaemon"`
		Logging   loglevel.Config  `yaml:"logging" json:"logging"`
	}

	AllPluginConfig struct {
		Home               boot.Config                `yaml:"home" json:"home"`
		BoltDB             boltdb.Config              `yaml:"boltdb" json:"boltDb"`
		EventStore         eventstore.Config          `yaml:"eventstore" json:"eventstore"`
		CLIDaemon          clidaemon.Config           `yaml:"cliDaemon" json:"cliDaemon"`
		Logging            loglevel.Config            `yaml:"logging" json:"logging"`
		NATS               nats.Config                `yaml:"nats" json:"nats"`
		Etcd               etcd.Config                `yaml:"etcd" json:"etcd"`
		RestJWT            restjwt.Config             `yaml:"restJwt" json:"restJwt"`
		HeartbeatConsumer  heartbeat_consumer.Config  `yaml:"heartbeatConsumer" json:"heartbeatConsumer"`
		HeartbeatPublisher heartbeat_publisher.Config `yaml:"heartbeatPublisher" json:"heartbeatPublisher"`
		Kafka              kafka.Config               `yaml:"kafka" json:"kafka"`
		Postgres           postgres.Config            `yaml:"postgres" json:"postgres"`
		GitHelper          githelper.Config           `yaml:"gitHelper" json:"gitHelper"`
		// TODO enable libp2p when ready
		// Libp2p             libp2p.Config              `yaml:"libp2p" json:"libp2p"`
		PostgresCrud postgres_crud.Config `yaml:"postgresCrud" json:"postgresCrud"`
		Prometheus   prometheus.Config    `yaml:"prometheus" json:"prometheus"`
		REST         rest.Config          `yaml:"rest" json:"rest"`
		S3File       s3file.Config        `yaml:"s3File" json:"s3File"`
		Telegram     telegram.Config      `yaml:"telegram" json:"telegram"`
		Usermanager  usermanager.Config   `yaml:"usermanager" json:"usermanager"`
	}
)

func NewMinimalCore(flags ...interface{}) *Core {
	var c *Core
	if len(flags) > 0 {
		c = NewCore(nil, flags[0])
	} else {
		c = NewCore(nil, nil)
	}
	c.WithMinimalPlugins()
	return c
}

func NewCore(bootConf interface{}, flags interface{}) *Core {
	l := logrus.WithField("component", "core")

	c := &Core{
		bp:      boot.New().Boot(bootConf).(*boot.Plugin),
		yl:      yamlloader.New().Boot(bootConf).(*yamlloader.Plugin),
		cl:      configurator.New().Boot(bootConf).(*configurator.Plugin),
		cliD:    clidaemon.New().Boot(nil, l).(*clidaemon.Plugin),
		logger:  l,
		plugins: map[string]plugins.Plugin{},
	}

	if _, err := c.cliD.ParseFlags(flags, os.Args); err != nil {
		l.WithError(err).Fatal("error parsing flags")
	}

	return c
}

func (c *Core) WithOptions(bootConf interface{}, coreOptions ...Option) *Core {
	l := c.Logger()

	for _, opt := range coreOptions {
		if err := opt(c, bootConf); err != nil {
			l.WithError(err).Fatal("error calling core options")
		}
	}

	return c
}

func (c *Core) WithMinimalPlugins() *Core {
	l := c.Logger()

	c.Register(c.yl, c.cl, c.cliD)
	l.Trace("boot finished")
	return c
}

func (c *Core) WithAllPlugins(config AllPluginConfig) *Core {
	c.SetLogLevel(config.Logging)

	l := c.Logger()
	c.Register(c.yl, c.cl, c.cliD)

	p := postgres.New().Boot(config.Postgres, l).(*postgres.Plugin)
	pr := prometheus.New().Boot(config.Prometheus, l).(*prometheus.Plugin)
	n := nats.New().Boot(config.NATS, l).(*nats.Plugin)
	um := usermanager.New().Boot(config.Usermanager, l, p).(*usermanager.Plugin)
	es := eventstore.New().Boot(config.EventStore, l).(*eventstore.Plugin)
	r := rest.New().Boot(config.REST, l).(*rest.Plugin)

	c.Register(boltdb.New().Boot(config.BoltDB, l).(*boltdb.Plugin))
	c.Register(postgres_crud.New().Boot(config.PostgresCrud, l, p).(*postgres_crud.Plugin))
	c.Register(kafka.New().Boot(config.Kafka, l, pr).(*kafka.Plugin))
	c.Register(heartbeat_consumer.New().Boot(config.HeartbeatConsumer, c.bp.ID, l, n).(*heartbeat_consumer.Plugin))
	c.Register(heartbeat_publisher.New().Boot(config.HeartbeatPublisher, c.bp.ID, l, n).(*heartbeat_publisher.Plugin))
	c.Register(telegram.New().Boot(config.Telegram, l, um).(*telegram.Plugin))
	c.Register(s3file.New().Boot(config.S3File, l).(*s3file.Plugin))
	c.Register(githelper.New().Boot(config.GitHelper, l).(*githelper.Plugin))
	c.Register(etcd.New().Boot(config.Etcd, l, c.bp.ID).(*etcd.Plugin))
	c.Register(restjwt.New().Boot(config.RestJWT, l).(*restjwt.Plugin))
	// c.Register(libp2p.New().Boot(config.Libp2p, l, r.Router()).(*libp2p.Plugin))
	c.Register(p, pr, n, um, es, r)

	if es.IsEnabled() {
		storages := []eventsourcing.Storage{}
		if config.EventStore.Storages.Postgres.Enable {
			storages = append(storages, postgres2.NewPostgresEventStorage(
				l,
				config.EventStore.Storages.Postgres,
				postgres.NewGolangMigrator(l, config.EventStore.Storages.Postgres.MigrationsTable),
			))
		}

		if config.EventStore.Storages.BoltDB.Enable {
			storages = append(storages, boltdb2.NewBoltDBEventStorage(l, config.EventStore.Storages.BoltDB))
		}

		es.BootStorage(storages...)
	}

	return c
}

func (c *Core) SetLogLevel(conf loglevel.Config) *Core {
	loglevel.New().Boot(conf).Start()
	c.logger = logrus.WithField("component", "core")
	return c
}

func (c *Core) MachineID() (string, error) {
	return machineid.ID()
}

func (c *Core) Register(plugins ...plugins.Plugin) *Core {
	for _, p := range plugins {
		c.plugins[p.Name()] = p
	}

	return c
}

func (c *Core) CLI(f func()) {
	l := c.Logger()

	go c.MustGet(clidaemon.Name).(*clidaemon.Plugin).RunCLI(func() {
		defer func() { c.SendSigIntSignal() }()

		f()

		l.Info("done")
	})
	go c.Shutdown(func() {})
	<-c.WaitOnCleanup()
}

func (c *Core) CustomConfig(name string, conf interface{}) {
	l := c.Logger()
	if err := c.MustGet(yamlloader.Name).(*yamlloader.Plugin).LoadYamlConfig(name, conf); err != nil {
		l.WithError(err).Fatalf("error loading config: %s", name)
	}
}

func (c *Core) MustGet(name string) plugins.Plugin {
	if _, isOk := c.plugins[name]; !isOk {
		c.logger.WithField("name", name).Fatal("core plugin not found or booted")
	}
	return c.plugins[name]
}

func (c *Core) Logger() *logrus.Entry {
	return c.logger
}

func (c *Core) StartPlugins() {
	for pn, p := range c.plugins {
		go func(name string, cp plugins.Plugin) {
			if err := cp.Start(); err != nil {
				c.logger.
					WithError(err).
					WithField("component", "start-plugin-"+name).
					Error("error starting plugin")
			}
		}(pn, p)
	}
}

func (c *Core) ID() string {
	return c.bp.ID.String()
}

func (c *Core) SendSigIntSignal() {
	c.bp.SignalChan <- syscall.SIGINT
}

func (c *Core) close() {
	l := c.Logger()

	wg := sync.WaitGroup{}
	wg.Add(len(c.plugins))
	for pn, p := range c.plugins {
		go func(name string, cp plugins.Plugin) {
			l.Tracef("closing %s", name)
			if err := cp.Close(); err != nil {
				c.logger.
					WithError(err).
					WithField("component", "start-plugin-"+name).
					Error("error starting plugin")
			}
			wg.Done()
		}(pn, p)
	}
	wg.Wait()
}

func (c *Core) Shutdown(f func()) {
	for {
		select {
		case event := <-c.bp.SignalChan:
			l := c.logger.WithField("component", "shutdown")
			l.Infof("Received %s event...", event)
			c.close()

			f()

			l.Info("finished shutdown")
			c.bp.CleanupDone <- true
			return
		}
	}
}

func (c *Core) WaitOnCleanup() <-chan bool {
	return c.bp.CleanupDone
}
