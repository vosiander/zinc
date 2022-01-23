package main

import (
	"github.com/siklol/zinc/cmd/eventstore/aggregate"
	"github.com/siklol/zinc/core"
	"github.com/siklol/zinc/plugins/clidaemon"
	"github.com/siklol/zinc/plugins/eventstore"
	"github.com/siklol/zinc/plugins/eventstore/eventsourcing"
	"github.com/sirupsen/logrus"
)

type (
	Config struct {
		Test int                  `yaml:"test" json:"test"`
		Core core.AllPluginConfig `yaml:"core" json:"core"`
	}

	CLIOptions struct {
		ConfiguratorURL string `short:"u" long:"configurator" description:"Configurator URL" default:"https://config.example.com"`
	}
)

var (
	conf    Config
	cliOpts CLIOptions
	db      = "boltdb"
)

func main() {
	c := core.NewCore(&conf, &cliOpts).
		WithOptions(&conf, core.LoadConfigurator("de.sikindustries.tools.eventstore", cliOpts.ConfiguratorURL)).
		WithAllPlugins(conf.Core)

	l := c.Logger()

	c.StartPlugins()
	c.MustGet(clidaemon.Name).(*clidaemon.Plugin).RunCLI(func() {
		defer func() { c.SendSigIntSignal() }()
		esP := c.MustGet(eventstore.Name).(*eventstore.Plugin)

		l.Debugf("%s eventstore", esP.Name())
		a := aggregate.NewExampleAggregate("abcdefg")
		err(l, a.On(&aggregate.BlaBlubEvent{M: eventsourcing.NewMetaWithID("abc-123", "blablub", "1.0"), FirstValue: "schnippi", SecondValue: "schnapp"}))
		err(l, a.On(&aggregate.BlaBlubEvent{M: eventsourcing.NewMetaWithID("xyz", "blablub", "1.0"), FirstValue: "hasdasd", SecondValue: "etzjetzj"}))
		err(l, a.On(&aggregate.BlaBlubEvent{M: eventsourcing.NewMetaWithID("5hwrzjedtum", "blablub", "1.0"), FirstValue: "wertwrgqerg", SecondValue: "netznetzn"}))

		store := esP.EventStore()

		if err := store.Persist(db, a); err != nil {
			l.WithError(err).Fatal("error saving eventstore")
		}

		a2 := aggregate.NewExampleAggregate("abcdefg")
		if err := store.Load(db, a2); err != nil {
			l.WithError(err).Fatal("error saving eventstore")
		}
		l.WithFields(logrus.Fields{"first": a2.First, "second": a2.Second}).Warn("a2 aggregate after loading")

		l.Info("done")
	})
	go c.Shutdown(func() {})

	<-c.WaitOnCleanup()
}

func err(l logrus.FieldLogger, err error) {
	if err == nil {
		return
	}
	l.WithError(err).Error("error handling")
}
