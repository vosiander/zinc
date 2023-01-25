package main

import (
	"encoding/json"
	"time"

	"github.com/siklol/zinc/core"
	"github.com/siklol/zinc/plugins/etcd"
)

type (
	Config struct {
		Test int                  `yaml:"test" json:"test"`
		Core core.AllPluginConfig `yaml:"core" json:"core"`
	}

	CLIOptions struct {
		ConfigFile          string `short:"c" long:"config" description:"config file" default:""`
		ConfiguratorBrokers string `short:"b" long:"brokers" default:"localhost:9093"`
	}
)

var (
	conf    Config
	cliOpts CLIOptions
	c       *core.Core
)

func main() {
	shutdownChan := make(chan bool)
	c = core.NewCore(&conf, &cliOpts)
	c.WithOptions(&conf,
		core.LoadKafkaConfigurator(cliOpts.ConfiguratorBrokers, "etcd-example", func(cfg string) error { return json.Unmarshal([]byte(cfg), &conf) }),
		core.CLIShutdownFunc(func() { shutdownChan <- true }),
	)
	c.WithAllPlugins(conf.Core)

	l := c.Logger()
	etcdP := c.MustGet(etcd.Name).(*etcd.Plugin)

	c.CLI(func() {
		l.Debug("started")
		lead := etcdP.Leader()

		if err := lead.Acquire(); err != nil {
			l.WithError(err).Warn("could not acquire leader position")
		}

		t := time.Tick(5 * time.Second)
		go func() {
			for {
				select {
				case <-shutdownChan:
					l.Warn("closing etcd loop")
					return
				case <-t:
					if lead.IsLeader() {
						l.Debug("--- leader")
					} else {
						l.Debug("~~~ follower")
					}
				}
			}
		}()
	})
}
