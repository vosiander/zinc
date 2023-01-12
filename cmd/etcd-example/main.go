package main

import (
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
		ConfigFile      string `short:"c" long:"config" description:"config file" default:""`
		ConfiguratorURL string `short:"u" long:"configurator" description:"Configurator URL" default:"https://config.example.com"`
	}
)

var (
	conf    Config
	cliOpts CLIOptions
)

func main() {
	shutdownChan := make(chan bool)
	c := core.NewCore(&conf, &cliOpts).
		WithOptions(&conf,
			core.LoadConfigFromEnvironment(cliOpts.ConfigFile, "etcd-example", cliOpts.ConfiguratorURL),
			core.CLIShutdownFunc(func() { shutdownChan <- true }),
		).
		WithAllPlugins(conf.Core)

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
