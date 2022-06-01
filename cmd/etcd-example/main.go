package main

import (
	"github.com/siklol/zinc/core"
	"github.com/siklol/zinc/plugins/clidaemon"
	"github.com/siklol/zinc/plugins/etcd"
	"time"
)

type (
	Config struct {
		Test int                  `yaml:"test" json:"test"`
		Core core.AllPluginConfig `yaml:"core" json:"core"`
	}

	CLIOptions struct {
		ConfigFile      string `short:"c" long:"config" description:"config file" default:"config.yaml"`
		ConfiguratorURL string `short:"u" long:"configurator" description:"Configurator URL" default:"https://config.example.com"`
	}
)

var (
	conf    Config
	cliOpts CLIOptions
)

func main() {
	c := core.NewCore(&conf, &cliOpts).
		WithOptions(&conf, core.LoadYamlConfig(cliOpts.ConfigFile)).
		WithAllPlugins(conf.Core)

	l := c.Logger()
	etcdP := c.MustGet(etcd.Name).(*etcd.Plugin)

	c.StartPlugins()
	c.MustGet(clidaemon.Name).(*clidaemon.Plugin).RunCLI(func() {
		l.Debug("started")
		lead := etcdP.Leader()

		if err := lead.Acquire(); err != nil {
			l.WithError(err).Warn("could not acquire leader position")
		}

		t := time.Tick(5 * time.Second)
		go func() {
			for {
				select {
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
	go c.Shutdown(func() {
	})

	<-c.WaitOnCleanup()
}
