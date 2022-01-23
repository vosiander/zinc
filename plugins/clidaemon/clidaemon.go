package clidaemon

import (
	"github.com/jessevdk/go-flags"
	_ "github.com/jessevdk/go-flags"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
)

type (
	Plugin struct {
		logger *logrus.Entry
		conf   Config
	}

	Config struct {
		Name        string `env:"CLI_DAEMON_NAME" default:"solar" yaml:"name"`
		Description string `env:"CLI_DAEMON_DESCRIPTION" yaml:"description"`
	}

	CLIOptions struct {
		ConfigFile string `short:"c" long:"config" description:"config file" default:"config.yaml"`
	}
)

const Name = "clidaemon"

func New() *Plugin {
	return &Plugin{}
}

func (cliD *Plugin) Name() string {
	return Name
}

func (cliD *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	for _, d := range dependencies {
		if dp, isOk := d.(*logrus.Entry); isOk {
			cliD.logger = dp.WithField("component", "cli-daeemon")
		}
	}
	if cliD.logger == nil {
		cliD.logger = logrus.WithField("component", "cli-daeemon")
	}

	return cliD
}

func (cliD *Plugin) ParseFlags(opts interface{}, args []string) ([]string, error) {
	return flags.ParseArgs(opts, args)
}

func (cliD *Plugin) IsEnabled() bool {
	return true
}

func (cliD *Plugin) Close() error {
	return nil
}

func (cliD *Plugin) Start() error {
	return nil
}

func (cliD *Plugin) RunCLI(f func()) error {
	f()
	return nil
}
