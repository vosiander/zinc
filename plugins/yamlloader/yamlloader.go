package yamlloader

import (
	"fmt"
	"io/ioutil"

	"github.com/caarlos0/env/v6"

	"github.com/creasty/defaults"

	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

const Name = "yamlloader"

type (
	Plugin struct {
		Logger *log.Entry
	}

	Config struct {
	}
)

func New() *Plugin {
	return &Plugin{}
}

func (bp *Plugin) Start() error {
	return nil
}

func (bp *Plugin) Name() string {
	return Name
}

func (bp *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	bp.Logger = logrus.WithField("component", "yaml-plugin")
	l := bp.Logger

	l.Debug("booting up...")

	return bp
}

func (bp *Plugin) IsEnabled() bool {
	return true
}

func (bp *Plugin) Close() error {
	return nil
}

func (bp *Plugin) LoadYamlConfig(name string, conf interface{}) error {
	if err := defaults.Set(conf); err != nil {
		panic(err)
	}

	data, err := ioutil.ReadFile(name)
	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(data, conf); err != nil {
		bp.Logger.WithError(err).Error("could not unmarshal yaml")
		return err
	}

	if err := env.Parse(conf); err != nil {
		fmt.Printf("%+v\n", err)
	}

	return err
}
