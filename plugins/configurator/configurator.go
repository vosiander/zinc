package configurator

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/creasty/defaults"

	"github.com/caarlos0/env/v6"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const Name = "configurator"

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
	bp.Logger = logrus.WithField("component", "configurator-plugin")
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

func (bp *Plugin) LoadConfig(url string, name string, conf interface{}) error {
	l := bp.Logger

	if err := defaults.Set(conf); err != nil {
		panic(err)
	}

	resp, err := http.Get(fmt.Sprintf("%s/config/%s?raw=1", url, name))
	if err != nil {
		return err
	}
	l.WithField("code", resp.StatusCode).Trace("status code from config service")

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	l.Trace(string(data))

	if err := json.Unmarshal(data, conf); err != nil {
		bp.Logger.WithError(err).Error("could not unmarshal json from configurator")
		return err
	}

	if err := env.Parse(conf); err != nil {
		fmt.Printf("%+v\n", err)
	}

	return err
}
