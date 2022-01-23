package loglevel

import (
	"os"

	"github.com/siklol/zinc/plugins"
	log "github.com/sirupsen/logrus"
)

type (
	Plugin struct {
		conf Config
	}

	Config struct {
		LogLevel   string `env:"LOG_LEVEL" default:"INFO" yaml:"logLevel"`
		Output     string `env:"LOG_OUTPUT" yaml:"output"` // possible variables: file
		OutputFile string `env:"LOG_OUTPUT_FILE" yaml:"outputFile"`
	}
)

const Name = "loglevel"

func New() *Plugin {
	return &Plugin{}
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Start() error {
	setLogLevel(p.conf.LogLevel)
	switchLoggingOutput(p.conf)
	return nil
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	return &Plugin{
		conf: conf.(Config),
	}
}

func (p *Plugin) Close() error {
	return nil
}

func (bp *Plugin) IsEnabled() bool {
	return true
}

func setLogLevel(logLevel string) {
	if logLevel != "" {
		lvl, err := log.ParseLevel(logLevel)
		if err == nil {
			log.SetLevel(lvl)
		}
	}
}

func switchLoggingOutput(conf Config) {
	if conf.Output == "" {
		return
	}

	f, err := os.OpenFile(conf.OutputFile, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.WithError(err).Fatal("could not open log file")
	}
	log.SetOutput(f)
}
