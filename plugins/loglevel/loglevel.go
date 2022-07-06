package loglevel

import (
	"os"
	"strings"

	"github.com/siklol/zinc/plugins"
	log "github.com/sirupsen/logrus"
)

type (
	Plugin struct {
		conf    Config
		logFile *os.File
	}

	Config struct {
		LogLevel  string `env:"LOG_LEVEL" default:"INFO" yaml:"logLevel"`
		LogFile   string `env:"LOG_FILE" yaml:"logFile"`
		LogFormat string `env:"LOG_FORMAT" yaml:"logFormat"`
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

	if strings.ToLower(p.conf.LogFormat) == "json" {
		log.SetFormatter(&log.JSONFormatter{})
	}

	if p.conf.LogFile == "" {
		return nil
	}

	f, err := os.OpenFile(p.conf.LogFile, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.WithError(err).Fatal("could not open log file")
	}

	p.logFile = f
	log.SetOutput(p.logFile)
	return nil
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	return &Plugin{
		conf: conf.(Config),
	}
}

func (p *Plugin) Close() error {
	if p.logFile != nil {
		p.logFile.Close()
	}
	return nil
}

func (p *Plugin) IsEnabled() bool {
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
