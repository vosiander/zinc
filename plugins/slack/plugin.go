package slack

import (
	"errors"

	"github.com/ashwanthkumar/slack-go-webhook"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const Name = "slack"

type (
	Plugin struct {
		logger *log.Entry
		conf   Config
	}

	Config struct {
		Enable bool   `env:"SLACK_ENABLE" default:"false" yaml:"enable"`
		URL    string `env:"SLACK_URL" yaml:"url"`
		Proxy  string `env:"SLACK_PROXY" yaml:"proxy"`
	}
)

func New() *Plugin {
	return &Plugin{}
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	p.conf = conf.(Config)

	for _, d := range dependencies {
		switch dp := d.(type) {
		case *logrus.Entry:
			p.logger = dp.WithField("component", "slack-plugin")
		}
	}
	l := p.logger

	if !p.conf.Enable {
		l.Debug("slack is not enabled. nothing to init...")
		return p
	}

	if p.conf.URL == "" {
		l.Fatal("slack webhook url is empty.")
	}

	l.Debug("booting up...")

	return p
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Close() error {
	return nil
}

func (p *Plugin) Send(msg string) error {
	errStr := ""
	for _, e := range p.SendPayload(slack.Payload{Text: msg}) {
		errStr += e.Error()
	}

	return errors.New(errStr)
}

func (p *Plugin) SendPayload(payload slack.Payload) []error {
	return slack.Send(p.conf.URL, p.conf.Proxy, payload)
}
