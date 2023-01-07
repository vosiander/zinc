package telegram

import (
	"time"

	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	tb "gopkg.in/telebot.v3"
)

const Name = "telegram"

type (
	Plugin struct {
		Logger *log.Entry
		b      *tb.Bot
		conf   Config
	}

	Config struct {
		Enable    bool   `env:"TELEGRAM_ENABLE" default:"false" yaml:"enable"`
		APIkey    string `env:"TELEGRAM_API_KEY" yaml:"apiKey"`
		URL       string `env:"TELEGRAM_URL" yaml:"url"`
		UserTable string `env:"TELEGRAM_USER_TABLE" yaml:"userTable"`
	}
)

func New() *Plugin {
	return &Plugin{}
}

func (bp *Plugin) Start() error {
	if !bp.IsEnabled() {
		return nil
	}
	bp.Logger.Debug("start telegram bot")
	bp.b.Start()
	return nil
}

func (bp *Plugin) Name() string {
	return Name
}

func (bp *Plugin) IsEnabled() bool {
	return bp.conf.Enable
}

func (bp *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	bp.conf = conf.(Config)

	for _, d := range dependencies {
		switch dp := d.(type) {
		case *logrus.Entry:
			bp.Logger = dp.WithField("component", "telegram-bot")
		}
	}
	l := bp.Logger

	if !bp.IsEnabled() {
		return bp
	}

	b, err := tb.NewBot(tb.Settings{
		URL:    bp.conf.URL,
		Token:  bp.conf.APIkey,
		Poller: &tb.LongPoller{Timeout: 10 * time.Second},
	})
	bp.b = b

	if err != nil {
		l.WithError(err).Fatal("could not boot telegram bot")
	}

	l.Debug("done booting up...")

	return bp
}

func (bp *Plugin) Close() error {
	if !bp.IsEnabled() {
		return nil
	}
	bp.b.Stop()
	return nil
}

func (bp *Plugin) Bot() *tb.Bot {
	if !bp.IsEnabled() {
		bp.Logger.Fatal("telegram plugin is disabled. Bot cannot be retrieved. Failing!")
	}
	return bp.b
}
