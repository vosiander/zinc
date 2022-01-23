package boot

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/xid"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const Name = "boot"

type (
	Plugin struct {
		Logger      *log.Entry
		ID          xid.ID
		SignalChan  chan os.Signal
		CleanupDone chan bool
	}

	Config struct {
		Home string `env:"HOME" yaml:"home"`
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
	setLogLevel(os.Getenv("LOG_LEVEL"))
	bp.Logger = logrus.WithField("component", "boot-plugin")
	l := bp.Logger.WithField("component", "boot")

	bp.ID = xid.New()
	l.Infof("ID is %s", bp.ID.String())

	bp.SignalChan = make(chan os.Signal, 1)
	bp.CleanupDone = make(chan bool)
	signal.Notify(bp.SignalChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	return bp
}

func (bp *Plugin) IsEnabled() bool {
	return true
}

func (bp *Plugin) Close() error {
	return nil
}

func setLogLevel(logLevel string) {
	if logLevel != "" {
		lvl, err := log.ParseLevel(logLevel)
		if err == nil {
			log.SetLevel(lvl)
		}
	}
}
