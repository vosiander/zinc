package heartbeat_consumer

import (
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/rs/xid"
	"github.com/siklol/heartbeat"
	"github.com/siklol/zinc/plugins"
	natsPlugin "github.com/siklol/zinc/plugins/nats"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const Name = "heartbeat-consumer"

type (
	Plugin struct {
		logger *log.Entry
		hbh    *heartbeat.Handler
		id     xid.ID
		conf   Config
		nc     *nats.Conn
	}

	Config struct {
		Enable                bool          `env:"HEARTBEAT_CONSUMER_ENABLE" default:"false" yaml:"enable"`
		Duration              time.Duration `env:"HEARTBEAT_CONSUMER_DURATION" default:"5m" yaml:"duration"`
		HeartbeatTopic        string        `env:"HEARTBEAT_CONSUMER_HEARTBEAT_TOPIC" default:"go-plugins-heartbeat" yaml:"heartbeatTopic"`
		ConsumerRegisterTopic string        `env:"HEARTBEAT_CONSUMER_CONSUMER_REGISTER_TOPIC" default:"go-plugins-heartbeat-consumer-register" yaml:"consumerRegisterTopic"`
	}
)

func New() *Plugin {
	return &Plugin{}
}

func (p *Plugin) Start() error {
	if !p.conf.Enable {
		return nil
	}
	p.logger.Debug("start listening for heartbeat publishers")

	p.hbh.ListenForHeartbeatPublisher()
	return nil
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	p.id = xid.New()
	p.conf = conf.(Config)

	for _, d := range dependencies {
		switch dp := d.(type) {
		case xid.ID:
			p.id = dp
		case *logrus.Entry:
			p.logger = dp.WithField("component", "heartbeat-consumer-plugin")
		case *natsPlugin.Plugin:
			if !dp.IsEnabled() {
				return p
			}
			p.nc = dp.NC()
		}
	}
	l := p.logger

	if !p.conf.Enable {
		l.Debug("heartbeat consumer is not enabled. nothing to init...")
		return p
	}

	if p.nc == nil {
		l.Fatal("nats connection not available. failing")
	}

	l.Debug("starting up node Heartbeat")
	p.hbh = heartbeat.NewHandler(
		p.id.String(),
		p.nc,
		heartbeat.SetHeartbeatTickerTime(p.conf.Duration),
		heartbeat.SetLogger(l),
		heartbeat.SetHeartbeatTopic(p.conf.HeartbeatTopic),
		heartbeat.SetConsumerRegisterTopic(p.conf.ConsumerRegisterTopic),
	)

	l.Debug("booting up...")

	return p
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Close() error {
	return nil
}
