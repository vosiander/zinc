package etcd

import (
	"github.com/rs/xid"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
	"time"
)

type (
	Plugin struct {
		logger *logrus.Entry
		conf   Config
		e      *clientv3.Client
		lead   *Leader
		id     string
	}

	Config struct {
		Enable      bool          `env:"ETCD_ENABLE" default:"false" yaml:"enable"`
		Endpoints   string        `env:"ETCD_ENDPOINTS" default:"localhost:2379" yaml:"host"`
		DialTimeout time.Duration `env:"ETCD_DIAL_TIMEOUT" default:"5m" yaml:"dialTimeout"`
	}
)

const Name = "etcd"

func New() *Plugin {
	return &Plugin{}
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	for _, d := range dependencies {
		if dp, isOk := d.(*logrus.Entry); isOk {
			p.logger = dp.WithField("component", "etcd")
		}
		if id, isOk := d.(xid.ID); isOk {
			p.id = id.String()
		}
	}
	if p.logger == nil {
		p.logger = logrus.WithField("component", "etcd")
	}
	l := p.logger
	p.conf = conf.(Config)

	if !p.conf.Enable {
		l.Debug("etcd is not enabled. nothing to init...")
		return p
	}

	endpoints := strings.Split(p.conf.Endpoints, ",")

	l.WithFields(logrus.Fields{"endpoints": endpoints}).Debug("finished init etcd...")
	e, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: p.conf.DialTimeout,
	})

	if err != nil {
		l.WithError(err).Fatal("etcd could not connect. failing")
	}

	p.e = e

	return p
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Close() error {
	if !p.conf.Enable {
		return nil
	}

	if p.lead != nil {
		if err := p.lead.Close(); err != nil {
			p.logger.Warn("error resigning leader")
		}
	}

	return p.e.Close()
}

func (p *Plugin) Etcd() *clientv3.Client {
	return p.e
}

func (p *Plugin) Leader(opts ...LeaderOption) *Leader {
	if p.lead == nil {
		p.lead = NewLeader(p.e, p.logger.WithField("component", "etcd-leader"), p.id, opts...)
	}
	return p.lead
}
