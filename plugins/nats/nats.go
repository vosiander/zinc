package nats

import (
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
)

type (
	Plugin struct {
		logger *logrus.Entry
		conf   Config
		nc     *nats.Conn
		sc     stan.Conn
		js     *jsm.Manager
	}

	Config struct {
		Enable      bool   `env:"NATS_ENABLE" default:"false" yaml:"enable"`
		Host        string `env:"NATS_HOST" yaml:"host"`
		ClientName  string `default:"mesh-node" env:"NATS_CLIENT_NAME" yaml:"clientName"`
		ClusterID   string `env:"NATS_CLUSTER_ID" yaml:"clusterId"`
		ClientID    string `env:"NATS_CLIENT_ID" yaml:"clientId"`
		WorkerQueue string `env:"NATS_WORKER_QUEUE" default:"mesh.worker-queue" yaml:"workerQueue"`
		JetStream   struct {
			Timeout int `env:"JETSTREAM_TIMEOUT" default:"10" yaml:"timeout"`
		} `yaml:"jetstream"`
	}
)

const Name = "nats"

func New() *Plugin {
	return &Plugin{}
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	for _, d := range dependencies {
		if dp, isOk := d.(*logrus.Entry); isOk {
			p.logger = dp.WithField("component", "nats-streaming")
		}
	}
	if p.logger == nil {
		p.logger = logrus.WithField("component", "nats-streaming")
	}
	var err error
	l := p.logger
	p.conf = conf.(Config)

	if !p.conf.Enable {
		l.Debug("nats is not enabled. nothing to init...")
		return p
	}

	nc, err := nats.Connect(p.conf.Host, []nats.Option{
		nats.Name(p.conf.ClientName),
		nats.ReconnectWait(1 * time.Second),
	}...)
	if err != nil {
		l.WithField("error-type", "nats connection error").Fatal(err)
	}
	p.nc = nc

	sc, err := stan.Connect(p.conf.ClusterID, p.conf.ClientID, stan.NatsConn(nc))
	if err != nil {
		l.WithField("error-type", "nats streaming connection error").Fatal(err)
	}
	p.sc = sc

	mgr, err := jsm.New(p.nc, jsm.WithTimeout(time.Duration(p.conf.JetStream.Timeout)*time.Second))
	if err != nil {
		l.WithField("error-type", "nats jetstream connection error").Fatal(err)
	}
	p.js = mgr

	l.WithFields(logrus.Fields{
		"cluster":   p.conf.Host,
		"clusterid": p.conf.ClusterID,
		"clientid":  p.conf.ClientID,
	}).Debug("finished init nats streaming...")

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
	p.nc.Close()
	return p.sc.Close()
}

func (p *Plugin) NC() *nats.Conn {
	return p.nc
}

func (p *Plugin) SC() stan.Conn {
	return p.sc
}

func (p *Plugin) JS() *jsm.Manager {
	return p.js
}
