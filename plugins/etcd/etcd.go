package etcd

import (
	"context"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"strings"
	"time"
)

type (
	Plugin struct {
		logger          *logrus.Entry
		conf            Config
		e               *clientv3.Client
		session         *concurrency.Session
		election        *concurrency.Election
		electionContext context.Context
		isLeader        bool
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

	if err := p.ResignLeader(); err != nil {
		p.logger.Warn("error resigning leader")
	}

	if err := p.session.Close(); err != nil {
		p.logger.Warn("error closing etcd session")
	}

	return p.e.Close()
}

func (p *Plugin) Etcd() *clientv3.Client {
	return p.e
}

func (p *Plugin) LockWithContext(ctx context.Context, key string, do func() error) error {
	l := p.logger.WithField("component", "etcd-lock")

	s, err := concurrency.NewSession(p.e)
	if err != nil {
		return err
	}
	defer s.Close()

	m := concurrency.NewMutex(s, key)
	if err := m.Lock(ctx); err != nil {
		return err
	}

	l.Tracef("acquired lock for %s", key)
	defer m.Unlock(ctx)
	if err := do(); err != nil {
		return err
	}
	l.Tracef("released lock for %s", key)

	return nil
}

func (p *Plugin) LockWithTTL(key string, ttl time.Duration, do func() error) error {
	ctx, cancel := context.WithTimeout(context.Background(), ttl)
	defer cancel()

	return p.LockWithContext(ctx, key, do)
}

func (p *Plugin) Lock(key string, do func() error) error {
	return p.LockWithContext(context.Background(), key, do)
}

func (p *Plugin) AcquireLeader(key string, prefix string) error {
	l := p.logger.WithField("component", "etcd-leader")

	s, err := concurrency.NewSession(p.e)
	if err != nil {
		return err
	}

	p.session = s
	p.electionContext = context.Background()
	p.election = concurrency.NewElection(s, key)

	go func() {
		l.Trace("before election")

		if err := p.elect(p.election, p.electionContext, prefix); err != nil {
			l.WithError(err).Warn("error during election")
		}
	}()

	for {
		resp, err := p.election.Leader(p.electionContext)
		if err != nil {
			if err != concurrency.ErrElectionNoLeader {
				l.WithError(err).Warn("Leader returned error", err)
				return err
			}
			l.WithError(err).Warn("error getting leader. retrying")
			time.Sleep(time.Millisecond * 300)
			continue
		}
		if string(resp.Kvs[0].Value) != prefix {
			l.WithFields(map[string]any{"leader": string(resp.Kvs[0].Value), "we": prefix}).Debug("we are NOT leader")
		}
		break
	}

	return nil
}

func (p *Plugin) elect(election *concurrency.Election, ctx context.Context, id string) error {
	l := p.logger.WithField("component", "etcd-leader-election")

	if err := election.Campaign(ctx, id); err != nil {
		l.WithError(err).Trace("election campaign failed")
		return err
	}

	l.Trace("election campaign finished")

	obsChan := election.Observe(ctx)

	go func() {
		l.Trace("election observing")
		for {
			select {
			case resp, ok := <-obsChan:
				if !ok {
					l.Warn("election observation channel closed")
					return
				}

				l.Debugf("current leader value %s --> our %s", string(resp.Kvs[0].Value), id)
				p.isLeader = false
				if string(resp.Kvs[0].Value) == id {
					p.isLeader = true
				}
			}
		}
	}()
	return nil
}

func (p *Plugin) ResignLeader() error {
	return p.election.Resign(p.electionContext)
}

func (p *Plugin) IsLeader() bool {
	return p.isLeader
}
