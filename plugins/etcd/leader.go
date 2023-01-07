package etcd

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type (
	Leader struct {
		logger   logrus.FieldLogger
		session  *concurrency.Session
		election *concurrency.Election
		ctx      context.Context
		e        *clientv3.Client
		isLeader bool

		key    string
		prefix string
	}

	LeaderOption func(lead *Leader) error
)

const (
	key = "zinc-leader-election"
)

func NewLeader(e *clientv3.Client, l logrus.FieldLogger, serviceID string, opts ...LeaderOption) *Leader {
	lead := &Leader{
		e:      e,
		logger: l,
		key:    key,
		prefix: serviceID,
	}

	for _, o := range opts {
		if err := o(lead); err != nil {
			l.WithError(err).Warn("error executing leader option")
		}
	}

	return lead
}

func (lead *Leader) Close() error {
	if err := lead.election.Resign(lead.ctx); err != nil {
		lead.logger.WithError(err).Warn("error resigning leader from election")
	}

	if err := lead.session.Close(); err != nil {
		lead.logger.WithError(err).Warn("error closing etcd session")
	}

	return nil
}

func (lead *Leader) Acquire() error {
	l := lead.logger.WithField("component", "etcd-leader")

	s, err := concurrency.NewSession(lead.e)
	if err != nil {
		return err
	}

	lead.session = s
	lead.ctx = context.Background()
	lead.election = concurrency.NewElection(s, lead.key)

	go func() {
		l.Trace("before election")

		if err := lead.elect(lead.election, lead.ctx, lead.prefix); err != nil {
			l.WithError(err).Warn("error during election")
		}
	}()

	for {
		resp, err := lead.election.Leader(lead.ctx)
		if err != nil {
			if err != concurrency.ErrElectionNoLeader {
				l.WithError(err).Warn("Leader returned error", err)
				return err
			}
			l.WithError(err).Warn("error getting leader. retrying")
			time.Sleep(time.Millisecond * 300)
			continue
		}
		if string(resp.Kvs[0].Value) != lead.prefix {
			l.WithFields(map[string]any{"leader": string(resp.Kvs[0].Value), "we": lead.prefix}).Debug("we are follower")
		}
		break
	}

	return nil
}

func (lead *Leader) Resign() error {
	return lead.election.Resign(lead.ctx)
}

func (lead *Leader) IsLeader() bool {
	return lead.isLeader
}

func (lead *Leader) elect(election *concurrency.Election, ctx context.Context, id string) error {
	l := lead.logger.WithField("component", "etcd-leader-election")

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
				lead.isLeader = false
				if string(resp.Kvs[0].Value) == id {
					lead.isLeader = true
				}
			}
		}
	}()
	return nil
}

func WithElectionKey(key string) LeaderOption {
	return func(lead *Leader) error {
		lead.key = key
		return nil
	}
}

func WithElectionPrefix(prefix string) LeaderOption {
	return func(lead *Leader) error {
		lead.prefix = prefix
		return nil
	}
}
