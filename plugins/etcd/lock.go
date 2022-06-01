package etcd

import (
	"context"
	"go.etcd.io/etcd/client/v3/concurrency"
	"time"
)

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
