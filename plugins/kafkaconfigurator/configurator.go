package kafkaconfigurator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/xid"
	"github.com/segmentio/kafka-go"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const (
	Name          = "kafkaconfigurator"
	configTimeout = 15 * time.Second
)

type (
	Plugin struct {
		Logger    *log.Entry
		k         *BrokerHandler
		messageC  chan kafka.Message
		shutdownC chan bool
		bootupC   chan bool
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
	p.Logger = logrus.WithField("component", "kafkaconfigurator-plugin")
	l := p.Logger
	p.shutdownC = make(chan bool, 1)
	p.bootupC = make(chan bool, 1)
	p.messageC = make(chan kafka.Message, 1024)

	l.Debug("booting up...")

	return p
}

func (p *Plugin) IsEnabled() bool {
	return true
}

func (p *Plugin) Close() error {
	p.shutdownC <- true
	return nil
}

func (p *Plugin) LoadConfig(brokers string, topic string, updateFn func(conf string) error) error {
	l := p.Logger.WithField("module", "kafkaconsumer-channel")
	p.k = NewBrokerHandler(p.Logger, brokers)
	go func() {
		for {
			select {
			case <-time.After(configTimeout):
				p.bootupC <- false
			case <-p.shutdownC:
				l.Debug("shutdown kafkaconsumer-channel")
				return
			case m := <-p.messageC:
				l.Trace("config message received")
				if m.HighWaterMark > 0 && (m.HighWaterMark-1) > m.Offset {
					l.WithFields(map[string]interface{}{"HighWaterMark": m.HighWaterMark, "Offset": m.Offset}).
						Trace("skipping message as we have more messages in topic")
					continue
				}
				if err := updateFn(string(m.Value)); err != nil {
					l.WithError(err).Warn("could not update config")
				}
				l.Debug("finished updating config")

				p.bootupC <- true
			}
		}
	}()

	go func() {
		err := p.k.ReadFromTopicWithContext(context.Background(), topic, xid.New().String(), p.messageC)
		if err != nil {
			l.WithError(err).Fatal("error getting configuration")
		}
	}()

	if isOk := <-p.bootupC; !isOk {
		return errors.New(fmt.Sprintf("configuration could not be loaded after %d ms", configTimeout))
	}

	return nil
}
