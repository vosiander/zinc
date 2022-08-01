package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
)

type (
	Plugin struct {
		logger  *logrus.Entry
		conf    Config
		krs     map[string]*kafka.Reader
		kws     map[string]*kafka.Writer
		metrics MetricsWriter
	}

	Config struct {
		Enable  bool   `env:"KAFKA_ENABLE" default:"false" yaml:"enable"`
		Brokers string `env:"KAFKA_BROKERS" default:"localhost:9092" yaml:"brokers"`
	}

	MetricsWriter interface {
		AddCounter(name string, count float64, help string)
	}

	nullMetricsWriter struct {
	}
)

const Name = "kafka"

func New() *Plugin {
	return &Plugin{
		krs: map[string]*kafka.Reader{},
		kws: map[string]*kafka.Writer{},
	}
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	p.conf = conf.(Config)
	for _, d := range dependencies {
		switch dp := d.(type) {
		case *logrus.Entry:
			p.logger = dp.WithField("component", "kafka-reader")
		case MetricsWriter:
			p.metrics = dp
		}
	}

	if p.logger == nil {
		p.logger = logrus.WithField("component", "kafka-reader")
	}

	if !p.conf.Enable {
		p.logger.Debug("kafka is not enabled. nothing to init...")
		return p
	}

	p.logger.Debug("finished init kafka...")

	return p
}

func (p *Plugin) EnableMetrics(metrics MetricsWriter) {
	p.metrics = metrics
}

func (p *Plugin) ReadFromTopic(topic string, consumerGroupID string, messageC chan kafka.Message) {
	ctx := context.Background()
	p.ReadFromTopicWithContext(ctx, topic, consumerGroupID, messageC)
}

func (p *Plugin) ReadFromTopicWithContext(ctx context.Context, topic string, consumerGroupID string, messageC chan kafka.Message) {
	l := p.logger.WithField("component", "kafka-reader-messages")

	if !p.conf.Enable {
		l.Warn("kafka is not enabled. nothing to read from...")
		return
	}

	if _, isOK := p.krs[topic]; !isOK {
		p.krs[topic] = kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(p.conf.Brokers, ","),
			GroupID: consumerGroupID,
			Topic:   topic,
		})
	}
	kr := p.krs[topic]

	l.Debug("starting reading kafka messages routine")

	for {
		select {
		case <-ctx.Done():
			l.Trace("finished reading kafka messages...")

			return
		default:
			m, err := kr.FetchMessage(ctx)
			if err != nil {
				l.WithError(err).Error("error fetching kafka message")
				break // TODO restart reading?
			}
			l.WithFields(logrus.Fields{
				"Topic":     m.Topic,
				"Partition": m.Partition,
				"Offset":    m.Offset,
				"Key":       string(m.Key),
				"Value":     string(m.Value),
			}).Trace("message received") // TODO trace?

			messageC <- m

			if err := kr.CommitMessages(ctx, m); err != nil {
				l.WithField("message", m).WithError(err).Error("could not commit messge!")
				p.metrics.AddCounter("kafka_"+topic+"_failure", 1.0, fmt.Sprintf("kafka [%s] failure message count", topic))
				continue
			}

			p.metrics.AddCounter("kafka_"+topic+"_success", 1.0, fmt.Sprintf("kafka [%s] successfull message count", topic))
			l.Trace("message commited")
		}
	}
}

func (p *Plugin) WriteToTopic(topic string, key string, value string) error {
	l := p.logger.WithField("component", "kafka-writer-messages")

	if !p.conf.Enable {
		l.Warn("kafka is not enabled. nothing to read from...")
		return nil
	}

	if _, isOK := p.kws[topic]; !isOK {
		p.kws[topic] = &kafka.Writer{
			Addr:     kafka.TCP(p.conf.Brokers),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}
	}

	err := p.kws[topic].WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: []byte(value),
		},
	)
	if err != nil {
		l.WithError(err).Error("failed to write message")
		return err
	}

	l.Trace("finished writing kafka message")
	return nil
}

func (p *Plugin) Close() error {
	if !p.conf.Enable {
		return nil
	}

	for topic, kr := range p.krs {
		p.logger.WithField("topic", topic).Debug("closing kafka reader")
		kr.Close()
	}

	for topic, kw := range p.kws {
		p.logger.WithField("topic", topic).Debug("closing kafka writer")
		kw.Close()
	}

	return nil
}

func (nmw *nullMetricsWriter) AddCounter(name string, count float64, help string) {}
