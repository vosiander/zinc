package kafka

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

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
		rMutex  *sync.RWMutex
		wMutex  *sync.RWMutex
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
		krs:    map[string]*kafka.Reader{},
		kws:    map[string]*kafka.Writer{},
		rMutex: &sync.RWMutex{},
		wMutex: &sync.RWMutex{},
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

func (p *Plugin) ReadFromTopicEnd(topic string, consumerGroupID string, messageC chan<- Message) {
	ctx := context.Background()
	p.ReadFromTopicWithContext(ctx, topic, consumerGroupID, true, messageC)
}

func (p *Plugin) ReadFromTopic(topic string, consumerGroupID string, messageC chan<- Message) {
	ctx := context.Background()
	p.ReadFromTopicWithContext(ctx, topic, consumerGroupID, false, messageC)
}

func (p *Plugin) ReadFromTopicWithContext(ctx context.Context, topic string, consumerGroupID string, useLastOffset bool, messageC chan<- Message) {
	l := p.logger.WithField("component", "kafka-reader-messages")

	if !p.conf.Enable {
		l.Warn("kafka is not enabled. nothing to read from...")
		return
	}

	startOffset := kafka.FirstOffset
	if useLastOffset {
		startOffset = kafka.LastOffset
	}

	kr := p.getReader(topic)
	if kr == nil {
		kr = kafka.NewReader(kafka.ReaderConfig{
			Brokers:     strings.Split(p.conf.Brokers, ","),
			GroupID:     consumerGroupID,
			Topic:       topic,
			StartOffset: startOffset,
		})
		p.addToReaderMap(topic, kr)
	}

	l.Debug("starting reading kafka messages routine")

	for {
		select {
		case <-ctx.Done():
			l.Trace("finished reading kafka messages...")

			return
		default:
			m, err := kr.FetchMessage(ctx)
			if err != nil && err == io.EOF {
				l.WithError(err).Warn("reader has been closed")
				return
			} else if err != nil {
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

			messageC <- Message{
				Topic:         m.Topic,
				Partition:     m.Partition,
				Offset:        m.Offset,
				HighWaterMark: m.HighWaterMark,
				Key:           m.Key,
				Value:         m.Value,
				Time:          m.Time,
			}

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

	kw := p.getWriter(topic)
	if kw == nil {
		kw = &kafka.Writer{
			Addr:     kafka.TCP(p.conf.Brokers),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}
		p.addToWriterMap(topic, kw)
	}

	err := kw.WriteMessages(context.Background(),
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

func (p *Plugin) WriteToTopicAsync(topic string, key string, value string) {
	l := p.logger.WithField("component", "kafka-writer-messages")

	if !p.conf.Enable {
		l.Fatal("kafka is not enabled. nothing to read from...")
	}

	kw := p.getWriter(topic)
	if kw == nil {
		kw = &kafka.Writer{
			Addr:     kafka.TCP(p.conf.Brokers),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
			Async:    true,
		}
		p.addToWriterMap(topic, kw)
	}

	kw.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: []byte(value)})
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

func (p *Plugin) addToReaderMap(topic string, reader *kafka.Reader) {
	defer p.rMutex.Unlock()
	p.rMutex.Lock()

	p.krs[topic] = reader
}

func (p *Plugin) getReader(topic string) *kafka.Reader {
	defer p.rMutex.RUnlock()
	p.rMutex.RLock()

	if _, isOK := p.krs[topic]; !isOK {
		return nil
	}

	return p.krs[topic]
}

func (p *Plugin) addToWriterMap(topic string, writer *kafka.Writer) {
	defer p.wMutex.Unlock()
	p.wMutex.Lock()

	p.kws[topic] = writer

}

func (p *Plugin) getWriter(topic string) *kafka.Writer {
	defer p.wMutex.RUnlock()
	p.wMutex.RLock()

	if _, isOK := p.kws[topic]; !isOK {
		return nil
	}

	return p.kws[topic]
}
