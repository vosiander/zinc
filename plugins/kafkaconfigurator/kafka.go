package kafkaconfigurator

import (
	"context"
	"errors"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type (
	BrokerHandler struct {
		logger  *logrus.Entry
		brokers string
		krs     map[string]*kafka.Reader
	}
)

func NewBrokerHandler(l *logrus.Entry, brokers string) *BrokerHandler {
	return &BrokerHandler{
		logger:  l,
		brokers: brokers,
		krs:     map[string]*kafka.Reader{},
	}
}

func (bh *BrokerHandler) ReadFromTopicWithContext(ctx context.Context, topic string, consumerGroupID string, messageC chan kafka.Message) error {
	l := bh.logger.WithField("component", "kafka-reader-messages")

	if _, isOK := bh.krs[topic]; !isOK {
		bh.krs[topic] = kafka.NewReader(kafka.ReaderConfig{
			Brokers:     strings.Split(bh.brokers, ","),
			GroupID:     consumerGroupID,
			Topic:       topic,
			StartOffset: kafka.FirstOffset,
		})
	}
	kr := bh.krs[topic]

	l.Debug("starting reading kafka messages routine")

	for {
		select {
		case <-ctx.Done():
			l.Trace("finished reading kafka messages...")

			return errors.New("context closed")
		default:
			m, err := kr.ReadMessage(ctx)
			if err != nil {
				return err
			}
			l.WithFields(logrus.Fields{
				"Topic":         m.Topic,
				"Partition":     m.Partition,
				"HighWaterMark": m.HighWaterMark,
				"Offset":        m.Offset,
				"Key":           string(m.Key),
				"Value":         string(m.Value),
			}).Trace("message received") // TODO trace?

			messageC <- m
		}
	}
}

func (bh *BrokerHandler) Close() error {
	for topic, kr := range bh.krs {
		bh.logger.WithField("topic", topic).Debug("closing kafka reader")
		kr.Close()
	}

	return nil
}
