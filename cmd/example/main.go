package main

import (
	"fmt"
	"time"

	"github.com/siklol/zinc/core"
	"github.com/siklol/zinc/plugins/boltdb"
	kafkaPlugin "github.com/siklol/zinc/plugins/kafka"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

type (
	Receipt struct {
		ID string
	}

	Config struct {
		Test int                  `yaml:"test" json:"test"`
		Core core.AllPluginConfig `yaml:"core" json:"core"`
	}

	CLIOptions struct {
		ConfigFile      string `short:"c" long:"config" description:"config file" default:"config.yaml"`
		ConfiguratorURL string `short:"u" long:"configurator" description:"Configurator URL" default:"https://config.example.com"`
	}
)

var (
	conf    Config
	cliOpts CLIOptions
)

func main() {
	c := core.NewCore(&conf, &cliOpts).
		WithOptions(&conf, core.LoadYamlConfig(cliOpts.ConfigFile)).
		//WithOptions(&conf, core.LoadConfigurator("de.siklol-zinc.example-go-plugins", cliOpts.ConfiguratorURL)).
		WithAllPlugins(conf.Core)

	c.CLI(func() {
		go writeKafka(c)
		go readKafka(c)
		time.Sleep(300 * time.Second)
	})
}

func writeKafka(c *core.Core) {
	l := c.Logger()
	k := c.MustGet(kafkaPlugin.Name).(*kafkaPlugin.Plugin)
	bDB := c.MustGet(boltdb.Name).(*boltdb.Plugin)

	topic := "events"
	ticker := time.NewTicker(10 * time.Second)
	for t := range ticker.C {
		id := "test-ticker-" + t.Format(time.RFC3339Nano)
		if err := k.WriteToTopic(topic, id, ""); err != nil {
			l.WithError(err).WithField("topic", topic).Warn("error writing to kafka")
		}

		db := bDB.DB()
		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte("DB"))
			if err != nil {
				return fmt.Errorf("could not create root bucket: %v", err)
			}
			return nil
		})
		if err != nil {
			l.WithError(err).Warn("error creating bucket db")
		}

		err = db.Update(func(tx *bolt.Tx) error {
			err = tx.Bucket([]byte("DB")).Put([]byte("ID"), []byte(id))
			if err != nil {
				return fmt.Errorf("could not set config: %v", err)
			}
			return nil
		})
		if err != nil {
			l.WithError(err).Warn("error updating bucket db")
		}

		err = db.View(func(tx *bolt.Tx) error {
			conf := tx.Bucket([]byte("DB")).Get([]byte("ID"))
			l.Warnf("id: %s\n", string(conf))
			return nil
		})
		if err != nil {
			l.WithError(err).Warn("error reading bucket db")
		}
	}
}

func readKafka(c *core.Core) {
	l := c.Logger()
	k := c.MustGet(kafkaPlugin.Name).(*kafkaPlugin.Plugin)

	ch := make(chan kafkaPlugin.Message, 1024)

	l.Debug("readKafka starting")
	go k.ReadFromTopic("events", "go-plugin-example-"+time.Now().Format("DMY-his"), ch)

	for {
		select {
		case m := <-ch:
			l.WithFields(logrus.Fields{
				"Topic":     m.Topic,
				"Partition": m.Partition,
				"Offset":    m.Offset,
				"Key":       string(m.Key),
				"Value":     string(m.Value),
			}).Debug("message received from kafka")
		}
	}
}

func (r *Receipt) Recipient() string {
	return r.ID
}
