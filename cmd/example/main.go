package main

import (
	"fmt"
	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/siklol/zinc/core"
	"github.com/siklol/zinc/plugins/boltdb"
	kafkaPlugin "github.com/siklol/zinc/plugins/kafka"
	postgres_crud "github.com/siklol/zinc/plugins/postgres-crud"
	"github.com/siklol/zinc/plugins/telegram"
	"github.com/siklol/zinc/plugins/usermanager"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
	tb "gopkg.in/telebot.v3"
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

	l := c.Logger()
	b := c.MustGet(telegram.Name).(*telegram.Plugin).Bot()
	um := c.MustGet(usermanager.Name).(*usermanager.Plugin)
	b.Handle("/fe", func(c tb.Context) error {
		m := c.Message()
		um.RegisterUser(
			strconv.FormatInt(m.Sender.ID, 10),
			m.Sender.FirstName,
			m.Sender.LastName,
			m.Sender.Username,
		)
		b.Send(m.Sender, "events followed")
		return nil
	})
	b.Handle("/ue", func(c tb.Context) error {
		m := c.Message()
		if err := um.Delete(strconv.FormatInt(m.Sender.ID, 10)); err != nil {
			l.WithError(err).Warn("error deleting telegram user from db")
		}
		b.Send(m.Sender, "events unfollowed")
		return nil
	})

	c.CLI(func() {
		go writeKafka(c)
		go readKafka(c)
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
	b := c.MustGet(telegram.Name).(*telegram.Plugin).Bot()
	um := c.MustGet(usermanager.Name).(*usermanager.Plugin)
	pcrud := c.MustGet(postgres_crud.Name).(*postgres_crud.Plugin)

	ch := make(chan kafka.Message, 1024)
	tableName := "abcdefgh"

	tx, err := pcrud.CreateTable(tableName)
	if err != nil {
		l.WithError(err).Fatal("could not create table")
	}

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

			ma := map[string]interface{}{
				"Topic":     m.Topic,
				"Partition": m.Partition,
				"Offset":    m.Offset,
				"Key":       string(m.Key),
				"Value":     string(m.Value),
			}

			if err := tx.Upsert(string(m.Key), ma); err != nil {
				l.WithError(err).Warn("error persisting crud data to db")
			}

			var mb map[string]interface{}
			if err := tx.Find(string(m.Key), &mb); err != nil {
				l.WithError(err).Warn("error getting crud data from db")
			}
			l.Debugf("%#v", mb)

			if err := tx.Delete(string(m.Key)); err != nil {
				l.WithError(err).Warn("error deleting crud data from db")
			}

			telegramUsers, err := um.FindAll()
			if err != nil {
				l.WithError(err).Warn("could not get all telegram users from the db")
				continue
			}

			for _, u := range telegramUsers {
				b.Send(&Receipt{ID: u.ID}, fmt.Sprintf("message: %s %s", string(m.Key), string(m.Value)))
			}
		}
	}
}

func (r *Receipt) Recipient() string {
	return r.ID
}
