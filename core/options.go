package core

import (
	"github.com/caarlos0/env/v6"
	"github.com/creasty/defaults"
)

type (
	Option   func(c *Core, conf interface{}) error
	UpdateFn func(cfg string) error
)

func LoadEnv() Option {
	return func(c *Core, conf interface{}) error {
		l := c.Logger().WithField("component", "LoadEnv")

		if err := defaults.Set(conf); err != nil {
			l.WithError(err).Fatal("Could not load defaults")
		}

		if err := env.Parse(conf); err != nil {
			l.WithError(err).Fatal("Could not load environment variables")
		}

		l.Trace("finished loading configs from env")
		return nil
	}
}

func LoadConfigFromEnvironment(yamlFile string, service string, url string) Option {
	return func(c *Core, conf interface{}) error {
		l := c.Logger().WithFields(map[string]any{
			"component": "LoadConfigFromEnvironment",
			"yaml":      yamlFile,
			"service":   service,
			"url":       url,
		})

		if yamlFile != "" {
			err := c.yl.LoadYamlConfig(yamlFile, conf)
			if err == nil {
				l.Tracef("%#v", conf)
				return nil
			}
			l.WithError(err).Warn("error loading yaml config")
		}

		if service != "" && url != "" {
			err := c.cl.LoadConfig(url, service, conf)
			if err == nil {
				l.Tracef("%#v", conf)
				return nil
			}
			l.WithError(err).Warn("error loading yaml config")
		}

		l.Fatal("could not load configs from either file or configurator")
		return nil
	}
}

func LoadOptionalYamlConfig(filename string) Option {
	return func(c *Core, conf interface{}) error {
		l := c.Logger().WithField("component", "LoadFromYamlConfig")

		if err := c.yl.LoadYamlConfig(filename, conf); err != nil {
			l.WithError(err).Warn("error loading yaml config. Ignoring...")
		}
		l.Tracef("%#v", conf)
		return nil
	}
}

func LoadYamlConfig(filename string) Option {
	return func(c *Core, conf interface{}) error {
		l := c.Logger().WithField("component", "LoadFromYamlConfig")

		if err := c.yl.LoadYamlConfig(filename, conf); err != nil {
			l.WithError(err).Fatal("error loading yaml config")
		}
		l.Tracef("%#v", conf)
		return nil
	}
}

func LoadConfigurator(service string, url string) Option {
	return func(c *Core, conf interface{}) error {
		l := c.Logger().WithField("component", "LoadConfigurator")

		if url == "" {
			l.Debug("url is empty. using https://config.example.com as default")
			url = "https://config.example.com"
		}

		if err := c.cl.LoadConfig(url, service, conf); err != nil {
			l.WithError(err).Fatal("error loading configurator config")
		}
		l.Tracef("%#v", conf)
		return nil
	}
}

func LoadKafkaConfigurator(brokers string, topic string, updateFn UpdateFn) Option {
	return func(c *Core, conf interface{}) error {
		l := c.Logger().WithField("component", "LoadConfigurator")

		if err := defaults.Set(conf); err != nil {
			panic(err)
		}

		if err := c.kcl.LoadConfig(brokers, topic, updateFn); err != nil {
			l.WithError(err).Fatal("error loading configurator config")
		}
		l.Tracef("%#v", conf)
		return nil
	}
}

func CLIShutdownFunc(f func()) Option {
	return func(c *Core, conf interface{}) error {
		c.cliShutdownFunc = f
		return nil
	}
}
