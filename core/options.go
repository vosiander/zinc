package core

type (
	Option func(c *Core, conf interface{}) error
)

func LoadYamlConfig(filename string) Option {
	return func(c *Core, conf interface{}) error {
		l := c.Logger().WithField("component", "LoadFromConfig")

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
