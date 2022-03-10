package prometheus

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
)

type (
	Plugin struct {
		logger      *logrus.Entry
		conf        Config
		incCounters map[string]prometheus.Counter
		incGauges   map[string]prometheus.Gauge
	}

	Config struct {
		Enable bool   `env:"PROMETHEUS_ENABLE" default:"false" yaml:"enable"`
		Port   string `env:"PROMETHEUS_PORT" default:"2112" yaml:"port"`
	}
)

const Name = "prometheus"

func New() *Plugin {
	return &Plugin{
		incCounters: map[string]prometheus.Counter{},
	}
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	for _, d := range dependencies {
		if dp, isOk := d.(*logrus.Entry); isOk {
			p.logger = dp.WithField("component", "prometheus-plugin")
		}
	}
	if p.logger == nil {
		p.logger = logrus.WithField("component", "prometheus-plugin")
	}
	p.conf = conf.(Config)

	if !p.conf.Enable {
		return p
	}

	p.incGauges = map[string]prometheus.Gauge{}
	p.incCounters = map[string]prometheus.Counter{}

	return p
}

func (p *Plugin) Close() error {
	if !p.conf.Enable {
		return nil
	}

	return nil
}

func (p *Plugin) Start() error {
	if !p.conf.Enable {
		return nil
	}

	p.logger.Debug("starting up prometheus server")
	http.Handle("/metrics", promhttp.Handler())

	return http.ListenAndServe(":"+p.conf.Port, nil)
}

func (p *Plugin) AddCounter(name string, count float64, help string) {
	if !p.conf.Enable {
		return
	}

	l := p.logger
	if name == "" {
		l.Warn("incCounter metric with empty name. skipping. ")
		return
	}

	if _, isOK := p.incCounters[name]; !isOK {
		inc := promauto.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
		l.WithField("name", name).Debug("init metrics counter")

		p.incCounters[name] = inc
	}
	p.incCounters[name].Add(count)
}

func (p *Plugin) AddGauges(name string, gauge float64, help string) {
	if !p.conf.Enable {
		return
	}

	l := p.logger
	if name == "" {
		l.Warn("incGauge metric with empty name. skipping. ")
		return
	}

	if _, isOK := p.incGauges[name]; !isOK {
		inc := promauto.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		l.WithField("name", name).Debug("init metrics counter")

		p.incGauges[name] = inc
	}
	p.incGauges[name].Set(gauge)
}
