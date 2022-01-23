package rest

import (
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	echomiddleware "github.com/labstack/echo/v4/middleware"
	"github.com/neko-neko/echo-logrus/v2/log"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
)

type (
	Plugin struct {
		logger *logrus.Entry
		conf   Config
		e      *echo.Echo
	}

	Config struct {
		Enable bool   `env:"REST_ENABLE" default:"false" yaml:"enable"`
		Port   string `env:"PORT" default:"8099" yaml:"port"`
	}
)

const Name = "rest"

func New() *Plugin {
	return &Plugin{}
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	for _, d := range dependencies {
		if dp, isOk := d.(*logrus.Entry); isOk {
			p.logger = dp.WithField("component", "rest")
		}
	}
	if p.logger == nil {
		p.logger = logrus.WithField("component", "rest")
	}
	l := p.logger
	p.conf = conf.(Config)

	if !p.conf.Enable {
		return p
	}

	e := echo.New()
	e.Logger = &log.MyLogger{Logger: l.WithField("component", "rest-plugin").Logger}
	e.HideBanner = true
	e.HidePort = true
	e.Use(echoMiddlewareLogger(l))
	e.Use(echomiddleware.CORS())

	e.GET("/version", p.version)

	p.e = e

	return p
}

func (p *Plugin) Close() error {
	if !p.conf.Enable {
		return nil
	}
	return p.e.Close()
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Start() error {
	if !p.conf.Enable {
		return nil
	}

	p.logger.WithField("port", p.conf.Port).Debug("rest webserver started...")
	p.e.Logger.Error(p.e.Start(":" + p.conf.Port))
	return nil
}

func (p *Plugin) Router() *echo.Echo {
	return p.e
}

func (p *Plugin) version(c echo.Context) error {
	return c.JSON(200, map[string]string{"version": "v1"})
}

func echoMiddlewareLogger(l logrus.FieldLogger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			req := c.Request()
			res := c.Response()
			start := time.Now()

			var err error
			if err = next(c); err != nil {
				c.Error(err)
			}
			stop := time.Now()

			id := req.Header.Get(echo.HeaderXRequestID)
			if id == "" {
				id = res.Header().Get(echo.HeaderXRequestID)
			}
			reqSize := req.Header.Get(echo.HeaderContentLength)
			if reqSize == "" {
				reqSize = "0"
			}

			l.
				WithFields(logrus.Fields{
					"id":               id,
					"real-ip":          c.RealIP(),
					"time":             stop.Format(time.RFC3339),
					"host":             req.Host,
					"method":           req.Method,
					"request-uri":      req.RequestURI,
					"status":           res.Status,
					"request-size":     reqSize,
					"request-size-int": strconv.FormatInt(res.Size, 10),
					"duration":         stop.Sub(start).String(),
					"referer":          req.Referer(),
					"agent":            req.UserAgent(),
				}).
				Infof("http request")
			return err
		}
	}
}
