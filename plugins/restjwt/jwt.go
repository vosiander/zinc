package restjwt

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"github.com/golang-jwt/jwt"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	"strings"
)

type (
	Plugin struct {
		logger *logrus.Entry
		conf   Config
		j      *Handler
	}

	Config struct {
		Enable     bool   `env:"REST_ENABLE" default:"false" yaml:"enable" json:"enable"`
		ServerCert string `env:"SERVER_CERT" yaml:"serverCert" json:"serverCert"`
	}

	Handler struct {
		serverCertificate string
		l                 *logrus.Entry
		key               *rsa.PublicKey
	}

	Info struct {
		ID                string
		Email             string
		EmailVerified     bool
		FamilyName        string
		GivenName         string
		Name              string
		PreferredUsername string
	}
)

var (
	ErrStatusUnauthorized       = errors.New("status unauthorized")
	ErrInvalidServerCertificate = errors.New("invalid server certificate")
	ErrUnexpectedSigningMethod  = errors.New("unexpected signing method")
	ErrInvalidToken             = errors.New("invalid token")
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
			p.logger = dp.WithField("component", "rest-jwt")
		}
	}
	if p.logger == nil {
		p.logger = logrus.WithField("component", "rest-jwt")
	}
	p.conf = conf.(Config)

	if !p.conf.Enable {
		return p
	}

	j, err := NewJwtHandler(p.logger, "")
	if err != nil {
		p.logger.WithError(err).Fatal("error init jwt handler")
	}
	p.j = j

	return p
}

func (p *Plugin) Close() error {
	if !p.conf.Enable {
		return nil
	}
	return nil
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Start() error {
	if !p.conf.Enable {
		return nil
	}

	return nil
}

func NewJwtHandler(l *logrus.Entry, cert string) (*Handler, error) {
	j := &Handler{
		serverCertificate: cert,
		l:                 l,
	}

	cert = fmt.Sprintf("-----BEGIN CERTIFICATE-----\n%s\n-----END CERTIFICATE-----", j.serverCertificate)
	key, err := jwt.ParseRSAPublicKeyFromPEM([]byte(cert))
	if err != nil {
		j.l.WithError(err).Warn("error parsing rsa pub key")

		return nil, ErrInvalidServerCertificate
	}

	j.key = key

	return j, nil
}

func (j *Handler) Token(reqToken string) (*jwt.Token, error) {
	reqToken = strings.ReplaceAll(reqToken, "Bearer ", "")
	token, err := jwt.Parse(reqToken, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			j.l.WithField("alg", token.Header["alg"]).Error("Unexpected signing method")

			return nil, ErrUnexpectedSigningMethod
		}

		return j.key, nil
	})

	if err != nil {
		j.l.WithError(err).Warn("error parsing token")

		return nil, ErrStatusUnauthorized
	}

	if _, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		j.l.Info("token is valid")

		return token, nil
	}
	j.l.WithField("token", token).Warn("invalid token provided")

	return nil, ErrInvalidToken
}

func (j *Handler) UserInfo(token *jwt.Token) Info {
	claims := token.Claims.(jwt.MapClaims)

	info := Info{}
	if d, isOk := claims["sub"]; isOk {
		info.ID = d.(string)
	}
	if d, isOk := claims["email"]; isOk {
		info.Email = d.(string)
	}
	if d, isOk := claims["email_verified"]; isOk {
		info.EmailVerified = d.(bool)
	}
	if d, isOk := claims["family_name"]; isOk {
		info.FamilyName = d.(string)
	}
	if d, isOk := claims["given_name"]; isOk {
		info.GivenName = d.(string)
	}
	if d, isOk := claims["name"]; isOk {
		info.Name = d.(string)
	}
	if d, isOk := claims["preferred_username"]; isOk {
		info.PreferredUsername = d.(string)
	}

	return info
}
