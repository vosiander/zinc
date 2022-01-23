package libp2p

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	mrand "math/rand"
	"sync"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/libp2p/go-libp2p-core/network"
	discovery "github.com/libp2p/go-libp2p-discovery"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	disc "github.com/libp2p/go-libp2p-discovery"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/multiformats/go-multiaddr"
	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
)

type (
	Plugin struct {
		logger *logrus.Entry
		conf   Config
		node   host.Host
		dht    *disc.RoutingDiscovery
		ctx    context.Context
		r      *echo.Echo
	}

	Config struct {
		Enable        bool   `env:"LIBP2P_ENABLE" default:"false" yaml:"enable"`
		ListenAddress string `env:"LIBP2P_LISTEN_ADDRESS" default:"0.0.0.0" yaml:"listenAddress"`
		Port          string `env:"LIBP2P_PORT" default:"9999" yaml:"port"`
		Seed          int64  `env:"LIBP2P_SEED" default:"0" yaml:"seed"`
		Peer          string `env:"LIBP2P_PEER" default:"" yaml:"peer"`
		Rendezvous    string `env:"LIBP2P_RENDEZVOUS" default:"libp2prendezvous" yaml:"rendezvous"`
	}
)

const Name = "libp2p"

func New() *Plugin {
	return &Plugin{}
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	for _, d := range dependencies {
		if dp, isOk := d.(*logrus.Entry); isOk {
			p.logger = dp.WithField("component", "libp2p")
		}
		if dp, isOk := d.(*echo.Echo); isOk {
			p.r = dp
		}
	}
	if p.logger == nil {
		p.logger = logrus.WithField("component", "libp2p")
	}

	l := p.logger
	p.conf = conf.(Config)

	if !p.conf.Enable {
		l.Debug("libp2p is not enabled. nothing to init...")
		return p
	}

	if p.r == nil {
		l.Fatal("rest dependency was not given. failing!")
	}

	var r io.Reader
	if p.conf.Seed == 0 {
		r = rand.Reader
	} else {
		r = mrand.New(mrand.NewSource(p.conf.Seed))
	}

	p.ctx = context.Background()

	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
	if err != nil {
		l.WithError(err).Fatal("Could not create rsa key for libp2p. Failing!")
	}

	addr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s", p.conf.ListenAddress, p.conf.Port))
	if err != nil {
		l.WithError(err).Fatal("Could not create libp2p multiaddress. Failing!")
	}

	node, err := libp2p.New(
		libp2p.ListenAddrs(addr),
		libp2p.Identity(priv),
	)
	if err != nil {
		l.WithError(err).Fatal("Could not start libp2p. Failing!")
	}
	p.node = node

	maddr := []multiaddr.Multiaddr{}
	if p.conf.Peer != "" {
		a, err := multiaddr.NewMultiaddr(p.conf.Peer)
		if err != nil {
			l.WithError(err).Fatal("Could not start libp2p. Peer could not be converted to multiaddr. Failing!")
		}
		maddr = append(maddr, a)
	}

	kdht, err := p.createDHT(p.ctx, p.node, maddr)
	if err != nil {
		l.WithError(err).Fatal("cannot create DHT for libp2p. Failing!")
	}
	p.dht = kdht

	go p.discover(p.ctx, node, kdht)

	l.WithFields(logrus.Fields{"node-id": node.ID(), "address": node.Addrs()}).Debug("node started")
	for _, ad := range node.Addrs() {
		l.WithField("addr", fmt.Sprintf("%s/p2p/%s", ad.String(), p.node.ID().Pretty())).Info("node address")
	}

	p.r.GET("/p2p/peers", p.peers)

	return p
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Close() error {
	if !p.conf.Enable {
		return nil
	}
	return p.node.Close()
}

func (p *Plugin) Node() host.Host {
	if !p.conf.Enable {
		p.logger.Fatal("cannot use libp2p. plugin is not enabled")
	}
	return p.node
}

func (p *Plugin) createDHT(ctx context.Context, host host.Host, bootstrapPeers []multiaddr.Multiaddr) (*disc.RoutingDiscovery, error) {
	l := p.logger.WithField("component", "dht")
	var options []dht.Option

	if len(bootstrapPeers) == 0 {
		l.Debug("no peers given. starting as bootstrap server")
		options = append(options, dht.Mode(dht.ModeServer))
	}

	kdht, err := dht.New(ctx, host, options...)
	if err != nil {
		l.WithError(err).Warn("error creating DHT discovery")
		return nil, err
	}

	if err = kdht.Bootstrap(ctx); err != nil {
		l.WithError(err).Warn("error bootstrapping DHT discovery")
		return nil, err
	}

	wg := &sync.WaitGroup{}
	for _, peerAddr := range bootstrapPeers {
		peerinfo, err := peer.AddrInfoFromP2pAddr(peerAddr)
		if err != nil {
			l.WithError(err).Warn("could not get address from p2p bootstrap peer")
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := host.Connect(ctx, *peerinfo); err != nil {
				l.WithError(err).Warnf("Error while connecting to node %q: %-v", peerinfo, err)
				return
			}
			l.WithError(err).Debugf("Connection established with bootstrap bootstrap node: %q", *peerinfo)
		}()
	}
	wg.Wait()

	return disc.NewRoutingDiscovery(kdht), nil
}

func (p *Plugin) discover(ctx context.Context, h host.Host, dht *disc.RoutingDiscovery) {
	l := p.logger.WithField("component", "discover-peers")
	discovery.Advertise(ctx, dht, p.conf.Rendezvous)

	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	l.Debug("peer discovery started")

	for {
		select {
		case <-ctx.Done():
			l.Debug("context done. killing discovery")
			return
		case <-ticker.C:
			peers, err := discovery.FindPeers(ctx, dht, p.conf.Rendezvous)
			if err != nil {
				l.WithError(err).Fatal("failing because finding peers encountered an error")
			}

			for _, peerHost := range peers {
				if peerHost.ID == h.ID() {
					continue
				}
				if h.Network().Connectedness(peerHost.ID) != network.Connected {
					if _, err = h.Network().DialPeer(ctx, peerHost.ID); err != nil {
						continue
					}
					l.WithField("peerHost", peerHost.ID.Pretty()).Trace("connected to peer")
				}
			}
		}
	}
}

type (
	PeerInfo struct {
		Address string `yaml:"address" json:"address"`
	}
)

func (p *Plugin) peers(c echo.Context) error {
	ps := p.node.Peerstore()

	list := map[string]PeerInfo{}
	for _, pr := range ps.Peers() {
		addr := ps.PeerInfo(pr)

		for _, ip := range addr.Addrs {
			list[pr.Pretty()] = PeerInfo{
				Address: fmt.Sprintf("%s/p2p/%s", ip.String(), pr.Pretty()),
			}
		}
	}

	return c.JSON(200, list)
}
