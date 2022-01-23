package githelper

import (
	"errors"
	"os/exec"
	"strings"

	"github.com/Masterminds/semver/v3"

	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const (
	Name        = "githelper"
	None Semver = iota
	Major
	Minor
	Patch
)

type (
	Semver int
	Plugin struct {
		Logger     *log.Entry
		conf       Config
		gitVersion string
	}

	Config struct {
		Enable bool `env:"GITHELPER_ENABLE" default:"false" yaml:"enable" json:"enable"`
	}
)

func New() *Plugin {
	return &Plugin{}
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) Name() string {
	return Name
}

func (p *Plugin) Boot(conf interface{}, dependencies ...interface{}) plugins.Plugin {
	for _, d := range dependencies {
		if dp, isOk := d.(*logrus.Entry); isOk {
			p.Logger = dp
		}
	}
	if p.Logger == nil {
		p.Logger = logrus.WithField("component", "githelper")
	}
	l := p.Logger.WithField("component", "githelper")
	p.conf = conf.(Config)

	if !p.conf.Enable {
		l.Debug("githelper is not enabled. nothing to init...")
		return p
	}

	l.Debug("checking for git version...")

	if _, err := exec.LookPath("git"); err != nil {
		log.WithError(err).Fatal("git not installed")
	}
	out, err := exec.Command("git", "version").Output()
	if err != nil {
		log.WithError(err).Fatal("git version failed")
	}
	parts := strings.Split(strings.Trim(string(out), "\n"), " ")

	if len(parts) < 3 {
		log.WithField("output", parts).Fatal("git version unknown. failing")
	}

	p.gitVersion = parts[2]
	l.WithField("version", p.gitVersion).Debug("git version")

	return p
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Close() error {
	return nil
}

func (p *Plugin) TagVersion() (*semver.Version, error) {
	out, err := exec.Command("git", "describe", "--abbrev=0", "--tags").Output()
	// git describe --abbrev=0 --tags
	if err != nil {
		log.WithError(err).Error("could not load git tag version")
		return nil, err
	}

	v := strings.ReplaceAll(strings.Trim(string(out), "\n"), "v", "")

	return semver.NewVersion(v)

}

func (p *Plugin) SemVer(msg string) Semver {
	switch true {
	case strings.Contains(msg, "[patch]"):
		return Patch
	case strings.Contains(msg, "[major]"):
		return Major
	case strings.Contains(msg, "[minor]"):
		return Minor
	}

	return None
}

func (p *Plugin) GitCommitMessage() string {
	out, err := exec.Command("bash", "-c", "bash -c \"git log -1 --oneline --format=%s | sed 's/^.*: //'\"").Output()
	if err != nil {
		log.WithError(err).Error("could not load git tag version")
		return ""
	}

	return strings.ReplaceAll(strings.Trim(string(out), "\n"), "v", "")
}

func (p *Plugin) GitTag(annotation string, msg string) error {
	out, err := exec.Command("git", "tag", "-m", msg, "-a", annotation).Output()
	if err != nil {
		log.WithError(err).Error("could not tag")
		return err
	}
	log.WithField("git output", string(out)).Trace("git tag output")

	return nil
}

func (p *Plugin) NextVersion(v string, next Semver) (*semver.Version, error) {
	sv, err := semver.NewVersion(v)

	if err != nil {
		return nil, err
	}

	switch next {
	case None:
		return sv, nil
	case Patch:
		nv := sv.IncPatch()
		return &nv, nil
	case Minor:
		nv := sv.IncMinor()
		return &nv, nil
	case Major:
		nv := sv.IncMajor()
		return &nv, nil
	}
	return nil, errors.New("unknown next version")
}
