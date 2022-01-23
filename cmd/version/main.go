package main

import (
	"fmt"

	"github.com/siklol/zinc/core"
	"github.com/siklol/zinc/plugins/githelper"
	"github.com/sirupsen/logrus"
)

type (
	CLIOptions struct {
		Force bool `short:"f" long:"force" description:"force triggers git tag"`
	}
)

func main() {
	cliOpts := CLIOptions{}
	c := core.NewMinimalCore(&cliOpts)
	l := c.Logger()
	gitH := githelper.New().Boot(githelper.Config{Enable: true}, l).(*githelper.Plugin)

	c.CLI(func() {
		v, err := gitH.TagVersion()
		if err != nil {
			l.WithError(err).Fatal("could not parse tag version. does it exist?")
		}
		l.WithField("version", v.String()).Info("Project tag version found")

		nextV := gitH.SemVer(gitH.GitCommitMessage())
		if nextV == githelper.None {
			l.Info("no semver upgrade in commit message. nothing to do")
			return
		}

		tagVersion, err := gitH.NextVersion(v.String(), nextV)
		if err != nil {
			l.WithError(err).Fatal("could not upgrade to new version")
		}

		if !cliOpts.Force {
			l.WithFields(logrus.Fields{"old": v.String(), "new": tagVersion.String()}).Info("dry-run finished")
			return
		}

		l.Warn("triggering git tag now...")

		av := fmt.Sprintf("v%s", tagVersion.String())
		if err := gitH.GitTag(av, av); err != nil {
			l.WithError(err).WithField("v", av).Fatal("could not execute git tag")
		}

		l.Infof("version %s tagged", av)
	})
}
