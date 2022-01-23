package s3file

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/siklol/zinc/plugins"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const Name = "s3file"

type (
	Plugin struct {
		logger *log.Entry
		mc     *minio.Client
		conf   Config
	}

	Config struct {
		Enable           bool          `env:"S3_FILE_ENABLE" default:"false" yaml:"enable"`
		AutoCreateBucket bool          `env:"S3_FILE_AUTO_CREATE_BUCKET" default:"true" yaml:"autoCreateBucket"`
		URL              string        `env:"S3_FILE_URL" yaml:"url"`
		AccessKeyID      string        `env:"S3_FILE_ACCESS_KEY_ID" yaml:"accessKeyId"`
		AccessKey        string        `env:"S3_FILE_ACCESS_KEY" yaml:"accessKey"`
		Timeout          time.Duration `env:"S3_FILE_TIMEOUT" default:"30s" yaml:"timeout"`
		UseSSL           bool          `env:"S3_FILE_USE_SSL" default:"true" yaml:"useSsl"`
	}
)

var (
	policy = `{"Version": "2012-10-17","Statement": [{"Action": ["s3:GetObject"],"Effect": "Allow","Principal": {"AWS": ["*"]},"Resource": ["arn:aws:s3:::%s/*"],"Sid": ""}]}`
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
	p.conf = conf.(Config)

	for _, d := range dependencies {
		switch dp := d.(type) {
		case *logrus.Entry:
			p.logger = dp.WithField("component", "s3file-plugin")
		}
	}
	l := p.logger

	if !p.conf.Enable {
		l.Debug("s3file is not enabled. nothing to init...")
		return p
	}

	minioClient, err := minio.New(p.conf.URL, &minio.Options{
		Creds:  credentials.NewStaticV4(p.conf.AccessKeyID, p.conf.AccessKey, ""),
		Secure: p.conf.UseSSL,
	})
	if err != nil {
		l.WithError(err).Fatal("minio client creation failed")
	}
	p.mc = minioClient

	l.Debug("booting up...")

	return p
}

func (p *Plugin) IsEnabled() bool {
	return p.conf.Enable
}

func (p *Plugin) Close() error {
	return nil
}

func (p *Plugin) Put(bucketName string, fileName string, data []byte) (string, error) {
	l := p.logger.WithFields(logrus.Fields{
		"action":     "upload",
		"bucketname": bucketName,
		"file":       fileName,
	})

	if !p.conf.Enable {
		l.Warn("s3 fileupload is not enabled. nothing to upload...")
		return "", errors.New("s3 file plugin is not enabled")
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), p.conf.Timeout)
	defer cancelFunc()

	if p.conf.AutoCreateBucket {
		l.Debug("trying to auto create bucket")
		exists, err := p.mc.BucketExists(ctx, bucketName)
		if err != nil {
			l.Error("s3 creating bucket failed")
			return "", err
		}

		if !exists {
			l.Debug("Creating bucket")
			if err := p.mc.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
				l.Error("s3 creating bucket failed")
				return "", err
			}

			if err := p.mc.SetBucketPolicy(ctx, bucketName, fmt.Sprintf(policy, bucketName)); err != nil {
				l.Error("s3 creating bucket failed")
				return "", err
			}
		}
	}

	uploadInfo, err := p.mc.PutObject(
		ctx,
		bucketName,
		fileName,
		bytes.NewReader(data),
		int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	if err != nil {
		l.WithError(err).Warn("minio upload failed")
		return "", err
	}

	l.WithField("etag", uploadInfo.ETag).Debug("Successfully uploaded file")

	return fmt.Sprintf("%s/%s/%s", p.mc.EndpointURL(), bucketName, fileName), nil
}
