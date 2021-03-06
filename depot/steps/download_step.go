package steps

import (
	"fmt"
	"io"
	"net/url"

	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/cacheddownloader"
	"github.com/cloudfoundry-incubator/executor/depot/log_streamer"
	"github.com/cloudfoundry-incubator/garden"
	"github.com/pivotal-golang/bytefmt"
	"github.com/pivotal-golang/lager"
)

type downloadStep struct {
	container        garden.Container
	model            models.DownloadAction
	cachedDownloader cacheddownloader.CachedDownloader
	streamer         log_streamer.LogStreamer
	rateLimiter      chan struct{}

	logger lager.Logger

	*canceller
}

func NewDownload(
	container garden.Container,
	model models.DownloadAction,
	cachedDownloader cacheddownloader.CachedDownloader,
	rateLimiter chan struct{},
	streamer log_streamer.LogStreamer,
	logger lager.Logger,
) *downloadStep {
	logger = logger.Session("download-step", lager.Data{
		"to":       model.To,
		"cacheKey": model.CacheKey,
		"user":     model.User,
	})

	return &downloadStep{
		container:        container,
		model:            model,
		cachedDownloader: cachedDownloader,
		streamer:         streamer,
		rateLimiter:      rateLimiter,
		logger:           logger,

		canceller: newCanceller(),
	}
}

func (step *downloadStep) Perform() error {
	select {
	case step.rateLimiter <- struct{}{}:
	case <-step.Cancelled():
		return ErrCancelled
	}
	defer func() {
		<-step.rateLimiter
	}()

	err := step.perform()
	if err != nil {
		select {
		case <-step.Cancelled():
			return ErrCancelled
		default:
			return err
		}
	}

	return nil
}

func (step *downloadStep) perform() error {
	step.emit("Downloading %s...\n", step.model.Artifact)

	downloadedFile, downloadedSize, err := step.fetch()
	if err != nil {
		return NewEmittableError(err, "Downloading failed")
	}

	err = step.streamIn(step.model.To, downloadedFile)
	if err != nil {
		return NewEmittableError(err, "Copying into the container failed")
	}

	if downloadedSize != 0 {
		step.emit("Downloaded %s (%s)\n", step.model.Artifact, bytefmt.ByteSize(uint64(downloadedSize)))
	} else {
		step.emit("Downloaded %s\n", step.model.Artifact)
	}

	return nil
}

func (step *downloadStep) fetch() (io.ReadCloser, int64, error) {
	step.logger.Info("fetch-starting")
	url, err := url.ParseRequestURI(step.model.From)
	if err != nil {
		step.logger.Error("parse-request-uri-error", err)
		return nil, 0, err
	}

	tarStream, downlodedSize, err := step.cachedDownloader.Fetch(url, step.model.CacheKey, cacheddownloader.TarTransform, step.Cancelled())
	if err != nil {
		step.logger.Error("fetch-failed", err)
		return nil, 0, err
	}

	step.logger.Info("fetch-complete")
	return tarStream, downlodedSize, nil
}

func (step *downloadStep) streamIn(destination string, reader io.ReadCloser) error {
	step.logger.Info("stream-in-starting")

	// StreamIn will close the reader
	err := step.container.StreamIn(garden.StreamInSpec{Path: destination, TarStream: reader, User: step.model.User})
	if err != nil {
		step.logger.Error("stream-in-failed", err, lager.Data{
			"destination": destination,
		})
		return err
	}

	step.logger.Info("stream-in-complete")
	return nil
}

func (step *downloadStep) emit(format string, a ...interface{}) {
	if step.model.Artifact != "" {
		fmt.Fprintf(step.streamer.Stdout(), format, a...)
	}
}
