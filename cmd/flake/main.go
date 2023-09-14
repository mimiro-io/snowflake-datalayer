package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mimiro-io/datahub-snowflake-datalayer/internal"
)

func main() {
	var cfg *internal.Config = &internal.Config{}
	if err := cfg.ServerFlags().Parse(os.Args); err != nil {
		panic(err)
	}
	if err := cfg.LoadEnv(); err != nil {
		panic(err)
	}

	internal.LoadLogger(cfg.LogType, cfg.ServiceName, cfg.LogLevel)
	internal.LOG.Trace().Any("With config", cfg).Msg("Configuration")

	if err := cfg.Validate(); err != nil {
		internal.LOG.Panic().Err(err).Msg("")
		panic(err)
	}

	configLoader := internal.StartConfigLoader(cfg)

	s, err := internal.NewServer(cfg)
	if err != nil {
		internal.LOG.Error().Err(err).Msg(err.Error())
		panic(err)
	}
	// Start server
	go func() {
		if err := s.E.Start(":" + strconv.Itoa(cfg.Port)); err != nil && !errors.Is(err, http.ErrServerClosed) {
			internal.LOG.Fatal().Err(err).Msg("unexpected termination")
			panic(err)
		}
	}()

	// Wait for interrupt signal to gracefully shut down the server with a timeout of 10 seconds.
	// Use a buffered channel to avoid missing signals as recommended for signal.Notify
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	configLoader.Stop()
	if err := s.E.Shutdown(ctx); err != nil {
		internal.LOG.Fatal().Err(err).Msg(err.Error())
	}

}