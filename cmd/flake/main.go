package main

import (
	"context"
	"encoding/base64"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/cristalhq/acmd"

	"github.com/mimiro-io/datahub-snowflake-datalayer/internal"
)

func main() {
	cmds := []acmd.Command{
		{
			Name:        "run",
			Description: "runs the layer as a command line app",

			ExecFunc: func(ctx context.Context, args []string) error {
				var cfg internal.Config
				if err := cfg.Flags().Parse(args); err != nil {
					return err
				}
				if err := cfg.LoadEnv(); err != nil {
					return err
				}
				internal.LoadLogger(cfg.LogType, cfg.ServiceName, cfg.LogLevel)
				internal.LOG.Debug().Any("With config", cfg).Msg("Configuration")

				// set it up
				sf, err := internal.NewSnowflake(cfg)
				if err != nil {
					internal.LOG.Error().Err(err).Msg("Failed to connect to snowflake")
					return err
				}
				ds := internal.NewDataset(cfg, sf)

				// need a file reader
				reader, err := os.Open(cfg.File)
				if err != nil {
					internal.LOG.Error().Err(err).Msg("Failed to open dataset file")
					return nil
				}

				ctx = context.WithValue(ctx, "batchSize", 10000) // this makes sure memory won't blow up on large files
				ctx = context.WithValue(ctx, "recorded", time.Now().UnixNano())
				err = ds.Write(ctx, "", reader)
				if err != nil {
					internal.LOG.Error().Err(err).Msg(err.Error())
				}
				return nil
			},
		},
		{
			Name:        "server",
			Description: "starts as a web server",
			ExecFunc: func(ctx context.Context, args []string) error {
				var cfg internal.Config
				if err := cfg.ServerFlags().Parse(args); err != nil {
					return err
				}
				if err := cfg.LoadEnv(); err != nil {
					return err
				}

				internal.LoadLogger(cfg.LogType, cfg.ServiceName, cfg.LogLevel)
				internal.LOG.Trace().Any("With config", cfg).Msg("Configuration")

				if err := cfg.Validate(); err != nil {
					internal.LOG.Panic().Err(err).Msg("")
					return nil
				}

				e, err := internal.NewServer(cfg)
				if err != nil {
					internal.LOG.Error().Err(err).Msg(err.Error())
					return nil
				}
				// Start server
				go func() {
					if err := e.Start(":" + strconv.Itoa(cfg.Port)); err != nil && err != http.ErrServerClosed {
						internal.LOG.Fatal().Err(err).Msg("shutting down the server")
					}
				}()

				// Wait for interrupt signal to gracefully shutdown the server with a timeout of 10 seconds.
				// Use a buffered channel to avoid missing signals as recommended for signal.Notify
				quit := make(chan os.Signal, 1)
				signal.Notify(quit, os.Interrupt)
				<-quit
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := e.Shutdown(ctx); err != nil {
					internal.LOG.Fatal().Err(err).Msg(err.Error())
				}

				return nil
			},
		}, {
			Name:        "encode",
			Description: "url encodes a pem cert byte slice",
			ExecFunc: func(ctx context.Context, args []string) error {
				var fileName string
				fs := flag.NewFlagSet("some name for help", flag.ContinueOnError)
				fs.StringVar(&fileName, "input", "", "input file to encode")
				if err := fs.Parse(args); err != nil {
					return err
				}

				b, err := os.ReadFile(fileName)
				if err != nil {
					return err
				}

				println(base64.StdEncoding.EncodeToString(b))

				return nil
			},
		},
	}
	r := acmd.RunnerOf(cmds, acmd.Config{
		AppName:        "flake",
		AppDescription: "Snowflake Datalayer for Mimiro DataHub",
	})
	if err := r.Run(); err != nil {
		r.Exit(err)
	}
}