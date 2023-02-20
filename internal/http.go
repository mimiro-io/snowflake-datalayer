package internal

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/labstack/echo/v4"
	"net/http"
)

func NewServer(cfg Config) (*echo.Echo, error) {
	e := echo.New()
	e.HideBanner = true
	e.Debug = false
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		LOG.Error().Err(fmt.Errorf("request failed: %w", err)).Msg(err.Error())

		if c.Response().Committed {
			return
		}

		he, ok := err.(*echo.HTTPError)
		if ok {
			if he.Internal != nil {
				if herr, ok := he.Internal.(*echo.HTTPError); ok {
					he = herr
				}
			}
		} else {
			he = &echo.HTTPError{
				Code:    http.StatusInternalServerError,
				Message: http.StatusText(http.StatusInternalServerError),
			}
		}

		// Issue #1426
		code := he.Code
		message := he.Message
		if m, ok := he.Message.(string); ok {
			if e.Debug {
				message = echo.Map{"message": m, "error": err.Error()}
			} else {
				message = echo.Map{"message": m}
			}
		} else {
			// if not a string, convert it
			if e.Debug {
				message = echo.Map{"message": fmt.Sprintf("%v", err), "error": err.Error()}
			}
		}

		// Send response
		if c.Request().Method == http.MethodHead { // Issue #608
			err = c.NoContent(he.Code)
		} else {
			err = c.JSON(code, message)
		}
		if err != nil {
			LOG.Error().Err(err).Msg(err.Error())
		}
	}

	handler, err := newHandler(cfg)
	if err != nil {
		return nil, err
	}

	e.GET("/health", health)

	// add a group to prevent middleware on health
	g := e.Group("/datasets")
	g.Use(DefaultLoggerFilter(cfg, handler.m))
	if cfg.Authenticator == "jwt" {
		LOG.Info().Msg("Enabling jwt security")
		g.Use(DefaultJwtFilter(cfg))
		g.Use(JwtAuthorizer("datahub:w"))
	} else if cfg.Authenticator == "local" {
		LOG.Info().Msg("Enabling certificate security")
	}

	g.POST("/:dataset/changes", handler.changes)

	return e, nil
}

type handler struct {
	cfg Config
	sf  *Snowflake
	ds  *Dataset
	m   statsd.ClientInterface
}

func newHandler(cfg Config) (*handler, error) {
	metrics, err := NewMetrics(cfg)
	if err != nil {
		return nil, err
	}
	sf, err := NewSnowflake(cfg, metrics.Statsd)
	if err != nil {
		return nil, err
	}
	ds := NewDataset(cfg, sf, metrics.Statsd)

	return &handler{
		cfg: cfg,
		sf:  sf,
		ds:  ds,
		m:   metrics.Statsd,
	}, nil
}

func (h *handler) changes(c echo.Context) error {
	dataset := c.Param("dataset")
	err := h.ds.Write(context.Background(), dataset, c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(200)
}

func health(c echo.Context) error {
	return c.String(http.StatusOK, "UP")
}
