package internal

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
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

	m, err := NewMetrics(cfg)
	if err != nil {
		return nil, err
	}
	g.Use(DefaultLoggerFilter(cfg, m.Statsd))
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
	ds *Dataset
}

func newHandler(cfg Config) (*handler, error) {
	sf, err := NewSnowflake(cfg)
	if err != nil {
		return nil, err
	}
	ds := NewDataset(cfg, sf)

	return &handler{
		ds: ds,
	}, nil
}

func (h *handler) changes(c echo.Context) error {
	dataset := c.Param("dataset")
	ctx := context.Background()
	ctx = context.WithValue(ctx, "recorded", time.Now().UnixNano())
	err := h.ds.Write(ctx, dataset, c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(200)
}

func health(c echo.Context) error {
	return c.String(http.StatusOK, "UP")
}