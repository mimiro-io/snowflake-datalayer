package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog"
)

var LoggerSkipper = func(e echo.Context) bool {
	return e.Request().URL.Path == "/health"
}

type LoggerConfig struct {
	// Skipper defines a function to skip middleware.
	Skipper middleware.Skipper

	// BeforeFunc defines a function which is executed just before the middleware.
	BeforeFunc middleware.BeforeFunc

	log zerolog.Logger
	cfg Config
	m   statsd.ClientInterface
}

func DefaultLoggerFilter(m statsd.ClientInterface) echo.MiddlewareFunc {
	return LoggerFilter(LoggerConfig{
		Skipper: LoggerSkipper,
		m:       m,
		log:     LOG.With().Str("logger", "http").Logger(),
	})
}

func LoggerFilter(config LoggerConfig) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if config.Skipper(c) {
				return next(c)
			}

			if config.BeforeFunc != nil {
				config.BeforeFunc(c)
			}

			start := time.Now()
			req := c.Request()
			res := c.Response()

			tags := []string{
				fmt.Sprintf("application:%s", config.cfg.ServiceName),
				fmt.Sprintf("method:%s", strings.ToLower(req.Method)),
				fmt.Sprintf("url:%s", strings.ToLower(req.RequestURI)),
				fmt.Sprintf("status:%d", res.Status),
			}

			err := next(c)
			if err != nil {
				c.Error(err)
			}

			timed := time.Since(start)

			_ = config.m.Incr("http.count", tags, 1)
			_ = config.m.Timing("http.time", timed, tags, 1)
			_ = config.m.Gauge("http.size", float64(res.Size), tags, 1)

			msg := fmt.Sprintf(
				"%d - %s %s (time: %s, size: %d, user_agent: %s)",
				res.Status,
				req.Method,
				req.RequestURI,
				timed.String(),
				res.Size,
				req.UserAgent(),
			)

			l := config.log.Debug().
				Str("time", timed.String()).
				Str("request", fmt.Sprintf("%s %s", req.Method, req.RequestURI)).
				Int("status", res.Status).
				Int64("size", res.Size).
				Str("user_agent", req.UserAgent())

			id := req.Header.Get(echo.HeaderXRequestID)
			if id == "" {
				id = res.Header().Get(echo.HeaderXRequestID)
				l.Str("request_id", id)
			}
			l.Msg(msg)

			return err
		}
	}
}
