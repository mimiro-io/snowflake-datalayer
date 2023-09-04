package internal

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
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
	g.Use(MemoryGuard(cfg))
	if cfg.Authenticator == "jwt" {
		LOG.Info().Msg("Enabling jwt security")
		g.Use(DefaultJwtFilter(cfg))
		g.Use(JwtAuthorizer("datahub:w"))
	} else if cfg.Authenticator == "local" {
		LOG.Info().Msg("Enabling certificate security")
	}

	//keep support for POST to /changes for transition period.
	g.POST("/:dataset/changes", handler.postEntities)
	// /entities is the correct UDA endpoint, https://open.mimiro.io/specifications/uda/latest.html#post
	g.POST("/:dataset/entities", handler.postEntities)

	return e, nil
}

var defaultMemoryHeadroom = 500 * 1000 * 1000

func MemoryGuard(conf Config) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			minHeadRoom := defaultMemoryHeadroom
			if conf.MemoryHeadroom > 0 {
				minHeadRoom = conf.MemoryHeadroom * 1000 * 1000
			}
			mem := ReadMemoryStats()
			if mem.Max > 0 {
				headroom := int(mem.Max - mem.Current)
				log.Debug().Msg(fmt.Sprintf("MemoryGuard: headroom: %v (min: %v)", headroom, minHeadRoom))
				if headroom < minHeadRoom {
					log.Info().Msg(fmt.Sprintf("MemoryGuard: headroom too low, rejecting request: %v", c.Request().URL))
					return echo.NewHTTPError(http.StatusServiceUnavailable, "MemoryGuard: headroom too low, rejecting request")
				}
			} else {
				log.Debug().Msg("MemoryGuard: no memory stats available")
			}
			
			return next(c)
		}
	}
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

const (
	FsStartHeader = "universal-data-api-full-sync-start" //  bool
	FsEndHeader   = "universal-data-api-full-sync-end"   //  bool
	FsIdHeader    = "universal-data-api-full-sync-id"    // string
)

type dsInfo struct {
	name    string
	fsId    string
	fsStart bool
	fsEnd   bool
}

func (i dsInfo) IsFullSync() bool {
	return i.fsId != ""
}

func (h *handler) postEntities(c echo.Context) error {
	ds, err := extractDsInfo(c)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, "recorded", time.Now().UnixNano())
	if ds.IsFullSync() {
		err = h.ds.WriteFs(ctx, ds, c.Request().Body)
	} else {
		err = h.ds.Write(ctx, ds.name, c.Request().Body)
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(200)
}

func extractDsInfo(c echo.Context) (dsInfo, error) {
	dataset := c.Param("dataset")
	res := dsInfo{
		name:    dataset,
		fsId:    c.Request().Header.Get(FsIdHeader),
		fsStart: c.Request().Header.Get(FsStartHeader) == "true",
		fsEnd:   c.Request().Header.Get(FsEndHeader) == "true",
	}
	if res.fsId != "" {
		res.fsId = strings.ReplaceAll(res.fsId, "-", "_")
	}
	if dataset == "" {
		return dsInfo{}, fmt.Errorf("dataset not specified")
	}
	if (res.fsEnd || res.fsStart) && res.fsId == "" {
		return dsInfo{}, fmt.Errorf("full sync id not specified")
	}
	return res, nil
}

func health(c echo.Context) error {
	return c.String(http.StatusOK, "UP")
}

type Memory struct {
	Current int64
	Max     int64
}

// ReadMemoryStats reads the memory stats from cgroup. Only works in docker, where docker sets cgroup values.
// Other environments return empty values.
func ReadMemoryStats() Memory {
	bytes, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return Memory{}
	}
	path := strings.TrimSpace(strings.ReplaceAll(string(bytes), "0::", "/sys/fs/cgroup"))
	maxMem := path + "/memory.max"
	bytes, err = os.ReadFile(maxMem)
	if err != nil {
		return Memory{}
	}
	maxM, err := strconv.ParseInt(strings.TrimSpace(string(bytes)), 10, 64)
	if err != nil {
		return Memory{}
	}
	curMem := path + "/memory.current"
	bytes, err = os.ReadFile(curMem)
	if err != nil {
		return Memory{}
	}
	curM, err := strconv.ParseInt(strings.TrimSpace(string(bytes)), 10, 64)
	if err != nil {
		return Memory{}
	}

	return Memory{
		Current: curM,
		Max:     maxM,
	}
}