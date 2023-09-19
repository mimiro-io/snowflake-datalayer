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
)

type Server struct {
	cfg     *Config
	E       *echo.Echo
	handler *handler
}

func NewServer(cfg *Config) (*Server, error) {
	e := echo.New()
	e.HideBanner = true
	e.Debug = false

	h, err := newHandler(cfg)
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
	g.Use(DefaultLoggerFilter(m.Statsd))
	g.Use(memoryGuard(cfg))
	g.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			err := next(c)
			if err == nil {
				return err
			}

			LOG.Error().Err(err).Msg("request failed: " + c.Request().RequestURI)
			return ToHttpError(err)
		}
	})
	if cfg.Authenticator == "jwt" {
		LOG.Info().Msg("Enabling jwt security")
		g.Use(DefaultJwtFilter(cfg))
		g.Use(JwtAuthorizer("datahub:w"))
	} else if cfg.Authenticator == "local" {
		LOG.Info().Msg("Enabling certificate security")
	}
	// /entities is the correct UDA endpoint, https://open.mimiro.io/specifications/uda/latest.html#post
	g.POST("/:dataset/entities", h.postEntities)

	g.GET("/:dataset/entities", h.getEntities)
	return &Server{
		cfg:     cfg,
		E:       e,
		handler: h,
	}, nil
}

var defaultMemoryHeadroom = 500 * 1000 * 1000

func memoryGuard(conf *Config) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			minHeadRoom := defaultMemoryHeadroom
			if conf.MemoryHeadroom > 0 {
				minHeadRoom = conf.MemoryHeadroom * 1000 * 1000
			}
			mem := readMemoryStats()
			if mem.Max > 0 {
				headroom := int(mem.Max - mem.Current)
				LOG.Debug().Msg(fmt.Sprintf("MemoryGuard: headroom: %v (min: %v)", headroom, minHeadRoom))
				if headroom < minHeadRoom {
					LOG.Info().Msg(fmt.Sprintf("MemoryGuard: headroom too low, rejecting request: %v", c.Request().URL))
					return echo.NewHTTPError(http.StatusServiceUnavailable, "MemoryGuard: headroom too low, rejecting request")
				}
			} else {
				LOG.Debug().Msg("MemoryGuard: no memory stats available")
			}

			return next(c)
		}
	}
}

type handler struct {
	ds *Dataset
}

func newHandler(cfg *Config) (*handler, error) {
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
	limit   int
	since   string
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

func (h *handler) getEntities(c echo.Context) error {
	ds, err := extractDsInfo(c)
	if err != nil {
		return err
	}
	return h.ds.ReadAll(c.Request().Context(), c.Response(), ds)
}

func extractDsInfo(c echo.Context) (dsInfo, error) {
	dataset := c.Param("dataset")
	res := dsInfo{
		name:    dataset,
		fsId:    c.Request().Header.Get(FsIdHeader),
		fsStart: c.Request().Header.Get(FsStartHeader) == "true",
		fsEnd:   c.Request().Header.Get(FsEndHeader) == "true",
		limit:   0,
		since:   "",
	}
	if c.QueryParam("since") != "" {
		return dsInfo{}, fmt.Errorf("limit not supported yet")
		//res.since = c.QueryParam("since")
	}
	if c.QueryParam("limit") != "" {
		limit, err := strconv.Atoi(c.QueryParam("limit"))
		if err != nil {
			return dsInfo{}, fmt.Errorf("limit is not a number")
		}
		if limit < 0 {
			return dsInfo{}, fmt.Errorf("limit is negative")
		}
		return dsInfo{}, fmt.Errorf("limit not supported yet")
		//res.limit = limit
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

// readMemoryStats reads the memory stats from cgroup. Only works in docker, where docker sets cgroup values.
// Other environments return empty values.
func readMemoryStats() Memory {
	bytes, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return Memory{}
	}
	path := strings.TrimSpace(strings.ReplaceAll(string(bytes), "0::", "/sys/fs/cgroup"))
	maxMem := path + "/memory.max"
	bytes, err = os.ReadFile(maxMem)
	if err != nil {
		// fallback to
		bytes, err = os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
		if err != nil {
			return Memory{}
		}
	}
	maxM, err := strconv.ParseInt(strings.TrimSpace(string(bytes)), 10, 64)
	if err != nil {
		return Memory{}
	}
	curMem := path + "/memory.current"
	bytes, err = os.ReadFile(curMem)
	if err != nil {
		bytes, err = os.ReadFile("/sys/fs/cgroup/memory/memory.usage_in_bytes")
		if err != nil {
			return Memory{}
		}
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