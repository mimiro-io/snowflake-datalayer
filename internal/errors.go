package internal

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
)

var ErrNoImplicitDataset = errors.New("no implicit mapping for dataset")
var ErrQuery = errors.New("failed to query snowflake")

func ToHttpError(err error) *echo.HTTPError {
	if errors.Is(err, ErrNoImplicitDataset) {
		return echo.NewHTTPError(http.StatusBadRequest, "No mapping for dataset")
	}
	if errors.Is(err, ErrQuery) {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to query snowflake")
	}
	return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
}