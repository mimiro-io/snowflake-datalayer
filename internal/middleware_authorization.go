package internal

import (
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v4"
	"github.com/juliangruber/go-intersect"
	"github.com/labstack/echo/v4"
)

func JwtAuthorizer(scopes ...string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if c.Get("user") == nil { // user never got set, oops
				return echo.NewHTTPError(http.StatusForbidden, "user not set")
			}
			token := c.Get("user").(*jwt.Token)

			claims := token.Claims.(*CustomClaims)
			if claims.Gty == "client-credentials" { // this is a machine or an application token
				var claimScopes []string
				if len(claims.scopes()) > 0 {
					claimScopes = strings.Split(claims.scopes()[0], " ")
				}
				res := intersect.Simple(claimScopes, scopes)
				if len(res) == 0 { // no intersection
					LOG.Debug().
						Str("subject", token.Claims.(*CustomClaims).Subject).
						Strs("scopes", claimScopes).
						Strs("userScopes", scopes).
						Msg("user attempted login with missing or wrong scope")

					return echo.NewHTTPError(http.StatusForbidden, "user attempted login with missing or wrong scope")

				}
			} else {
				// this is a user
				if !claims.Adm { // this will only be set for system admins, we only support mimiro Adm at the moment
					// if not, we need to see if the url requested contains the user id
					subject := claims.Subject
					// it needs the subject in the url
					uri := c.Request().RequestURI
					if !strings.Contains(uri, subject) { // not present, so forbidden
						return echo.NewHTTPError(http.StatusForbidden, "user has no access to path")
					}
				}
			}

			return next(c)
		}
	}
}
