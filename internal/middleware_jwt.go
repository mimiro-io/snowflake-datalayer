package internal

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v4"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/lestrrat-go/jwx/v2/jwk"
)

type (
	JwtConfig struct {
		// Skipper defines a function to skip middleware.
		Skipper middleware.Skipper

		// BeforeFunc defines a function which is executed just before the middleware.
		BeforeFunc middleware.BeforeFunc

		cache     *jwk.Cache
		Wellknown string
		Audience  string
		Issuer    string
	}
)

type CustomClaims struct {
	Scope string `json:"scope"`
	Gty   string `json:"gty"`
	Adm   bool   `json:"adm"`
	jwt.RegisteredClaims
}

func (claims CustomClaims) scopes() []string {
	return strings.Split(claims.Scope, ",")
}

type Response struct {
	Message string `json:"message"`
}

type Jwks struct {
	Keys []JSONWebKeys `json:"keys"`
}

type JSONWebKeys struct {
	Kty string   `json:"kty"`
	Kid string   `json:"kid"`
	Use string   `json:"use"`
	N   string   `json:"n"`
	E   string   `json:"e"`
	X5c []string `json:"x5c"`
}

// Errors
var (
	ErrJWTMissing = echo.NewHTTPError(http.StatusBadRequest, "missing or malformed jwt")
	ErrJWTInvalid = echo.NewHTTPError(http.StatusUnauthorized, "invalid or expired jwt")
)

func DefaultJwtFilter(cfg *Config) echo.MiddlewareFunc {
	config := JwtConfig{
		Skipper:   middleware.DefaultSkipper,
		Wellknown: cfg.JwtWellKnown,
		Audience:  cfg.TokenAudience,
		Issuer:    cfg.TokenIssuer,
	}
	return JWTWithConfig(config)
}

func JWTWithConfig(config JwtConfig) echo.MiddlewareFunc {
	if config.cache == nil {
		c := jwk.NewCache(context.Background())
		if err := c.Register(config.Wellknown); err != nil {
			LOG.Panic().Err(err).Msg(err.Error())
		}
		config.cache = c
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if config.Skipper(c) {
				return next(c)
			}

			if config.BeforeFunc != nil {
				config.BeforeFunc(c)
			}

			auth, err := extractToken(c)
			if err != nil {
				return err
			}

			token, _, err := config.validateToken(auth)

			if err == nil {
				c.Set("user", token)
				return next(c)
			}

			return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
		}
	}
}

func (config *JwtConfig) validateToken(auth string) (*jwt.Token, *CustomClaims, error) {
	token, err := jwt.ParseWithClaims(auth, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		set, err := config.cache.Get(context.Background(), config.Wellknown)
		if err != nil {
			return nil, errors.New("unable to load well-known from cache")
		}

		switch jwks := set.(type) {
		case jwk.Set:
			kid := token.Header["kid"].(string)
			k, ok := jwks.LookupKeyID(kid)
			if !ok {
				return nil, errors.New("kid not found in jwks")
			}

			// Check for x5c. If present, convert to RSA Public key.
			// If not present, create raw key.
			der, ok := k.X509CertChain().Get(0)
			if ok {
				pem := "-----BEGIN CERTIFICATE-----\n" + string(der) + "\n-----END CERTIFICATE-----"
				return jwt.ParseRSAPublicKeyFromPEM([]byte(pem))
			} else {
				var pk any
				err = k.Raw(&pk)
				return pk, err
			}
		default:
			return nil, errors.New("unknown type in well-known cache")
		}
	})
	if err != nil {
		return nil, nil, err
	}

	if !token.Valid {
		return token, nil, ErrJWTInvalid
	}

	claims := token.Claims.(*CustomClaims)
	audience := config.Audience
	issuer := config.Issuer

	checkAud := claims.VerifyAudience(audience, false)
	if !checkAud {
		err = fmt.Errorf("invalid audience: %w", ErrJWTInvalid)
	}

	checkIss := claims.VerifyIssuer(issuer, false)
	if !checkIss {
		err = fmt.Errorf("invalid issuer: %w", ErrJWTInvalid)
	}

	checkSigningMethod := token.Method.Alg() == "RS256"
	if !checkSigningMethod {
		err = fmt.Errorf("non matching signing method: %w", ErrJWTInvalid)
	}

	return token, claims, err
}

func extractToken(c echo.Context) (string, error) {
	auth := c.Request().Header.Get("Authorization")
	l := len("Bearer")
	if len(auth) > l+1 && auth[:l] == "Bearer" {
		return auth[l+1:], nil
	}
	return "", ErrJWTMissing
}
