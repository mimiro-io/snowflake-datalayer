package internal

import (
	"errors"
	"flag"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type Config struct {
	LogType            string
	LogLevel           string
	ServiceName        string
	File               string
	PrivateCert        string
	CertPassword       string
	SnowflakeUser      string
	SnowflakePassword  string
	SnowflakeAccount   string
	SnowflakeDb        string
	SnowflakeSchema    string
	SnowflakeWarehouse string
	SnowflakeUri       string
	Port               int
	JwtWellKnown       string
	TokenIssuer        string
	TokenAudience      string
	NodePublicKey      string
	Authenticator      string
}

func (c *Config) common() *flag.FlagSet {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&c.LogType, "log-type", "console", "Determines log type. Valid are console or json.")
	fs.StringVar(&c.LogLevel, "log-level", "info", "Log level. error, warn, trace, debug or info.")
	fs.StringVar(&c.ServiceName, "service", "datahub-snowflake-datalayer", "Override service name. For logging purposes.")
	fs.StringVar(&c.File, "file", "", "uda dataset to load")
	fs.StringVar(&c.SnowflakeUser, "snowflake-user", "", "Snowflake username. Required.")
	fs.StringVar(&c.SnowflakePassword, "snowflake-password", "", "Snowflake password. Required.")
	fs.StringVar(&c.SnowflakeAccount, "snowflake-account", "", "Snowflake account to use.")
	fs.StringVar(&c.SnowflakeDb, "snowflake-db", "", "Snowflake db to write to.")
	fs.StringVar(&c.SnowflakeSchema, "snowflake-schema", "", "Snowflake schema if set.")
	fs.StringVar(&c.SnowflakeWarehouse, "snowflake-warehouse", "", "Snowflake warehouse")
	fs.StringVar(&c.SnowflakeUri, "snowflake-connection-string", "", "Alternative if more parameters are needed.")
	fs.StringVar(&c.PrivateCert, "private-cert", "", "Base64 encoded private cert")
	fs.StringVar(&c.CertPassword, "cert-password", "", "Password to unlock private cert (can be empty if cert is not encrypted)")

	return fs
}

func (c *Config) Flags() *flag.FlagSet {
	return c.common()
}

// ServerFlags add some extra flags when run as a server
func (c *Config) ServerFlags() *flag.FlagSet {
	fs := c.common()
	fs.IntVar(&c.Port, "port", 8080, "http server port")
	fs.StringVar(&c.JwtWellKnown, "well-known", "", "url to well-known.json endpoint")
	fs.StringVar(&c.TokenIssuer, "issuer", "", "either a jwt issuer or a node:<id> issuer if public key is set")
	fs.StringVar(&c.TokenAudience, "audience", "", "either a jwt audience or a node:<id> audience if public key is set")
	fs.StringVar(&c.NodePublicKey, "public-key", "", "DataHub public key. Enables public key access.")
	fs.StringVar(&c.Authenticator, "authenticator", "jwt", "middleware for authentication. 'noop' disables auth, jwt enables it")
	return fs
}

// LoadEnv goes through all configuration fields and load values from ENV if present.
// Values from ENV will always overwrite params if they are present.
func (c *Config) LoadEnv() error {
	elems := []string{
		"LogType:LOG_TYPE",
		"LogLevel:LOG_LEVEL",
		"ServiceName:SERVICE_NAME",
		"File:FILE",
		"SnowflakeUser:SNOWFLAKE_USER",
		"SnowflakePassword:SNOWFLAKE_PASSWORD",
		"SnowflakeAccount:SNOWFLAKE_ACCOUNT",
		"SnowflakeDb:SNOWFLAKE_DB",
		"SnowflakeSchema:SNOWFLAKE_SCHEMA",
		"SnowflakeUri:SNOWFLAKE_CONNECTION_STRING",
		"Port:PORT",
		"JwtWellKnown:WELL_KNOWN",
		"TokenIssuer:ISSUER",
		"TokenAudience:AUDIENCE",
		"NodePublicKey:NODE_PUBLIC_KEY",
		"Authenticator:AUTHENTICATOR",
		"PrivateCert:PRIVATE_CERT",
		"CertPassword:CERT_PASSWORD",
	}
	o := reflect.ValueOf(c).Elem()
	for _, item := range elems {
		fieldName, env, _ := strings.Cut(item, ":")
		if v, ok := os.LookupEnv(env); ok {
			field := o.FieldByName(fieldName)
			if field.CanInt() {
				conv, err := strconv.Atoi(v)
				if err != nil {
					return err
				}
				field.Set(reflect.ValueOf(conv))
			} else {
				field.Set(reflect.ValueOf(v))
			}

		}
	}
	return nil
}

// Validate does some simple validity checking. If there are required values somewhere, they should be
// validated here.
func (c *Config) Validate() error {
	if c.Authenticator == "jwt" && c.JwtWellKnown == "" {
		return errors.New("must set well known when jwt is used for auth")
	}
	if c.Authenticator == "jwt" && (c.TokenIssuer == "" || c.TokenAudience == "") {
		return errors.New("must set well audience/issuer when jwt is used for auth")
	}
	//if c.SnowflakeUri == "" && c.SnowflakeUser == "" {
	//	return errors.New("you must either provide both a snowflake uri and user/password")
	//}
	return nil
}
