package internal

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	common_datalayer "github.com/mimiro-io/common-datalayer"
)

type Config struct {
	LogType             string
	LogLevel            string
	ServiceName         string
	PrivateCert         string // deprecated
	SnowflakeUser       string
	SnowflakePassword   string
	SnowflakeAccount    string
	SnowflakeDB         string
	SnowflakeSchema     string
	SnowflakeWarehouse  string
	SnowflakeURI        string
	SnowflakePrivateKey string
	Port                int
	JwtWellKnown        string
	TokenIssuer         string
	TokenAudience       string
	// NodePublicKey            string
	Authenticator            string
	MemoryHeadroom           int
	DsMappings               []*common_datalayer.DatasetDefinition
	ConfigLocation           string
	ConfigLoaderInterval     int
	ConfigLoaderClientID     string
	ConfigLoaderClientSecret string
	ConfigLoaderAudience     string
	ConfigLoaderGrantType    string
	ConfigLoaderAuthEndpoint string
}

func (c *Config) common() *flag.FlagSet {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&c.LogType, "log-type", "console", "Determines log type. Valid are console or json.")
	fs.StringVar(&c.LogLevel, "log-level", "info", "Log level. error, warn, trace, debug or info.")
	fs.StringVar(
		&c.ServiceName,
		"service",
		"datahub-snowflake-datalayer",
		"Override service name. For logging purposes.",
	)
	fs.StringVar(&c.SnowflakeUser, "snowflake-user", "", "Snowflake username. Required.")
	fs.StringVar(&c.SnowflakePassword, "snowflake-password", "", "Snowflake password. Required.")
	fs.StringVar(&c.SnowflakeAccount, "snowflake-account", "", "Snowflake account to use.")
	fs.StringVar(&c.SnowflakeDB, "snowflake-db", "", "Snowflake db to write to.")
	fs.StringVar(&c.SnowflakeSchema, "snowflake-schema", "", "Snowflake schema if set.")
	fs.StringVar(&c.SnowflakeWarehouse, "snowflake-warehouse", "", "Snowflake warehouse")
	fs.StringVar(&c.SnowflakeURI, "snowflake-connection-string", "", "Alternative if more parameters are needed.")
	fs.StringVar(&c.SnowflakePrivateKey, "snowflake-private-key", "", "base64 encoded private key.")
	fs.StringVar(&c.PrivateCert, "private-cert", "", "deprecated, use snowflake-private-key.")
	fs.IntVar(&c.ConfigLoaderInterval, "config-loader-interval", 60, "Interval in seconds to reload config file")
	fs.StringVar(&c.ConfigLocation, "config-location", "", "Location of config file. file:// or http://")
	fs.StringVar(&c.ConfigLoaderClientID, "config-loader-client-id", "", "Client id for config loader")
	fs.StringVar(&c.ConfigLoaderClientSecret, "config-loader-client-secret", "", "Client secret for config loader")
	fs.StringVar(&c.ConfigLoaderAudience, "config-loader-audience", "", "Audience for config loader")
	fs.StringVar(&c.ConfigLoaderGrantType, "config-loader-grant-type", "", "Grant type for config loader")
	fs.StringVar(&c.ConfigLoaderAuthEndpoint, "config-loader-auth-endpoint", "", "Auth endpoint for config loader")

	return fs
}

func (c *Config) Flags() *flag.FlagSet {
	return c.common()
}

// ServerFlags add some extra flags when run as a server
func (c *Config) ServerFlags() *flag.FlagSet {
	fs := c.common()
	fs.IntVar(&c.Port, "port", 8080, "http server port")
	fs.IntVar(&c.MemoryHeadroom, "memory-headroom", 0, "http server port")
	fs.StringVar(&c.JwtWellKnown, "well-known", "", "url to well-known.json endpoint")
	fs.StringVar(&c.TokenIssuer, "issuer", "", "either a jwt issuer or a node:<id> issuer if public key is set")
	fs.StringVar(&c.TokenAudience, "audience", "", "either a jwt audience or a node:<id> audience if public key is set")
	// fs.StringVar(&c.NodePublicKey, "public-key", "", "DataHub public key. Enables public key access.")
	fs.StringVar(
		&c.Authenticator,
		"authenticator",
		"jwt",
		"middleware for authentication. 'noop' disables auth, jwt enables it",
	)
	return fs
}

// LoadEnv goes through all configuration fields and load values from ENV if present.
// Values from ENV will always overwrite params if they are present.
func (c *Config) LoadEnv() error {
	elems := []string{
		"LogType:LOG_TYPE",
		"LogLevel:LOG_LEVEL",
		"ServiceName:SERVICE_NAME",
		"SnowflakeUser:SNOWFLAKE_USER",
		"SnowflakePassword:SNOWFLAKE_PASSWORD",
		"SnowflakeAccount:SNOWFLAKE_ACCOUNT",
		"SnowflakeDB:SNOWFLAKE_DB",
		"SnowflakeSchema:SNOWFLAKE_SCHEMA",
		"SnowflakeURI:SNOWFLAKE_CONNECTION_STRING",
		"SnowflakePrivateKey:SNOWFLAKE_PRIVATE_KEY",
		"Port:PORT",
		"MemoryHeadroom:MEMORY_HEADROOM",
		"JwtWellKnown:WELL_KNOWN",
		"TokenIssuer:ISSUER",
		"TokenAudience:AUDIENCE",
		//"NodePublicKey:NODE_PUBLIC_KEY",
		"Authenticator:AUTHENTICATOR",
		"PrivateCert:PRIVATE_CERT",
		"ConfigLocation:CONFIG_LOCATION",
		"ConfigLoaderInterval:CONFIG_LOADER_INTERVAL",
		"ConfigLoaderClientID:CONFIG_LOADER_CLIENT_ID",
		"ConfigLoaderClientSecret:CONFIG_LOADER_CLIENT_SECRET",
		"ConfigLoaderAudience:CONFIG_LOADER_AUDIENCE",
		"ConfigLoaderGrantType:CONFIG_LOADER_GRANT_TYPE",
		"ConfigLoaderAuthEndpoint:CONFIG_LOADER_AUTH_ENDPOINT",
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

func (c *Config) Mapping(name string) (*common_datalayer.DatasetDefinition, error) {
	for _, mapping := range c.DsMappings {
		if mapping.DatasetName == name {
			return mapping, nil
		}
	}
	return nil, fmt.Errorf("mapping %s not found", name)
}
