package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"
)

type ConfigLoader struct {
	ticker      *time.Ticker
	httpClient  *http.Client
	loadConfig  func(location string) ([]DatasetDefinition, error)
	cachedToken string
	cacheUntil  time.Time
}

func StartConfigLoader(cfg *Config) *ConfigLoader {
	c := newConfigLoader(cfg)
	if cfg.ConfigLocation == "" {
		LOG.Info().Msg("No config file specified, skipping config loader. Set CONFIG_LOCATION to specify a config file.")
		return c
	}
	LOG.Info().Msg("Starting config loader")
	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.update(cfg)
			}
		}
	}()
	return c
}

func newConfigLoader(cfg *Config) *ConfigLoader {
	ticker := time.NewTicker(time.Duration(cfg.ConfigLoaderInterval) * time.Second)
	c := &ConfigLoader{ticker: ticker}
	c.loadConfig = c.loadFile
	if strings.HasPrefix(cfg.ConfigLocation, "http") {
		c.httpClient = &http.Client{
			Timeout: 10 * time.Second,
		}
		c.loadConfig = c.loadUrl(
			cfg.ConfigLoaderClientId,
			cfg.ConfigLoaderClientSecret,
			cfg.ConfigLoaderAudience,
			cfg.ConfigLoaderGrantType,
			cfg.ConfigLoaderAuthEndpoint,
		)
	}
	return c
}

// update checks if the config file has changed and updates the config if necessary
// returns true if config has changed
func (c *ConfigLoader) update(cfg *Config) bool {
	LOG.Debug().Msg("checking config for updates in " + cfg.ConfigLocation + ".")
	loadedConf, err := c.loadConfig(cfg.ConfigLocation)
	if err != nil {
		LOG.Error().Msg(fmt.Sprintf("Failed to load config: %v", err.Error()))
		return false
	}
	if !reflect.DeepEqual(cfg.DsMappings, loadedConf) {
		LOG.Info().Msg("Config changed, updating...")
		cfg.DsMappings = loadedConf
		return true
	} else {
		LOG.Debug().Msg("Config unchanged")
		return false
	}

}

func (c *ConfigLoader) Stop() {
	c.ticker.Stop()
}

func (c *ConfigLoader) loadUrl(clientId, clientSecret, audience, grantType, endPoint string) func(configEndpoint string) ([]DatasetDefinition, error) {
	return func(configEndpoint string) ([]DatasetDefinition, error) {
		req, err := http.NewRequest("GET", configEndpoint, nil) //
		if err != nil {
			return nil, err
		}
		now := time.Now()
		if c.cachedToken == "" || now.After(c.cacheUntil) {
			res, err2 := c.fetchNewConfigToken(clientId, clientSecret, audience, grantType, endPoint)
			if err2 != nil {
				LOG.Error().Err(err2).Msg("Unable to fetch new config token")
				return nil, err2
			}

			c.cacheUntil = time.Now().Add(time.Duration(res.ExpiresIn)*time.Second - 10*time.Second)
			c.cachedToken = res.AccessToken
		}
		bearer := fmt.Sprintf("Bearer %s", c.cachedToken)
		req.Header.Add("Authorization", bearer)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			LOG.Error().Err(err).Msg("Unable to open config url: " + configEndpoint)
			return nil, err
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		if resp.StatusCode == 200 {
			return c.processLoadedConfig(configEndpoint, resp.Body)
		} else {
			LOG.Info().Msgf("Endpoint returned %s", resp.Status)
			return nil, err
		}
	}
}

func (c *ConfigLoader) loadFile(location string) ([]DatasetDefinition, error) {
	reader, err := os.Open(location)
	if err != nil {
		LOG.Error().Err(err).Msg("Unable to open config file: " + location)
		return nil, err
	}
	return c.processLoadedConfig(location, reader)
}

func (c *ConfigLoader) processLoadedConfig(location string, reader io.ReadCloser) ([]DatasetDefinition, error) {
	s, err := io.ReadAll(reader)
	if err != nil {
		LOG.Error().Err(err).Msg("Unable to read config from : " + location)
		return nil, err
	}
	LOG.Debug().Str("config", string(s)).Msg("Loaded config from " + location)
	var dsMappingConfig DsMappingConfig
	err = json.Unmarshal(s, &dsMappingConfig)
	if err != nil {
		LOG.Warn().Err(err).Str("config", string(s)).Msg("Unable to parse config")
		return nil, err
	}
	return dsMappingConfig.DatasetDefinitions, nil
}

type authResponse struct {
	AccessToken string `json:"access_token"`
	Scope       string `json:"scope"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

func (c *ConfigLoader) fetchNewConfigToken(clientId, clientSecret, audience, grantType, endpoint string) (*authResponse, error) {
	requestBody, err := json.Marshal(map[string]string{
		"client_id":     clientId,
		"client_secret": clientSecret,
		"audience":      audience,
		"grant_type":    grantType,
	})
	if err != nil {
		LOG.Warn().Err(err).Msg("Unable to marshal auth request")
		return nil, err
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(requestBody))
	if err != nil {
		LOG.Warn().Err(err).Msg("Unable to create auth request")
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := c.httpClient.Do(req)
	if err != nil {
		LOG.Warn().Err(err).Msg("Unable to call auth endpoint")
		return nil, err
	}
	if res.StatusCode != 200 {
		b, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("not authorized. status=%v, err=%v", res.Status, string(b))
	}

	auth0Response := &authResponse{}
	err = json.NewDecoder(res.Body).Decode(auth0Response)
	if err != nil {
		return nil, err
	}
	return auth0Response, nil
}