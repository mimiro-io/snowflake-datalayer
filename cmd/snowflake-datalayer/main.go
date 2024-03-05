package main

import (
	"os"

	common "github.com/mimiro-io/common-datalayer"
	layer "github.com/mimiro-io/datahub-snowflake-datalayer/internal"
)

func main() {
	// either pass in command argument or set DATALAYER_CONFIG_PATH environment variable.
	// if nothing is set, the ServiceRunner defaults to ./config
	configFolderLocation := ""
	args := os.Args[1:]
	if len(args) >= 1 {
		configFolderLocation = args[0]
	}
	common.NewServiceRunner(layer.NewSnowflakeDataLayer).
		WithConfigLocation(configFolderLocation).
		WithEnrichConfig(layer.EnvOverrides).
		StartAndWait()
}
