package main

import (
	"os"

	common "github.com/mimiro-io/common-datalayer"
	layer "github.com/mimiro-io/datahub-snowflake-datalayer/internal/layer"
)

func main() {
	args := os.Args[1:]
	configFolderLocation := os.Environ()["CONFIG_LOCATION"]
	if len(args) >= 1 {
		configFolderLocation = args[0]
	}
	common.NewServiceRunner(layer.NewSnowflakeDataLayer).
		WithConfigLocation(configFolderLocation).
		// WithEnrichConfig(EnrichConfig).
		StartAndWait()
}
