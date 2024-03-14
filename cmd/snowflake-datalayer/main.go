// Copyright 2024 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"

	common "github.com/mimiro-io/common-datalayer"
	layer "github.com/mimiro-io/snowflake-datalayer/internal"
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
