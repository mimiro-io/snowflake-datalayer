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

package layer

import (
	"os"
	"strconv"
	"strings"
)

type Memory struct {
	Current int64
	Max     int64
}

// readMemoryStats reads the memory stats from cgroup. Only works in docker, where docker sets cgroup values.
// Other environments return empty values.
func ReadMemoryStats() Memory {
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
