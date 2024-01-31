package layer

import (
	"fmt"

	common "github.com/mimiro-io/common-datalayer"
	"github.com/mimiro-io/datahub-snowflake-datalayer/internal/api"
)

var defaultMemoryHeadroom = 500 * 1000 * 1000

func (sf *SnowflakeDataLayer) assertMemory() common.LayerError {
	minHeadRoom := defaultMemoryHeadroom
	if confHeadRoomAny, ok := sf.config.NativeSystemConfig[MemoryHeadroom]; ok {
		if confHeadRoom, ok := confHeadRoomAny.(int); ok && confHeadRoom > 0 {
			minHeadRoom = confHeadRoom * 1000 * 1000
		}
	}

	mem := api.ReadMemoryStats()
	if mem.Max > 0 {
		headroom := int(mem.Max - mem.Current)
		sf.logger.Debug(fmt.Sprintf("MemoryGuard: headroom: %v (min: %v)", headroom, minHeadRoom))
		if headroom < minHeadRoom {
			sf.logger.Warn("MemoryGuard: headroom too low, rejecting request")
			return ErrHeadroom
		}
	} else {
		sf.logger.Debug("MemoryGuard: no memory stats available")
	}
	return nil
}
