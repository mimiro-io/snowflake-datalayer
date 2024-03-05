package layer

import (
	"fmt"

	common "github.com/mimiro-io/common-datalayer"
)

var defaultMemoryHeadroom = 500 * 1000 * 1000

func (dl *SnowflakeDataLayer) assertMemory() common.LayerError {
	minHeadRoom := defaultMemoryHeadroom
	if confHeadRoomAny, ok := dl.config.NativeSystemConfig[MemoryHeadroom]; ok {
		if confHeadRoom, ok := confHeadRoomAny.(int); ok && confHeadRoom > 0 {
			minHeadRoom = confHeadRoom * 1000 * 1000
		}
	}

	mem := ReadMemoryStats()
	if mem.Max > 0 {
		headroom := int(mem.Max - mem.Current)
		dl.logger.Debug(fmt.Sprintf("MemoryGuard: headroom: %v (min: %v)", headroom, minHeadRoom))
		if headroom < minHeadRoom {
			dl.logger.Warn("MemoryGuard: headroom too low, rejecting request")
			return ErrHeadroom
		}
	} else {
		dl.logger.Debug("MemoryGuard: no memory stats available")
	}
	return nil
}
