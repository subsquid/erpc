package health

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordUpstreamFailure_IgnoresLocalResponseSizeCap(t *testing.T) {
	tracker := NewTracker(&log.Logger, "test-project", 10*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tracker.Bootstrap(ctx)

	method := "debug_traceBlockByHash"

	t.Run("local cap is not the upstream's error", func(t *testing.T) {
		ups := common.NewFakeUpstream("capped")
		tracker.RecordUpstreamRequest(ups, method, common.DataFinalityStateUnknown)
		tracker.RecordUpstreamFailure(ups, method, common.DataFinalityStateUnknown,
			common.NewErrEndpointResponseTooLarge(fmt.Errorf("dropped"), 1<<20))

		mt := tracker.GetUpstreamMethodMetrics(ups, method, common.DataFinalityStateAll)
		require.NotNil(t, mt)
		assert.Equal(t, int64(1), mt.RequestsTotal.Load())
		assert.Equal(t, int64(0), mt.ErrorsTotal.Load())
	})

	t.Run("upstream's own size complaint still counts", func(t *testing.T) {
		ups := common.NewFakeUpstream("complaining")
		tracker.RecordUpstreamRequest(ups, method, common.DataFinalityStateUnknown)
		tracker.RecordUpstreamFailure(ups, method, common.DataFinalityStateUnknown,
			common.NewErrEndpointRequestTooLarge(fmt.Errorf("Response is too big"), common.EvmBlockRangeTooLarge))

		mt := tracker.GetUpstreamMethodMetrics(ups, method, common.DataFinalityStateAll)
		require.NotNil(t, mt)
		assert.Equal(t, int64(1), mt.ErrorsTotal.Load())
	})
}
