package clients

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/erpc/erpc/common"
)

// jsonrpsee (and so reth) uses this message for a response over its own size cap.
const responseTooBigMessage = "Response is too big"

var errResponseTooBig = errors.New("upstream response exceeded maxResponseBytes")

// cappedBody stops reading once more than limit bytes have come through, so an
// oversized response is never buffered whole.
type cappedBody struct {
	body      io.ReadCloser
	remaining int64
	exceeded  bool
}

func newCappedBody(body io.ReadCloser, limit int64) *cappedBody {
	return &cappedBody{body: body, remaining: limit}
}

func (b *cappedBody) Read(p []byte) (int, error) {
	if b.exceeded {
		return 0, errResponseTooBig
	}

	// One byte past the cap is enough to tell "exactly at the cap" from "over it".
	// Compared as remaining < len(p) so that remaining+1 cannot overflow.
	if b.remaining < int64(len(p)) {
		p = p[:b.remaining+1]
	}

	n, err := b.body.Read(p)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		b.exceeded = true
		return n, errResponseTooBig
	}

	return n, err
}

func (b *cappedBody) Close() error {
	return b.body.Close()
}

// Exceeded is safe on a nil *cappedBody, which is what an uncapped body gets.
func (b *cappedBody) Exceeded() bool {
	return b != nil && b.exceeded
}

// declaredTooBig reports whether the response announces a body over the cap.
// Only an uncompressed Content-Length says anything about the decoded size.
func declaredTooBig(resp *http.Response, limit int64) bool {
	isGzip := resp.Header.Get("Content-Encoding") == "gzip"

	return !isGzip && resp.ContentLength > limit
}

// expectedBodySize is the read buffer to reserve up front. A gzip Content-Length
// is the compressed size, which can be far above the cap, so the reservation is
// clamped to the cap rather than trusted.
func expectedBodySize(resp *http.Response, limit int64) int {
	size := resp.ContentLength
	if limit > 0 && size > limit {
		size = limit
	}

	return int(size)
}

// newErrResponseTooBig is raised by eRPC itself, not parsed from an upstream
// body, so no architecture's error normalizer sees it: -32008 is jsonrpsee's
// "Response is too big" on EVM but NoSnapshot on Solana.
func newErrResponseTooBig(upstreamType common.UpstreamType, limit int64) error {
	// On EVM this matches what a reth upstream's own cap turns into, and it is
	// the code the eth_getLogs and trace_filter splitters act on.
	wireCode := common.JsonRpcErrorServerSideException
	if upstreamType == common.UpstreamTypeEvm {
		wireCode = common.JsonRpcErrorEvmLargeRange
	}

	return common.NewErrEndpointResponseTooLarge(
		common.NewErrJsonRpcExceptionInternal(
			0,
			wireCode,
			responseTooBigMessage,
			errResponseTooBig,
			map[string]interface{}{
				"data": fmt.Sprintf("Exceeded max limit of %d", limit),
			},
		),
		limit,
	)
}
