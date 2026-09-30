package svm

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/erpc/erpc/clients"
	"github.com/erpc/erpc/common"
	"github.com/rs/zerolog"
)

// -32008 is NoSnapshot on Solana, so a local size cap must not reach this
// extractor dressed as an upstream -32008.
func TestMaxResponseBytes_OverTheCapIsNotMissingData(t *testing.T) {
	t.Parallel()

	const limit = 64 << 10
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":"%s"}`, strings.Repeat("a", 4*limit)))
	}))
	defer srv.Close()

	cfg := &common.JsonRpcUpstreamConfig{MaxResponseBytes: limit}
	ups := common.NewFakeUpstream("svm1")
	ups.Config().Type = common.UpstreamTypeSvm
	ups.Config().Endpoint = srv.URL
	ups.Config().JsonRpc = cfg

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	logger := zerolog.Nop()
	client, err := clients.NewGenericHttpJsonRpcClient(context.Background(), &logger, "prj1", ups, u, cfg, nil, NewJsonRpcErrorExtractor())
	if err != nil {
		t.Fatal(err)
	}

	req := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"getProgramAccounts","params":[]}`))
	_, err = client.SendRequest(context.Background(), req)

	if common.HasErrorCode(err, common.ErrCodeEndpointMissingData) {
		t.Fatalf("over-cap response classified as missing data: %v", err)
	}
	if !common.HasErrorCode(err, common.ErrCodeEndpointRequestTooLarge) {
		t.Fatalf("expected ErrEndpointRequestTooLarge, got %T: %v", err, err)
	}
	if got := wireCodeOf(t, err); got != common.JsonRpcErrorServerSideException {
		t.Fatalf("wire code = %d, want %d", got, common.JsonRpcErrorServerSideException)
	}
}
