package syncclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

const defaultSyncPath = "/api/channel/sync"

// HTTPClient is a runtime.SyncClient implementation using HTTP.
type HTTPClient struct {
	BaseURL  string
	Endpoint string
	Client   *http.Client
}

func NewHTTPClient(baseURL string, client *http.Client) *HTTPClient {
	return &HTTPClient{BaseURL: baseURL, Client: client}
}

func (c *HTTPClient) Sync(ctx context.Context, in protocol.InboundMinionMessage) (protocol.OutboundMinionMessage, error) {
	if c == nil {
		return protocol.OutboundMinionMessage{}, coreerrors.New(coreerrors.CodeInvalidInput, "sync HTTP client is nil")
	}
	if strings.TrimSpace(c.BaseURL) == "" {
		return protocol.OutboundMinionMessage{}, coreerrors.New(coreerrors.CodeInvalidInput, "sync base URL is required")
	}
	if err := protocol.ValidateInbound(in); err != nil {
		return protocol.OutboundMinionMessage{}, coreerrors.Wrap(coreerrors.CodeCanonicalInvalid, "invalid inbound canonical message", err)
	}

	payload, err := json.Marshal(in)
	if err != nil {
		return protocol.OutboundMinionMessage{}, coreerrors.Wrap(coreerrors.CodeInternal, "marshal inbound message", err)
	}

	endpoint := c.Endpoint
	if endpoint == "" {
		endpoint = defaultSyncPath
	}
	url := strings.TrimRight(c.BaseURL, "/") + endpoint

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return protocol.OutboundMinionMessage{}, coreerrors.Wrap(coreerrors.CodeInternal, "build sync request", err)
	}
	req.Header.Set("Content-Type", "application/json")

	hc := c.Client
	if hc == nil {
		hc = http.DefaultClient
	}

	resp, err := hc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return protocol.OutboundMinionMessage{}, coreerrors.Wrap(coreerrors.CodeSyncTimeout, "sync request context canceled", err)
		}
		return protocol.OutboundMinionMessage{}, coreerrors.Wrap(coreerrors.CodeSyncRejected, "sync request failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return protocol.OutboundMinionMessage{}, coreerrors.New(coreerrors.CodeSyncRejected, fmt.Sprintf("sync rejected: status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body))))
	}

	var out protocol.OutboundMinionMessage
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return protocol.OutboundMinionMessage{}, coreerrors.Wrap(coreerrors.CodeSyncRejected, "decode sync response", err)
	}
	if err := protocol.ValidateOutbound(out); err != nil {
		return protocol.OutboundMinionMessage{}, coreerrors.Wrap(coreerrors.CodeCanonicalInvalid, "invalid outbound canonical message", err)
	}

	return out, nil
}
