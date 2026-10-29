package syncclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	coreerrors "github.com/logoscore/logos-golang-channel-core/v3/pkg/errors"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// DefaultMaxReplyBytes caps a reply's body when MaxReplyBytes is not set.
const DefaultMaxReplyBytes int64 = 16 << 20

// HTTPClient is a runtime.SyncClient implementation using HTTP: the minion's
// ciphertext as the request body, the message's ids in headers, and core's
// reply the same way (200 with ciphertext, 204 with nothing for the minion).
type HTTPClient struct {
	BaseURL string
	// Endpoint overrides protocol.SyncPath.
	Endpoint string
	Client   *http.Client
	// MaxReplyBytes caps a reply's body; DefaultMaxReplyBytes when zero.
	MaxReplyBytes int64
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

	endpoint := c.Endpoint
	if endpoint == "" {
		endpoint = protocol.SyncPath
	}
	url := strings.TrimRight(c.BaseURL, "/") + endpoint

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(in.Data))
	if err != nil {
		return protocol.OutboundMinionMessage{}, coreerrors.Wrap(coreerrors.CodeInternal, "build sync request", err)
	}
	in.SetHeaders(req.Header)

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

	out, err := ReadReply(resp, in.MinionID, c.MaxReplyBytes)
	if err != nil {
		var vErr *protocol.ValidationError
		if errors.As(err, &vErr) {
			return protocol.OutboundMinionMessage{}, coreerrors.Wrap(coreerrors.CodeCanonicalInvalid, "invalid outbound canonical message", err)
		}
		return protocol.OutboundMinionMessage{}, coreerrors.Wrap(coreerrors.CodeSyncRejected, "sync reply", err)
	}
	return out, nil
}

// ReadReply reads core's reply to a sync request for minionID: a 200 with
// ciphertext or a 204 with nothing for the minion. Any other status is an
// error carrying core's JSON error body. The body may be at most maxBytes
// (DefaultMaxReplyBytes when zero or less). A reply that fails
// protocol.ValidateOutbound returns its *protocol.ValidationError.
func ReadReply(resp *http.Response, minionID string, maxBytes int64) (protocol.OutboundMinionMessage, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxReplyBytes
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return protocol.OutboundMinionMessage{}, fmt.Errorf("rejected: status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	out := protocol.OutboundFromHeaders(resp.Header)
	if resp.StatusCode == http.StatusOK {
		if ct := resp.Header.Get("Content-Type"); ct != protocol.SyncContentType {
			return out, fmt.Errorf("reply content type %q, want %q", ct, protocol.SyncContentType)
		}
		if resp.ContentLength > maxBytes {
			return out, fmt.Errorf("reply of %d bytes is over the %d-byte limit", resp.ContentLength, maxBytes)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
		if err != nil {
			return out, fmt.Errorf("read reply: %w", err)
		}
		if int64(len(body)) > maxBytes {
			return out, fmt.Errorf("reply is over the %d-byte limit", maxBytes)
		}
		out.Data = body
	}
	if err := protocol.ValidateOutbound(out); err != nil {
		return out, err
	}
	if out.MinionID != minionID {
		return out, fmt.Errorf("reply is for minion %s, not %s", out.MinionID, minionID)
	}
	return out, nil
}
