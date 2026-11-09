package syncclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/logoscore/logos-golang-module-core/pkg/coretls"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// tlsCore is an HTTPS sync endpoint with httptest's certificate, one no
// system trusts, as a self-signed core's is. TestMain names it in
// LOGOS_CORE_CERT_SHA256 before any client for core is made.
var tlsCore *httptest.Server

func TestMain(m *testing.M) {
	tlsCore = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		protocol.OutboundMinionMessage{MessageID: protocol.NewID(), BuildID: buildID}.SetHeaders(w.Header())
		w.WriteHeader(http.StatusNoContent)
	}))
	_ = os.Setenv(coretls.EnvCertSHA256, coretls.Fingerprint(tlsCore.Certificate().Raw))
	code := m.Run()
	tlsCore.Close()
	os.Exit(code)
}

// A client given no http.Client of its own trusts core as the module's
// environment says (module-core's coretls): the self-signed core by its
// fingerprint, where the system's trust would refuse it.
func TestSyncTrustsCoreAsTheEnvironmentSays(t *testing.T) {
	if _, err := NewHTTPClient(tlsCore.URL, nil).Sync(context.Background(), inbound()); err != nil {
		t.Fatalf("through coretls: %v", err)
	}
	if _, err := NewHTTPClient(tlsCore.URL, &http.Client{}).Sync(context.Background(), inbound()); err == nil {
		t.Fatal("a client of the module's own, with the system's trust, took the self-signed core")
	}
}
