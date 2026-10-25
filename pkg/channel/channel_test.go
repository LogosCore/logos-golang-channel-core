package channel

import (
	"context"
	"testing"
	"time"

	"github.com/logoscore/logos-golang-module-core/pkg/lifecycle"
	"github.com/logoscore/logos-golang-module-core/pkg/module"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

func TestRPCQueue(t *testing.T) {
	if got := RPCQueue("http-1"); got != "logos.channel.rpc.http-1" {
		t.Errorf("RPCQueue = %q", got)
	}
}

func config(id lifecycle.Identity) Config {
	// Port 1 refuses connections, so a valid Config sits in its retry backoff.
	return Config{Config: module.Config{AMQPURL: "amqp://guest:guest@127.0.0.1:1/", Identity: id}}
}

func TestRunRefusesOtherModuleType(t *testing.T) {
	cfg := config(lifecycle.Identity{ModuleType: "minion-factory", ModuleName: "http", Instance: "t"})
	if err := Run(context.Background(), cfg); err == nil {
		t.Fatal("Run accepted module_type minion-factory")
	}
}

func TestRunRefusesInvalidConnection(t *testing.T) {
	cfg := config(lifecycle.Identity{ModuleName: "http", Instance: "t"})
	cfg.Connection = &protocol.ConnectionSchema{SchemaVersion: protocol.ConnectionSchemaV1, Version: 0}
	if err := Run(context.Background(), cfg); err == nil {
		t.Fatal("Run accepted an invalid connection schema")
	}
}

func TestRunDefaultsModuleType(t *testing.T) {
	cfg := config(lifecycle.Identity{ModuleName: "http", Instance: "t"})
	cfg.Connection = &protocol.ConnectionSchema{
		SchemaVersion: protocol.ConnectionSchemaV1,
		Version:       1,
		Fields:        []protocol.FieldSpec{{Key: "base_url", Type: protocol.FieldURL, Required: true}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
