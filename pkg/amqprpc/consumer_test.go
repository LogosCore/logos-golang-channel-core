package amqprpc

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/logoscore/logos-golang-channel-core/pkg/mgmtrpc"
	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
)

// memStore implements runtime.ProfileStore for testing.
type memStore struct {
	mu   sync.Mutex
	data map[string]map[int32]profile.Profile
}

func newMemStore() *memStore { return &memStore{data: map[string]map[int32]profile.Profile{}} }

func (m *memStore) List(_ context.Context, channelID string) ([]profile.Profile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := m.data[channelID]
	out := make([]profile.Profile, 0, len(ch))
	for _, p := range ch {
		out = append(out, p)
	}
	return out, nil
}
func (m *memStore) Get(_ context.Context, channelID string, profileID int32) (profile.Profile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.data[channelID]; ok {
		if p, ok := ch[profileID]; ok {
			return p, nil
		}
	}
	return profile.Profile{}, errors.New("not found")
}
func (m *memStore) Put(_ context.Context, channelID string, p profile.Profile) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[channelID]; !ok {
		m.data[channelID] = map[int32]profile.Profile{}
	}
	m.data[channelID][p.ProfileID] = p
	return nil
}
func (m *memStore) Delete(_ context.Context, channelID string, profileID int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[channelID]; ok {
		delete(m.data[channelID], profileID)
	}
	return nil
}

// mockChannel implements amqpChannel for testing.
type mockChannel struct {
	deliveries chan amqp.Delivery
	published  []amqp.Publishing
	mu         sync.Mutex
}

func newMockChannel() *mockChannel {
	return &mockChannel{
		deliveries: make(chan amqp.Delivery, 10),
	}
}

func (m *mockChannel) QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{}, nil
}
func (m *mockChannel) Qos(int, int, bool) error { return nil }
func (m *mockChannel) ConsumeWithContext(_ context.Context, _, _ string, _, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	return m.deliveries, nil
}
func (m *mockChannel) PublishWithContext(_ context.Context, _, _ string, _, _ bool, msg amqp.Publishing) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, msg)
	return nil
}
func (m *mockChannel) Close() error { return nil }

func (m *mockChannel) getPublished() []amqp.Publishing {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]amqp.Publishing, len(m.published))
	copy(out, m.published)
	return out
}

func testProfile() profile.Profile {
	return profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "in"}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "out"}},
		},
	}
}

type noopAck struct{}

func (n *noopAck) Ack(uint64, bool) error   { return nil }
func (n *noopAck) Nack(uint64, bool, bool) error { return nil }
func (n *noopAck) Reject(uint64, bool) error { return nil }

// runConsumerOnce runs the consumer loop, sends one delivery, and cancels context.
func runConsumerOnce(t *testing.T, server *mgmtrpc.Server, method string, params any) response {
	t.Helper()
	mock := newMockChannel()
	ctx, cancel := context.WithCancel(context.Background())

	paramBytes, _ := json.Marshal(params)
	body, _ := json.Marshal(request{Method: method, Params: paramBytes})
	mock.deliveries <- amqp.Delivery{
		Body:          body,
		ReplyTo:       "reply-queue",
		CorrelationId: "corr-1",
		Acknowledger:  &noopAck{},
	}

	done := make(chan error, 1)
	go func() {
		done <- consumeLoop(ctx, mock, "test-channel", server)
	}()

	// Close deliveries to stop the loop after processing
	close(mock.deliveries)
	<-done
	cancel()

	published := mock.getPublished()
	if len(published) == 0 {
		t.Fatal("expected a published response")
	}
	var resp response
	if err := json.Unmarshal(published[0].Body, &resp); err != nil {
		t.Fatal(err)
	}
	if published[0].CorrelationId != "corr-1" {
		t.Fatalf("expected correlation id 'corr-1', got %q", published[0].CorrelationId)
	}
	return resp
}

func TestQueueName(t *testing.T) {
	if got := QueueName("http-1"); got != "channel.profiles.http-1" {
		t.Fatalf("expected 'channel.profiles.http-1', got %q", got)
	}
}

func TestCreateProfile(t *testing.T) {
	server := mgmtrpc.NewServer(newMemStore())
	resp := runConsumerOnce(t, server, "profile.create", testProfile())
	if !resp.OK {
		t.Fatalf("expected ok, got error: %+v", resp.Error)
	}
}

func TestReadProfile(t *testing.T) {
	store := newMemStore()
	server := mgmtrpc.NewServer(store)
	ctx := context.Background()
	if err := server.CreateProfile(ctx, "test-channel", testProfile()); err != nil {
		t.Fatal(err)
	}
	resp := runConsumerOnce(t, server, "profile.read", profileIDParam{ProfileID: 1})
	if !resp.OK {
		t.Fatalf("expected ok, got error: %+v", resp.Error)
	}
	// Verify data contains the profile
	data, _ := json.Marshal(resp.Data)
	var p profile.Profile
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatal(err)
	}
	if p.ProfileID != 1 {
		t.Fatalf("expected profile_id 1, got %d", p.ProfileID)
	}
}

func TestDeleteProfile(t *testing.T) {
	store := newMemStore()
	server := mgmtrpc.NewServer(store)
	ctx := context.Background()
	// Need 2 profiles so deletion doesn't break the "at least 1 enabled" constraint
	p1 := testProfile()
	p2 := testProfile()
	p2.ProfileID = 2
	p2.Mapping.ID = profile.MapField{Target: profile.Target{Location: "body", Key: "id2"}}
	p2.Mapping.EncryptedDataIn = profile.MapField{Target: profile.Target{Location: "body", Key: "in2"}}
	p2.Mapping.EncryptedDataOut = profile.MapField{Target: profile.Target{Location: "body", Key: "out2"}}
	if err := server.CreateProfile(ctx, "test-channel", p1); err != nil {
		t.Fatal(err)
	}
	if err := server.CreateProfile(ctx, "test-channel", p2); err != nil {
		t.Fatal(err)
	}
	resp := runConsumerOnce(t, server, "profile.delete", profileIDParam{ProfileID: 1})
	if !resp.OK {
		t.Fatalf("expected ok, got error: %+v", resp.Error)
	}
}

func TestListProfiles(t *testing.T) {
	store := newMemStore()
	server := mgmtrpc.NewServer(store)
	ctx := context.Background()
	if err := server.CreateProfile(ctx, "test-channel", testProfile()); err != nil {
		t.Fatal(err)
	}
	resp := runConsumerOnce(t, server, "profile.list", nil)
	if !resp.OK {
		t.Fatalf("expected ok, got error: %+v", resp.Error)
	}
}

func TestDeactivateProfile(t *testing.T) {
	store := newMemStore()
	server := mgmtrpc.NewServer(store)
	ctx := context.Background()
	p1 := testProfile()
	p2 := testProfile()
	p2.ProfileID = 2
	p2.Mapping.ID = profile.MapField{Target: profile.Target{Location: "body", Key: "id2"}}
	p2.Mapping.EncryptedDataIn = profile.MapField{Target: profile.Target{Location: "body", Key: "in2"}}
	p2.Mapping.EncryptedDataOut = profile.MapField{Target: profile.Target{Location: "body", Key: "out2"}}
	if err := server.CreateProfile(ctx, "test-channel", p1); err != nil {
		t.Fatal(err)
	}
	if err := server.CreateProfile(ctx, "test-channel", p2); err != nil {
		t.Fatal(err)
	}
	resp := runConsumerOnce(t, server, "profile.deactivate", profileIDParam{ProfileID: 1})
	if !resp.OK {
		t.Fatalf("expected ok, got error: %+v", resp.Error)
	}
}

func TestUnknownMethod(t *testing.T) {
	server := mgmtrpc.NewServer(newMemStore())
	resp := runConsumerOnce(t, server, "profile.unknown", nil)
	if resp.OK {
		t.Fatal("expected error for unknown method")
	}
	if resp.Error.Code != "ERR_NOT_IMPLEMENTED" {
		t.Fatalf("expected ERR_NOT_IMPLEMENTED, got %q", resp.Error.Code)
	}
}

func TestMalformedJSON(t *testing.T) {
	mock := newMockChannel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock.deliveries <- amqp.Delivery{
		Body:          []byte(`{invalid json`),
		ReplyTo:       "reply-queue",
		CorrelationId: "corr-1",
		Acknowledger:  &noopAck{},
	}
	close(mock.deliveries)

	server := mgmtrpc.NewServer(newMemStore())
	done := make(chan error, 1)
	go func() {
		done <- consumeLoop(ctx, mock, "test-channel", server)
	}()
	<-done

	published := mock.getPublished()
	if len(published) == 0 {
		t.Fatal("expected a response for malformed JSON")
	}
	var resp response
	if err := json.Unmarshal(published[0].Body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.OK {
		t.Fatal("expected error response")
	}
	if resp.Error.Code != "ERR_INVALID_INPUT" {
		t.Fatalf("expected ERR_INVALID_INPUT, got %q", resp.Error.Code)
	}
}
