package mgmtrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
)

type memStore struct {
	data map[string]map[int32]profile.Profile
}

func newMemStore() *memStore { return &memStore{data: map[string]map[int32]profile.Profile{}} }

func (m *memStore) List(_ context.Context, channelID string) ([]profile.Profile, error) {
	ch := m.data[channelID]
	out := make([]profile.Profile, 0, len(ch))
	for _, p := range ch {
		out = append(out, p)
	}
	return out, nil
}
func (m *memStore) Get(_ context.Context, channelID string, profileID int32) (profile.Profile, error) {
	if ch, ok := m.data[channelID]; ok {
		if p, ok := ch[profileID]; ok {
			return p, nil
		}
	}
	return profile.Profile{}, errors.New("not found")
}
func (m *memStore) Put(_ context.Context, channelID string, p profile.Profile) error {
	if _, ok := m.data[channelID]; !ok {
		m.data[channelID] = map[int32]profile.Profile{}
	}
	m.data[channelID][p.ProfileID] = p
	return nil
}
func (m *memStore) Delete(_ context.Context, channelID string, profileID int32) error {
	if _, ok := m.data[channelID]; ok {
		delete(m.data[channelID], profileID)
	}
	return nil
}

func TestCreateAndValidate(t *testing.T) {
	s := NewServer(newMemStore())
	ctx := context.Background()
	p := profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "data_in"}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "data_out"}},
		},
	}
	if err := s.CreateProfile(ctx, "c1", p); err != nil {
		t.Fatal(err)
	}
}

func TestDeactivateProfile(t *testing.T) {
	store := newMemStore()
	s := NewServer(store)
	ctx := context.Background()
	p1 := profile.Profile{
		ProfileID: 1, Enabled: true, Action: profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "in"}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "out"}},
		},
	}
	p2 := profile.Profile{
		ProfileID: 2, Enabled: true, Action: profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "id2"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "in2"}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "out2"}},
		},
	}
	if err := s.CreateProfile(ctx, "c1", p1); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateProfile(ctx, "c1", p2); err != nil {
		t.Fatal(err)
	}
	if err := s.DeactivateProfile(ctx, "c1", 1); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadProfile(ctx, "c1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Enabled {
		t.Fatal("expected profile 1 to be disabled")
	}
}

func TestDeactivateNotFound(t *testing.T) {
	s := NewServer(newMemStore())
	if err := s.DeactivateProfile(context.Background(), "c1", 999); err == nil {
		t.Fatal("expected error")
	}
}

func TestActivateNotFound(t *testing.T) {
	s := NewServer(newMemStore())
	if err := s.ActivateProfile(context.Background(), "c1", 999); err == nil {
		t.Fatal("expected error")
	}
}

func TestListProfiles(t *testing.T) {
	store := newMemStore()
	s := NewServer(store)
	ctx := context.Background()
	p := profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "data_in"}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "data_out"}},
		},
	}
	if err := s.CreateProfile(ctx, "c1", p); err != nil {
		t.Fatal(err)
	}
	profiles, err := s.ListProfiles(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 1 {
		t.Fatalf("expected 1 profile, got %d", len(profiles))
	}
}

func TestSimulateMatch(t *testing.T) {
	store := newMemStore()
	s := NewServer(store)
	ctx := context.Background()
	p := profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "data_in"}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "data_out"}},
		},
	}
	if err := s.CreateProfile(ctx, "c1", p); err != nil {
		t.Fatal(err)
	}
	matched, err := s.SimulateMatch(ctx, "c1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if matched.ProfileID != 1 {
		t.Fatalf("expected profile 1, got %d", matched.ProfileID)
	}
	if _, err := s.SimulateMatch(ctx, "c1", 999); err == nil {
		t.Fatal("expected error for non-existent profile")
	}
}
