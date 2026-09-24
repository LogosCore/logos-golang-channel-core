package matcher

import (
	"context"
	"testing"
	"time"

	"github.com/logoscore/logos-golang-channel-core/pkg/cache"
	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
)

func p(id int32, enabled bool, hint string) profile.Profile {
	var hintField *profile.MapField
	if hint != "" {
		hintField = &profile.MapField{Target: profile.Target{Location: "body", Key: hint}}
	}
	return profile.Profile{
		ProfileID: id,
		Enabled:   enabled,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			ProfileID:        hintField,
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "in"}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "out"}},
		},
	}
}

func TestResolveHintSingle(t *testing.T) {
	m := New()
	res, err := m.Resolve(context.Background(), "", 2, []profile.Profile{p(1, true, "x"), p(2, true, "p1")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Profile.ProfileID != 2 {
		t.Fatalf("unexpected profile: %d", res.Profile.ProfileID)
	}
	if res.Source != MatchSourceHint {
		t.Fatalf("expected hint source, got %s", res.Source)
	}
}

func TestResolveNoHint(t *testing.T) {
	m := New()
	if _, err := m.Resolve(context.Background(), "", 0, []profile.Profile{p(1, true, "x")}); err == nil {
		t.Fatal("expected error")
	}
}

func TestEnabledOrdered(t *testing.T) {
	m := New()
	out := m.EnabledOrdered([]profile.Profile{p(3, true, "x"), p(1, true, "y"), p(2, false, "z")})
	if len(out) != 2 || out[0].ProfileID != 1 || out[1].ProfileID != 3 {
		t.Fatalf("unexpected order: %+v", out)
	}
}

func TestResolveCacheHit(t *testing.T) {
	aff := cache.NewAffinity(1 * time.Minute)
	aff.Set("src-1", 2)
	m := NewWithCache(aff)

	res, err := m.Resolve(context.Background(), "src-1", 0, []profile.Profile{p(1, true, "x"), p(2, true, "y")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Profile.ProfileID != 2 {
		t.Fatalf("expected cached profile 2, got %d", res.Profile.ProfileID)
	}
	if res.Source != MatchSourceCache {
		t.Fatalf("expected cache source, got %s", res.Source)
	}
}

func TestResolveCacheMissFallsToHint(t *testing.T) {
	aff := cache.NewAffinity(1 * time.Minute)
	m := NewWithCache(aff)

	res, err := m.Resolve(context.Background(), "src-1", 1, []profile.Profile{p(1, true, "x"), p(2, true, "y")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Profile.ProfileID != 1 {
		t.Fatalf("expected profile 1, got %d", res.Profile.ProfileID)
	}
	if res.Source != MatchSourceHint {
		t.Fatalf("expected hint source, got %s", res.Source)
	}
	// Should now be cached
	cached, ok := aff.Get("src-1")
	if !ok || cached != 1 {
		t.Fatalf("expected cache entry for src-1 -> 1, got %d ok=%v", cached, ok)
	}
}

func TestRecordMatch(t *testing.T) {
	aff := cache.NewAffinity(1 * time.Minute)
	m := NewWithCache(aff)
	m.RecordMatch("src-2", 3)

	cached, ok := aff.Get("src-2")
	if !ok || cached != 3 {
		t.Fatalf("expected cache entry for src-2 -> 3, got %d ok=%v", cached, ok)
	}
}
