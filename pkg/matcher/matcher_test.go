package matcher

import (
	"context"
	"testing"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
)

func TestResolveHintSingleMatch(t *testing.T) {
	m := New()
	candidates := []profile.Profile{
		{
			ProfileID:   "alpha",
			Enabled:     true,
			ChannelType: "http",
			Mapping: profile.Mapping{
				ProfileID:     "hint-alpha",
				ID:            "id",
				EncryptedData: "encrypted_data",
			},
		},
		{
			ProfileID:       "beta",
			Enabled:         true,
			DefaultFallback: true,
			Priority:        10,
			ChannelType:     "http",
			Mapping: profile.Mapping{
				ID:            "id",
				EncryptedData: "encrypted_data",
			},
		},
	}

	resolution, err := m.Resolve(context.Background(), "hint-alpha", candidates)
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if resolution.Source != MatchSourceHint {
		t.Fatalf("expected hint source, got %q", resolution.Source)
	}
	if resolution.Profile.ProfileID != "alpha" {
		t.Fatalf("expected alpha profile, got %q", resolution.Profile.ProfileID)
	}
}

func TestResolveHintAmbiguous(t *testing.T) {
	m := New()
	candidates := []profile.Profile{
		{
			ProfileID:   "alpha",
			Enabled:     true,
			ChannelType: "http",
			Mapping: profile.Mapping{
				ID:            "id",
				EncryptedData: "encrypted_data",
			},
		},
		{
			ProfileID:   "beta",
			Enabled:     true,
			ChannelType: "http",
			Mapping: profile.Mapping{
				ProfileID:     "alpha",
				ID:            "id",
				EncryptedData: "encrypted_data",
			},
		},
	}

	_, err := m.Resolve(context.Background(), "alpha", candidates)
	if err == nil {
		t.Fatal("expected ambiguous match error")
	}
	if code := coreerrors.Code(err); code != coreerrors.CodeProfileAmbiguous {
		t.Fatalf("unexpected error code: %s", code)
	}
}

func TestResolveFallbackSelected(t *testing.T) {
	m := New()
	candidates := []profile.Profile{
		{
			ProfileID:       "alpha",
			Enabled:         true,
			DefaultFallback: true,
			Priority:        1,
			ChannelType:     "http",
			Mapping: profile.Mapping{
				ID:            "id",
				EncryptedData: "encrypted_data",
			},
		},
		{
			ProfileID:       "beta",
			Enabled:         true,
			DefaultFallback: true,
			Priority:        9,
			ChannelType:     "http",
			Mapping: profile.Mapping{
				ID:            "id",
				EncryptedData: "encrypted_data",
			},
		},
	}

	resolution, err := m.Resolve(context.Background(), "missing-hint", candidates)
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if resolution.Source != MatchSourceFallback {
		t.Fatalf("expected fallback source, got %q", resolution.Source)
	}
	if resolution.Profile.ProfileID != "beta" {
		t.Fatalf("expected beta fallback profile, got %q", resolution.Profile.ProfileID)
	}
}

func TestResolveNoFallback(t *testing.T) {
	m := New()
	candidates := []profile.Profile{
		{
			ProfileID:   "alpha",
			Enabled:     true,
			ChannelType: "http",
			Mapping: profile.Mapping{
				ID:            "id",
				EncryptedData: "encrypted_data",
			},
		},
	}

	_, err := m.Resolve(context.Background(), "", candidates)
	if err == nil {
		t.Fatal("expected not found error")
	}
	if code := coreerrors.Code(err); code != coreerrors.CodeProfileNotFound {
		t.Fatalf("unexpected error code: %s", code)
	}
}
