package matcher

import (
	"context"
	"strings"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
)

// Matcher contains profile selection helpers.
type Matcher struct{}

func New() *Matcher {
	return &Matcher{}
}

// MatchSource describes where a profile resolution came from.
type MatchSource string

const (
	MatchSourceHint     MatchSource = "hint"
	MatchSourceFallback MatchSource = "fallback"
	MatchSourceNone     MatchSource = "none"
)

// Resolution captures the selected profile and selection source.
type Resolution struct {
	Profile profile.Profile
	Source  MatchSource
}

// Resolve selects a profile from candidates using deterministic hint-first logic.
func (m *Matcher) Resolve(_ context.Context, hintProfileID string, candidates []profile.Profile) (Resolution, error) {
	hintProfileID = strings.TrimSpace(hintProfileID)
	if hintProfileID != "" {
		hits := make([]profile.Profile, 0, 1)
		for _, p := range candidates {
			if !p.Enabled {
				continue
			}
			if p.ProfileID == hintProfileID || p.Mapping.ProfileID == hintProfileID {
				hits = append(hits, p)
			}
		}
		switch len(hits) {
		case 1:
			return Resolution{Profile: hits[0], Source: MatchSourceHint}, nil
		case 0:
			// Continue to fallback resolution.
		default:
			return Resolution{Source: MatchSourceNone}, coreerrors.New(coreerrors.CodeProfileAmbiguous, "hint matches multiple enabled profiles")
		}
	}

	selected, found := selectFallback(candidates)
	if !found {
		return Resolution{Source: MatchSourceNone}, coreerrors.New(coreerrors.CodeProfileNotFound, "no enabled fallback profile found")
	}
	return Resolution{Profile: selected, Source: MatchSourceFallback}, nil
}

// SelectHintFirst performs hint-first selection and falls back to the
// highest-priority enabled default profile.
func (m *Matcher) SelectHintFirst(ctx context.Context, _ string, hintProfileID string, candidates []profile.Profile) (profile.Profile, bool, error) {
	resolution, err := m.Resolve(ctx, hintProfileID, candidates)
	if err != nil {
		if coreerrors.Code(err) == coreerrors.CodeProfileNotFound {
			return profile.Profile{}, false, nil
		}
		return profile.Profile{}, false, err
	}
	return resolution.Profile, true, nil
}

func selectFallback(candidates []profile.Profile) (profile.Profile, bool) {
	var selected profile.Profile
	found := false
	for _, p := range candidates {
		if !p.Enabled || !p.DefaultFallback {
			continue
		}
		if !found || betterFallback(p, selected) {
			selected = p
			found = true
		}
	}
	return selected, found
}

func betterFallback(candidate, current profile.Profile) bool {
	if candidate.Priority != current.Priority {
		return candidate.Priority > current.Priority
	}
	if candidate.ProfileID != current.ProfileID {
		return candidate.ProfileID < current.ProfileID
	}
	return candidate.Mapping.ProfileID < current.Mapping.ProfileID
}
