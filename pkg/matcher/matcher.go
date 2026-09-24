package matcher

import (
	"context"
	"sort"

	"github.com/logoscore/logos-golang-channel-core/pkg/cache"
	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
)

type MatchSource string

const (
	MatchSourceHint       MatchSource = "hint"
	MatchSourceCache      MatchSource = "cache"
	MatchSourceBruteforce MatchSource = "bruteforce"
	MatchSourceNone       MatchSource = "none"
)

type Resolution struct {
	Profile profile.Profile
	Source  MatchSource
}

type Matcher struct {
	affinity *cache.Affinity
}

func New() *Matcher { return &Matcher{} }

// NewWithCache creates a matcher that uses source affinity caching.
func NewWithCache(affinity *cache.Affinity) *Matcher {
	return &Matcher{affinity: affinity}
}

// Resolve resolves a profile by cache, then hint, falling back to brute-force by channel module.
// sourceKey is used for cache lookups (e.g., IP address, session ID).
func (m *Matcher) Resolve(_ context.Context, sourceKey string, hintProfileID int32, candidates []profile.Profile) (Resolution, error) {
	enabled := enabledMap(candidates)

	// 1. Try cache
	if m.affinity != nil && sourceKey != "" {
		if cachedID, ok := m.affinity.Get(sourceKey); ok {
			if p, ok := enabled[cachedID]; ok {
				return Resolution{Profile: p, Source: MatchSourceCache}, nil
			}
		}
	}

	// 2. Try hint
	if hintProfileID > 0 {
		if p, ok := enabled[hintProfileID]; ok {
			if m.affinity != nil && sourceKey != "" {
				m.affinity.Set(sourceKey, p.ProfileID)
			}
			return Resolution{Profile: p, Source: MatchSourceHint}, nil
		}
		return Resolution{Source: MatchSourceNone}, coreerrors.New(coreerrors.CodeProfileNotFound, "hint did not match enabled profiles")
	}

	return Resolution{Source: MatchSourceNone}, coreerrors.New(coreerrors.CodeProfileNotFound, "no hint provided and no cache hit")
}

// RecordMatch updates the cache after a successful match (e.g., after brute-force by channel module).
func (m *Matcher) RecordMatch(sourceKey string, profileID int32) {
	if m.affinity != nil && sourceKey != "" {
		m.affinity.Set(sourceKey, profileID)
	}
}

// EnabledOrdered returns enabled profiles ordered by profile_id asc.
func (m *Matcher) EnabledOrdered(candidates []profile.Profile) []profile.Profile {
	out := make([]profile.Profile, 0, len(candidates))
	for _, p := range candidates {
		if p.Enabled {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ProfileID < out[j].ProfileID
	})
	return out
}

func enabledMap(candidates []profile.Profile) map[int32]profile.Profile {
	m := make(map[int32]profile.Profile, len(candidates))
	for _, p := range candidates {
		if p.Enabled {
			m[p.ProfileID] = p
		}
	}
	return m
}
