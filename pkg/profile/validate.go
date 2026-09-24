package profile

import (
	"fmt"
	"strings"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
)

// Validate performs baseline semantic checks for a single profile.
func Validate(p Profile) error {
	if strings.TrimSpace(p.ProfileID) == "" {
		return coreerrors.New(coreerrors.CodeProfileInvalid, "profile_id is required")
	}
	if strings.TrimSpace(p.ChannelType) == "" {
		return coreerrors.New(coreerrors.CodeProfileInvalid, "channel_type is required")
	}
	if strings.TrimSpace(p.Mapping.ID) == "" {
		return coreerrors.New(coreerrors.CodeProfileInvalid, "mapping.id is required")
	}
	if strings.TrimSpace(p.Mapping.EncryptedData) == "" {
		return coreerrors.New(coreerrors.CodeProfileInvalid, "mapping.encrypted_data is required")
	}
	return nil
}

// ValidateMany validates all profiles and annotates list index on failure.
func ValidateMany(profiles []Profile) error {
	for i := range profiles {
		if err := Validate(profiles[i]); err != nil {
			return coreerrors.Wrap(coreerrors.CodeProfileInvalid, fmt.Sprintf("profile[%d] invalid", i), err)
		}
	}
	return nil
}

// ValidateSet validates constraints across all profiles in one channel set.
func ValidateSet(profiles []Profile) error {
	enabledFallbacks := 0
	hintSeen := map[string]string{}
	shapeSeen := map[string]string{}
	enabledNoHint := 0

	for _, p := range profiles {
		if !p.Enabled {
			continue
		}
		if p.DefaultFallback {
			enabledFallbacks++
		}

		hint := strings.ToLower(strings.TrimSpace(p.Mapping.ProfileID))
		if hint != "" {
			if prev, ok := hintSeen[hint]; ok {
				return coreerrors.New(coreerrors.CodeProfileAmbiguous, "overlapping enabled mapping.profile_id between "+prev+" and "+p.ProfileID)
			}
			hintSeen[hint] = p.ProfileID
		} else if !p.DefaultFallback {
			enabledNoHint++
		}

		shape := strings.ToLower(strings.TrimSpace(p.Mapping.ID)) + "|" + strings.ToLower(strings.TrimSpace(p.Mapping.EncryptedData))
		if prev, ok := shapeSeen[shape]; ok {
			return coreerrors.New(coreerrors.CodeProfileAmbiguous, "overlapping enabled mapping shape between "+prev+" and "+p.ProfileID)
		}
		shapeSeen[shape] = p.ProfileID
	}

	if enabledFallbacks != 1 {
		return coreerrors.New(coreerrors.CodeProfileInvalid, "exactly one enabled default_fallback profile is required")
	}
	if enabledNoHint > 0 {
		return coreerrors.New(coreerrors.CodeProfileAmbiguous, "enabled non-fallback profiles require mapping.profile_id hint")
	}
	return nil
}
