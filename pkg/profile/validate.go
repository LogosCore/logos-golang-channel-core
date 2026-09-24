package profile

import (
	"fmt"
	"strings"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
)

// Validate performs baseline semantic checks for a single profile.
func Validate(p Profile) error {
	if p.ProfileID <= 0 {
		return coreerrors.New(coreerrors.CodeProfileInvalid, "profile_id must be a positive integer")
	}
	if strings.TrimSpace(p.Action.Type) == "" {
		return coreerrors.New(coreerrors.CodeProfileInvalid, "action.type is required")
	}
	if p.Mapping.CompositeIn == nil {
		if strings.TrimSpace(p.Mapping.ID.Ref()) == "" {
			return coreerrors.New(coreerrors.CodeProfileInvalid, "mapping.id target is required (or use mapping.composite_in)")
		}
		if strings.TrimSpace(p.Mapping.EncryptedDataIn.Ref()) == "" {
			return coreerrors.New(coreerrors.CodeProfileInvalid, "mapping.encrypted_data_in target is required (or use mapping.composite_in)")
		}
	} else {
		if strings.TrimSpace(p.Mapping.CompositeIn.Ref()) == "" {
			return coreerrors.New(coreerrors.CodeProfileInvalid, "mapping.composite_in target is required")
		}
		if err := validateSeparator(p.Mapping.CompositeIn.Separator); err != nil {
			return err
		}
	}
	if strings.TrimSpace(p.Mapping.EncryptedDataOut.Ref()) == "" {
		return coreerrors.New(coreerrors.CodeProfileInvalid, "mapping.encrypted_data_out target is required")
	}
	if p.Noise != nil {
		if err := validateNoiseKeys(p); err != nil {
			return err
		}
	}
	return nil
}

// validateNoiseKeys checks that fixed noise keys do not collide with mapping keys in the same location.
func validateNoiseKeys(p Profile) error {
	mappingKeys := map[string]bool{}
	addMappingKey := func(f MapField) {
		loc := strings.TrimSpace(strings.ToLower(f.Target.Location))
		key := strings.TrimSpace(strings.ToLower(f.Target.Key))
		if loc != "" && key != "" {
			mappingKeys[loc+":"+key] = true
		}
	}
	addMappingKey(p.Mapping.ID)
	addMappingKey(p.Mapping.EncryptedDataIn)
	addMappingKey(p.Mapping.EncryptedDataOut)
	if p.Mapping.ProfileID != nil {
		addMappingKey(*p.Mapping.ProfileID)
	}
	if p.Mapping.CompositeIn != nil {
		loc := strings.TrimSpace(strings.ToLower(p.Mapping.CompositeIn.Target.Location))
		key := strings.TrimSpace(strings.ToLower(p.Mapping.CompositeIn.Target.Key))
		if loc != "" && key != "" {
			mappingKeys[loc+":"+key] = true
		}
	}

	checkEntries := func(entries []NoiseEntry, direction string) error {
		for _, ne := range entries {
			if ne.Target.Key.FixedKey == "" {
				continue // random key — cannot collide at parse time
			}
			loc := strings.TrimSpace(strings.ToLower(ne.Target.Location))
			key := strings.TrimSpace(strings.ToLower(ne.Target.Key.FixedKey))
			if mappingKeys[loc+":"+key] {
				return coreerrors.New(coreerrors.CodeProfileInvalid,
					fmt.Sprintf("noise %s key %q collides with mapping key in location %q", direction, ne.Target.Key.FixedKey, ne.Target.Location))
			}
		}
		return nil
	}

	if err := checkEntries(p.Noise.Inbound, "inbound"); err != nil {
		return err
	}
	return checkEntries(p.Noise.Outbound, "outbound")
}

func validateSeparator(sep Separator) error {
	switch sep.Type {
	case "length_prefix":
		if sep.IDLength <= 0 {
			return coreerrors.New(coreerrors.CodeProfileInvalid, "composite_in.separator.id_length must be > 0 for length_prefix")
		}
	case "delimiter":
		if sep.Value == "" {
			return coreerrors.New(coreerrors.CodeProfileInvalid, "composite_in.separator.value is required for delimiter")
		}
	default:
		return coreerrors.New(coreerrors.CodeProfileInvalid, "composite_in.separator.type must be 'length_prefix' or 'delimiter'")
	}
	return nil
}

func ValidateMany(profiles []Profile) error {
	for i := range profiles {
		if err := Validate(profiles[i]); err != nil {
			return coreerrors.Wrap(coreerrors.CodeProfileInvalid, fmt.Sprintf("profile[%d] invalid", i), err)
		}
	}
	return nil
}

// ValidateSet validates constraints across all enabled profiles in one channel set.
func ValidateSet(profiles []Profile) error {
	hintSeen := map[string]int32{}
	shapeSeen := map[string]int32{}
	enabledCount := 0

	for _, p := range profiles {
		if !p.Enabled {
			continue
		}
		enabledCount++

		if p.Mapping.ProfileID != nil {
			hint := strings.ToLower(strings.TrimSpace(p.Mapping.ProfileID.Ref()))
			if hint != "" {
				if prev, ok := hintSeen[hint]; ok {
					return coreerrors.New(coreerrors.CodeProfileAmbiguous, fmt.Sprintf("overlapping enabled mapping.profile_id between %d and %d", prev, p.ProfileID))
				}
				hintSeen[hint] = p.ProfileID
			}
		}

		shape := strings.ToLower(strings.TrimSpace(p.Mapping.ID.Ref())) + "|" + strings.ToLower(strings.TrimSpace(p.Mapping.EncryptedDataIn.Ref()))
		if p.Mapping.CompositeIn != nil {
			shape = "composite:" + strings.ToLower(strings.TrimSpace(p.Mapping.CompositeIn.Ref()))
		}
		if prev, ok := shapeSeen[shape]; ok {
			return coreerrors.New(coreerrors.CodeProfileAmbiguous, fmt.Sprintf("overlapping enabled mapping shape between %d and %d", prev, p.ProfileID))
		}
		shapeSeen[shape] = p.ProfileID
	}

	if enabledCount == 0 {
		return coreerrors.New(coreerrors.CodeProfileInvalid, "at least one enabled profile is required")
	}
	return nil
}
