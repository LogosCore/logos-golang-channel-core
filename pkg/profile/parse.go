package profile

import (
	"fmt"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	"gopkg.in/yaml.v3"
)

// ParseYAML parses one YAML profile and validates semantic requirements.
func ParseYAML(data []byte) (Profile, error) {
	profiles, err := ParseYAMLProfiles(data)
	if err != nil {
		return Profile{}, err
	}
	if len(profiles) != 1 {
		return Profile{}, coreerrors.New(coreerrors.CodeProfileInvalid, "expected exactly one profile")
	}
	return profiles[0], nil
}

// ParseYAMLProfiles parses one or many profiles from YAML.
// Supported shapes:
//  1. single profile object
//  2. array of profile objects
//  3. wrapper object with `profiles: []`
func ParseYAMLProfiles(data []byte) ([]Profile, error) {
	if len(data) == 0 {
		return nil, coreerrors.New(coreerrors.CodeProfileInvalid, "profile YAML is empty")
	}

	type wrapper struct {
		Profiles []Profile `yaml:"profiles"`
	}

	var w wrapper
	if err := yaml.Unmarshal(data, &w); err == nil && len(w.Profiles) > 0 {
		if err := ValidateMany(w.Profiles); err != nil {
			return nil, err
		}
		if err := ValidateSet(w.Profiles); err != nil {
			return nil, err
		}
		return w.Profiles, nil
	}

	var arr []Profile
	if err := yaml.Unmarshal(data, &arr); err == nil && len(arr) > 0 {
		if err := ValidateMany(arr); err != nil {
			return nil, err
		}
		if err := ValidateSet(arr); err != nil {
			return nil, err
		}
		return arr, nil
	}

	var one Profile
	if err := yaml.Unmarshal(data, &one); err != nil {
		return nil, coreerrors.Wrap(coreerrors.CodeProfileInvalid, "unable to parse profile YAML", err)
	}
	if err := Validate(one); err != nil {
		return nil, coreerrors.Wrap(coreerrors.CodeProfileInvalid, fmt.Sprintf("invalid profile %q", one.ProfileID), err)
	}
	return []Profile{one}, nil
}
