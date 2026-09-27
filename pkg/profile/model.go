package profile

import (
	"encoding/json"
	"strings"

	"github.com/logoscore/logos-golang-channel-core/pkg/transform"
)

func marshalJSON(v any) ([]byte, error) { return json.Marshal(v) }

// Profile defines one transposition profile for a channel.
type Profile struct {
	ProfileID    int32   `json:"profile_id" yaml:"profile_id"`
	ProfileLabel string  `json:"profile_label,omitempty" yaml:"profile_label,omitempty"`
	Enabled      bool    `json:"enabled" yaml:"enabled"`
	Action       Action  `json:"action" yaml:"action"`
	Mapping      Mapping `json:"mapping" yaml:"mapping"`
	Noise        *Noise  `json:"noise,omitempty" yaml:"noise,omitempty"`
}

// Noise defines decoy fields injected into transport for traffic blending.
type Noise struct {
	Inbound  []NoiseEntry `json:"inbound,omitempty" yaml:"inbound,omitempty"`
	Outbound []NoiseEntry `json:"outbound,omitempty" yaml:"outbound,omitempty"`
}

// NoiseEntry defines a single noise field pattern.
type NoiseEntry struct {
	Target NoiseTarget    `json:"target" yaml:"target"`
	Value  ValueGenerator `json:"value" yaml:"value"`
	Count  int            `json:"count,omitempty" yaml:"count,omitempty"`
}

// NoiseTarget specifies where a noise field is placed.
type NoiseTarget struct {
	Location string   `json:"location" yaml:"location"`
	Key      NoiseKey `json:"key" yaml:"key"`
}

// NoiseKey can be a fixed string or a generator for random keys.
// When YAML is a plain string, FixedKey is populated.
// When YAML is an object, Generator is populated.
type NoiseKey struct {
	FixedKey  string          `json:"-" yaml:"-"`
	Generator *ValueGenerator `json:"-" yaml:"-"`
}

// UnmarshalYAML implements custom YAML parsing for NoiseKey (string or object).
func (k *NoiseKey) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err == nil {
		k.FixedKey = s
		return nil
	}
	var gen ValueGenerator
	if err := unmarshal(&gen); err != nil {
		return err
	}
	k.Generator = &gen
	return nil
}

// MarshalJSON implements JSON marshaling for NoiseKey.
func (k NoiseKey) MarshalJSON() ([]byte, error) {
	if k.Generator != nil {
		return marshalJSON(k.Generator)
	}
	return marshalJSON(k.FixedKey)
}

// ValueGenerator defines how to produce a noise value or key.
type ValueGenerator struct {
	Type   string   `json:"type" yaml:"type"`
	Value  string   `json:"value,omitempty" yaml:"value,omitempty"`
	Values []string `json:"values,omitempty" yaml:"values,omitempty"`
	Length int      `json:"length,omitempty" yaml:"length,omitempty"`
	Min    int      `json:"min,omitempty" yaml:"min,omitempty"`
	Max    int      `json:"max,omitempty" yaml:"max,omitempty"`
	Prefix string   `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Suffix string   `json:"suffix,omitempty" yaml:"suffix,omitempty"`
}

// Action defines what the channel does after profile matching and decoding.
type Action struct {
	Type   string         `json:"type" yaml:"type"`
	Params map[string]any `json:"params,omitempty" yaml:"params,omitempty"`
}

type Mapping struct {
	ProfileID        *MapField       `json:"profile_id,omitempty" yaml:"profile_id,omitempty"`
	ID               MapField        `json:"id,omitempty" yaml:"id,omitempty"`
	EncryptedDataIn  MapField        `json:"encrypted_data_in,omitempty" yaml:"encrypted_data_in,omitempty"`
	EncryptedDataOut MapField        `json:"encrypted_data_out,omitempty" yaml:"encrypted_data_out,omitempty"`
	CompositeIn      *CompositeField `json:"composite_in,omitempty" yaml:"composite_in,omitempty"`
}

type MapField struct {
	Target    Target           `json:"target" yaml:"target"`
	Transform []transform.Spec `json:"transform,omitempty" yaml:"transform,omitempty"`
}

type Target struct {
	Location string `json:"location" yaml:"location"`
	Key      string `json:"key,omitempty" yaml:"key,omitempty"`
}

// CompositeField maps a single transport value carrying both id and encrypted_data.
type CompositeField struct {
	Target    Target           `json:"target" yaml:"target"`
	Transform []transform.Spec `json:"transform,omitempty" yaml:"transform,omitempty"`
	Separator Separator        `json:"separator" yaml:"separator"`
}

// Separator defines how composite_in splits the combined value into id + encrypted_data.
type Separator struct {
	Type     string `json:"type" yaml:"type"`                               // "length_prefix" or "delimiter"
	IDLength int    `json:"id_length,omitempty" yaml:"id_length,omitempty"` // required for length_prefix
	Value    string `json:"value,omitempty" yaml:"value,omitempty"`         // required for delimiter
}

func (c CompositeField) Ref() string {
	loc := strings.TrimSpace(c.Target.Location)
	key := strings.TrimSpace(c.Target.Key)
	if loc == "" {
		return key
	}
	return loc + ":" + key
}

func (m MapField) Ref() string {
	loc := strings.TrimSpace(m.Target.Location)
	key := strings.TrimSpace(m.Target.Key)
	if loc == "" {
		return key
	}
	return loc + ":" + key
}
