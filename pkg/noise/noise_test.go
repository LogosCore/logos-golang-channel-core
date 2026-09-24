package noise

import (
	"strconv"
	"strings"
	"testing"

	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
)

func TestGenerateStatic(t *testing.T) {
	v, err := Generate(profile.ValueGenerator{Type: "static", Value: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if v != "hello" {
		t.Fatalf("expected 'hello', got %q", v)
	}
}

func TestGenerateChoice(t *testing.T) {
	values := []string{"a", "b", "c"}
	v, err := Generate(profile.ValueGenerator{Type: "choice", Values: values})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, opt := range values {
		if v == opt {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("value %q not in choices", v)
	}
}

func TestGenerateUUID(t *testing.T) {
	v, err := Generate(profile.ValueGenerator{Type: "uuid"})
	if err != nil {
		t.Fatal(err)
	}
	// UUID v4 format: 8-4-4-4-12
	if len(v) != 36 || v[8] != '-' || v[13] != '-' || v[18] != '-' || v[23] != '-' {
		t.Fatalf("invalid UUID format: %q", v)
	}
}

func TestGenerateAlphanumeric(t *testing.T) {
	v, err := Generate(profile.ValueGenerator{Type: "alphanumeric", Length: 16})
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 16 {
		t.Fatalf("expected length 16, got %d", len(v))
	}
}

func TestGenerateNumeric(t *testing.T) {
	v, err := Generate(profile.ValueGenerator{Type: "numeric", Length: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 10 {
		t.Fatalf("expected length 10, got %d", len(v))
	}
	for _, c := range v {
		if c < '0' || c > '9' {
			t.Fatalf("non-digit character in numeric output: %c", c)
		}
	}
}

func TestGenerateHex(t *testing.T) {
	v, err := Generate(profile.ValueGenerator{Type: "hex", Length: 32})
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 32 {
		t.Fatalf("expected length 32, got %d", len(v))
	}
}

func TestGenerateTimestampMs(t *testing.T) {
	v, err := Generate(profile.ValueGenerator{Type: "timestamp_ms"})
	if err != nil {
		t.Fatal(err)
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("not a valid int64: %q", v)
	}
	if ms <= 0 {
		t.Fatalf("timestamp should be positive: %d", ms)
	}
}

func TestGenerateRange(t *testing.T) {
	v, err := Generate(profile.ValueGenerator{Type: "range", Min: 10, Max: 20})
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("not a valid int: %q", v)
	}
	if n < 10 || n > 20 {
		t.Fatalf("value %d out of range [10, 20]", n)
	}
}

func TestGeneratePrefixSuffix(t *testing.T) {
	v, err := Generate(profile.ValueGenerator{Type: "static", Value: "val", Prefix: "X-", Suffix: "-end"})
	if err != nil {
		t.Fatal(err)
	}
	if v != "X-val-end" {
		t.Fatalf("expected 'X-val-end', got %q", v)
	}
}

func TestGenerateAllWithCount(t *testing.T) {
	entries := []profile.NoiseEntry{
		{
			Target: profile.NoiseTarget{
				Location: "header",
				Key:      profile.NoiseKey{FixedKey: "X-Cache"},
			},
			Value: profile.ValueGenerator{Type: "choice", Values: []string{"HIT", "MISS"}},
			Count: 1,
		},
		{
			Target: profile.NoiseTarget{
				Location: "header",
				Key:      profile.NoiseKey{Generator: &profile.ValueGenerator{Type: "alphanumeric", Length: 6, Prefix: "X-"}},
			},
			Value: profile.ValueGenerator{Type: "hex", Length: 8},
			Count: 3,
		},
	}
	kvs, err := GenerateAll(entries)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 4 { // 1 + 3
		t.Fatalf("expected 4 key-value pairs, got %d", len(kvs))
	}
	if kvs[0].Key != "X-Cache" {
		t.Fatalf("expected fixed key 'X-Cache', got %q", kvs[0].Key)
	}
	for _, kv := range kvs[1:] {
		if !strings.HasPrefix(kv.Key, "X-") {
			t.Fatalf("expected key with prefix 'X-', got %q", kv.Key)
		}
		if len(kv.Value) != 8 {
			t.Fatalf("expected hex value of length 8, got %q", kv.Value)
		}
	}
}
