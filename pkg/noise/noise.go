package noise

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
)

// KeyValue holds a generated noise key-value pair.
type KeyValue struct {
	Location string
	Key      string
	Value    string
}

// GenerateAll produces all noise key-value pairs for a list of noise entries.
func GenerateAll(entries []profile.NoiseEntry) ([]KeyValue, error) {
	var results []KeyValue
	for _, entry := range entries {
		count := entry.Count
		if count <= 0 {
			count = 1
		}
		for range count {
			key, err := generateKey(entry.Target)
			if err != nil {
				return nil, fmt.Errorf("noise key generation failed: %w", err)
			}
			val, err := Generate(entry.Value)
			if err != nil {
				return nil, fmt.Errorf("noise value generation failed: %w", err)
			}
			results = append(results, KeyValue{
				Location: entry.Target.Location,
				Key:      key,
				Value:    val,
			})
		}
	}
	return results, nil
}

func generateKey(target profile.NoiseTarget) (string, error) {
	if target.Key.Generator != nil {
		return Generate(*target.Key.Generator)
	}
	return target.Key.FixedKey, nil
}

// Generate produces a single value from a ValueGenerator spec.
func Generate(gen profile.ValueGenerator) (string, error) {
	var raw string
	var err error

	switch gen.Type {
	case "static":
		raw = gen.Value
	case "choice":
		raw, err = generateChoice(gen.Values)
	case "uuid":
		raw, err = generateUUID()
	case "alphanumeric":
		raw, err = generateAlphanumeric(gen.Length)
	case "numeric":
		raw, err = generateNumeric(gen.Length)
	case "hex":
		raw, err = generateHex(gen.Length)
	case "timestamp_ms":
		raw = strconv.FormatInt(time.Now().UnixMilli(), 10)
	case "range":
		raw, err = generateRange(gen.Min, gen.Max)
	default:
		return "", fmt.Errorf("unsupported noise generator type: %q", gen.Type)
	}
	if err != nil {
		return "", err
	}

	return gen.Prefix + raw + gen.Suffix, nil
}

func generateChoice(values []string) (string, error) {
	if len(values) == 0 {
		return "", fmt.Errorf("choice generator requires non-empty values list")
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(values))))
	if err != nil {
		return "", err
	}
	return values[n.Int64()], nil
}

func generateUUID() (string, error) {
	var uuid [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		return "", err
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}

const alphanumChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateAlphanumeric(length int) (string, error) {
	if length <= 0 {
		return "", fmt.Errorf("alphanumeric generator requires length > 0")
	}
	b := make([]byte, length)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphanumChars))))
		if err != nil {
			return "", err
		}
		b[i] = alphanumChars[n.Int64()]
	}
	return string(b), nil
}

func generateNumeric(length int) (string, error) {
	if length <= 0 {
		return "", fmt.Errorf("numeric generator requires length > 0")
	}
	b := make([]byte, length)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(10))
		if err != nil {
			return "", err
		}
		b[i] = '0' + byte(n.Int64())
	}
	return string(b), nil
}

func generateHex(length int) (string, error) {
	if length <= 0 {
		return "", fmt.Errorf("hex generator requires length > 0")
	}
	byteLen := (length + 1) / 2
	b := make([]byte, byteLen)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b)[:length], nil
}

func generateRange(min, max int) (string, error) {
	if min >= max {
		return "", fmt.Errorf("range generator requires min < max")
	}
	span := int64(max - min + 1)
	n, err := rand.Int(rand.Reader, big.NewInt(span))
	if err != nil {
		return "", err
	}
	return strconv.Itoa(min + int(n.Int64())), nil
}
