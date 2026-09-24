package transform

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// Spec defines one transform step.
type Spec struct {
	Type string `json:"type" yaml:"type"`
}

func ApplyEncode(input string, steps []Spec) (string, error) {
	out := input
	for _, s := range steps {
		switch strings.ToLower(strings.TrimSpace(s.Type)) {
		case "", "none":
			continue
		case "base64":
			out = base64.StdEncoding.EncodeToString([]byte(out))
		case "base64url":
			out = base64.RawURLEncoding.EncodeToString([]byte(out))
		default:
			return "", fmt.Errorf("unsupported transform encode: %s", s.Type)
		}
	}
	return out, nil
}

func ApplyDecode(input string, steps []Spec) (string, error) {
	out := input
	for i := len(steps) - 1; i >= 0; i-- {
		t := strings.ToLower(strings.TrimSpace(steps[i].Type))
		switch t {
		case "", "none":
			continue
		case "base64":
			b, err := base64.StdEncoding.DecodeString(out)
			if err != nil {
				return "", err
			}
			out = string(b)
		case "base64url":
			b, err := base64.RawURLEncoding.DecodeString(out)
			if err != nil {
				return "", err
			}
			out = string(b)
		default:
			return "", fmt.Errorf("unsupported transform decode: %s", steps[i].Type)
		}
	}
	return out, nil
}
