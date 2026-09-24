package transform

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
)

// Spec defines one transform step.
type Spec struct {
	Type  string `json:"type" yaml:"type"`
	Value string `json:"value,omitempty" yaml:"value,omitempty"`
	From  string `json:"from,omitempty" yaml:"from,omitempty"`
	To    string `json:"to,omitempty" yaml:"to,omitempty"`
}

func ApplyEncode(input string, steps []Spec) (string, error) {
	out := input
	for _, s := range steps {
		t := strings.ToLower(strings.TrimSpace(s.Type))
		switch t {
		case "", "none":
			continue
		case "base64":
			out = base64.StdEncoding.EncodeToString([]byte(out))
		case "base64url":
			out = base64.RawURLEncoding.EncodeToString([]byte(out))
		case "prefix":
			out = s.Value + out
		case "suffix":
			out = out + s.Value
		case "replace":
			out = strings.ReplaceAll(out, s.From, s.To)
		case "url_encode":
			out = url.QueryEscape(out)
		case "url_decode":
			v, err := url.QueryUnescape(out)
			if err != nil {
				return "", err
			}
			out = v
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
		s := steps[i]
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
		case "prefix":
			if !strings.HasPrefix(out, s.Value) {
				return "", fmt.Errorf("prefix mismatch")
			}
			out = strings.TrimPrefix(out, s.Value)
		case "suffix":
			if !strings.HasSuffix(out, s.Value) {
				return "", fmt.Errorf("suffix mismatch")
			}
			out = strings.TrimSuffix(out, s.Value)
		case "replace":
			out = strings.ReplaceAll(out, s.To, s.From)
		case "url_encode":
			v, err := url.QueryUnescape(out)
			if err != nil {
				return "", err
			}
			out = v
		case "url_decode":
			out = url.QueryEscape(out)
		default:
			return "", fmt.Errorf("unsupported transform decode: %s", s.Type)
		}
	}
	return out, nil
}
