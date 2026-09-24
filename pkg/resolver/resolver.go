package resolver

import (
	"fmt"
	"strings"
)

type Input struct {
	Body    map[string]any
	Headers map[string]string
	Query   map[string]string
	Cookies map[string]string
}

func ParseRef(ref string) (location, key string, err error) {
	parts := strings.SplitN(strings.TrimSpace(ref), ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid ref %q, expected <location>:<key>", ref)
	}
	location = strings.ToLower(strings.TrimSpace(parts[0]))
	key = strings.TrimSpace(parts[1])
	if key == "" {
		return "", "", fmt.Errorf("invalid ref %q, empty key", ref)
	}
	switch location {
	case "body", "header", "query", "cookie":
		return location, key, nil
	default:
		return "", "", fmt.Errorf("unsupported location %q", location)
	}
}

func Resolve(ref string, in Input) (string, bool, error) {
	loc, key, err := ParseRef(ref)
	if err != nil {
		return "", false, err
	}
	switch loc {
	case "body":
		if in.Body == nil {
			return "", false, nil
		}
		raw, ok := in.Body[key]
		if !ok {
			return "", false, nil
		}
		s, ok := raw.(string)
		if !ok {
			return "", false, nil
		}
		s = strings.TrimSpace(s)
		return s, s != "", nil
	case "header":
		v := strings.TrimSpace(in.Headers[key])
		return v, v != "", nil
	case "query":
		v := strings.TrimSpace(in.Query[key])
		return v, v != "", nil
	case "cookie":
		v := strings.TrimSpace(in.Cookies[key])
		return v, v != "", nil
	default:
		return "", false, nil
	}
}
