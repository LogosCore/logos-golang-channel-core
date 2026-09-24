package resolver

import "testing"

func TestResolveBody(t *testing.T) {
	v, ok, err := Resolve("body:id", Input{Body: map[string]any{"id": "a1"}})
	if err != nil || !ok || v != "a1" {
		t.Fatalf("unexpected: v=%q ok=%v err=%v", v, ok, err)
	}
}

func TestResolveHeader(t *testing.T) {
	v, ok, err := Resolve("header:X-ID", Input{Headers: map[string]string{"X-ID": "a1"}})
	if err != nil || !ok || v != "a1" {
		t.Fatalf("unexpected: v=%q ok=%v err=%v", v, ok, err)
	}
}

func TestResolveInvalidRef(t *testing.T) {
	_, _, err := Resolve("id", Input{})
	if err == nil {
		t.Fatal("expected error")
	}
}
