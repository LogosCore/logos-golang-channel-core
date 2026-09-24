package transform

import "testing"

func TestBase64Roundtrip(t *testing.T) {
	steps := []Spec{{Type: "base64"}}
	enc, err := ApplyEncode("hello", steps)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := ApplyDecode(enc, steps)
	if err != nil {
		t.Fatal(err)
	}
	if dec != "hello" {
		t.Fatalf("unexpected decode: %s", dec)
	}
}

func TestBase64URLRoundtrip(t *testing.T) {
	steps := []Spec{{Type: "base64url"}}
	enc, err := ApplyEncode("hello/world", steps)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := ApplyDecode(enc, steps)
	if err != nil {
		t.Fatal(err)
	}
	if dec != "hello/world" {
		t.Fatalf("unexpected decode: %s", dec)
	}
}
