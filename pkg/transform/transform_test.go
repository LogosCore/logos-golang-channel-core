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

func TestChainRoundtrip(t *testing.T) {
	steps := []Spec{{Type: "prefix", Value: "p:"}, {Type: "base64url"}}
	enc, err := ApplyEncode("abc", steps)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := ApplyDecode(enc, steps)
	if err != nil {
		t.Fatal(err)
	}
	if dec != "abc" {
		t.Fatalf("unexpected decode: %s", dec)
	}
}

func TestReplaceRoundtrip(t *testing.T) {
	steps := []Spec{{Type: "replace", From: "/", To: "_"}}
	enc, _ := ApplyEncode("a/b", steps)
	if enc != "a_b" {
		t.Fatalf("unexpected encode: %s", enc)
	}
	dec, _ := ApplyDecode(enc, steps)
	if dec != "a/b" {
		t.Fatalf("unexpected decode: %s", dec)
	}
}
