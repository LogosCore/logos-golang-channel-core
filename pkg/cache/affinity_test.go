package cache

import (
	"testing"
	"time"
)

func TestAffinityGetSet(t *testing.T) {
	c := NewAffinity(1 * time.Minute)
	c.Set("src", 1)
	v, ok := c.Get("src")
	if !ok || v != 1 {
		t.Fatalf("unexpected value: %d ok=%v", v, ok)
	}
}

func TestAffinityTTL(t *testing.T) {
	c := NewAffinity(5 * time.Millisecond)
	c.Set("src", 1)
	time.Sleep(10 * time.Millisecond)
	if _, ok := c.Get("src"); ok {
		t.Fatal("expected expired entry")
	}
}
