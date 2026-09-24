package profile

import "testing"

func TestValidateSetRequiresSingleFallback(t *testing.T) {
	profiles := []Profile{
		{ProfileID: "a", ChannelType: "http", Enabled: true, DefaultFallback: true, Mapping: Mapping{ID: "id", EncryptedData: "data"}},
		{ProfileID: "b", ChannelType: "http", Enabled: true, DefaultFallback: true, Mapping: Mapping{ID: "id", EncryptedData: "data"}},
	}
	if err := ValidateSet(profiles); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestValidateSetOverlapDetection(t *testing.T) {
	profiles := []Profile{
		{ProfileID: "a", ChannelType: "http", Enabled: true, DefaultFallback: true, Mapping: Mapping{ProfileID: "hint", ID: "body:id", EncryptedData: "body:data"}},
		{ProfileID: "b", ChannelType: "http", Enabled: true, DefaultFallback: false, Mapping: Mapping{ProfileID: "hint", ID: "body:id2", EncryptedData: "body:data2"}},
	}
	if err := ValidateSet(profiles); err == nil {
		t.Fatal("expected overlap error")
	}
}

func TestValidateSetNoHintNonFallback(t *testing.T) {
	profiles := []Profile{
		{ProfileID: "f", ChannelType: "http", Enabled: true, DefaultFallback: true, Mapping: Mapping{ID: "body:id", EncryptedData: "body:data"}},
		{ProfileID: "a", ChannelType: "http", Enabled: true, DefaultFallback: false, Mapping: Mapping{ID: "body:id2", EncryptedData: "body:data2"}},
	}
	if err := ValidateSet(profiles); err == nil {
		t.Fatal("expected no-hint ambiguity error")
	}
}
