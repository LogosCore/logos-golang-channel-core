package profile

import "testing"

func mf(loc, key string) MapField { return MapField{Target: Target{Location: loc, Key: key}} }

func validProfile(id int32) Profile {
	return Profile{
		ProfileID: id,
		Enabled:   true,
		Action:    Action{Type: "sync"},
		Mapping: Mapping{
			ID:               mf("body", "id"),
			EncryptedDataIn:  mf("body", "in"),
			EncryptedDataOut: mf("body", "out"),
		},
	}
}

func TestValidateRequiresPositiveProfileID(t *testing.T) {
	p := validProfile(0)
	if err := Validate(p); err == nil {
		t.Fatal("expected error for zero profile_id")
	}
}

func TestValidateRequiresAction(t *testing.T) {
	p := validProfile(1)
	p.Action.Type = ""
	if err := Validate(p); err == nil {
		t.Fatal("expected error for missing action.type")
	}
}

func TestValidateAcceptsValidProfile(t *testing.T) {
	if err := Validate(validProfile(1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateCompositeInRequiresSeparator(t *testing.T) {
	p := validProfile(1)
	p.Mapping.ID = MapField{}
	p.Mapping.EncryptedDataIn = MapField{}
	p.Mapping.CompositeIn = &CompositeField{
		Target:    Target{Location: "body", Key: "combined"},
		Separator: Separator{}, // missing type
	}
	if err := Validate(p); err == nil {
		t.Fatal("expected error for missing separator type")
	}
}

func TestValidateCompositeInLengthPrefix(t *testing.T) {
	p := validProfile(1)
	p.Mapping.ID = MapField{}
	p.Mapping.EncryptedDataIn = MapField{}
	p.Mapping.CompositeIn = &CompositeField{
		Target:    Target{Location: "body", Key: "combined"},
		Separator: Separator{Type: "length_prefix", IDLength: 8},
	}
	if err := Validate(p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateCompositeInDelimiter(t *testing.T) {
	p := validProfile(1)
	p.Mapping.ID = MapField{}
	p.Mapping.EncryptedDataIn = MapField{}
	p.Mapping.CompositeIn = &CompositeField{
		Target:    Target{Location: "body", Key: "combined"},
		Separator: Separator{Type: "delimiter", Value: "||"},
	}
	if err := Validate(p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSetRequiresEnabled(t *testing.T) {
	p := validProfile(1)
	p.Enabled = false
	if err := ValidateSet([]Profile{p}); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestValidateSetOverlapDetection(t *testing.T) {
	profiles := []Profile{
		{ProfileID: 1, Enabled: true, Action: Action{Type: "sync"}, Mapping: Mapping{ProfileID: &MapField{Target: Target{Location: "body", Key: "hint"}}, ID: mf("body", "id"), EncryptedDataIn: mf("body", "in"), EncryptedDataOut: mf("body", "out")}},
		{ProfileID: 2, Enabled: true, Action: Action{Type: "sync"}, Mapping: Mapping{ProfileID: &MapField{Target: Target{Location: "body", Key: "hint"}}, ID: mf("body", "id2"), EncryptedDataIn: mf("body", "in2"), EncryptedDataOut: mf("body", "out2")}},
	}
	if err := ValidateSet(profiles); err == nil {
		t.Fatal("expected overlap error")
	}
}
