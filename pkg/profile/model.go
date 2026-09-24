package profile

// Profile defines one obfuscation profile for a channel.
type Profile struct {
	ProfileID       string  `json:"profile_id" yaml:"profile_id"`
	ChannelType     string  `json:"channel_type" yaml:"channel_type"`
	Enabled         bool    `json:"enabled" yaml:"enabled"`
	DefaultFallback bool    `json:"default_fallback" yaml:"default_fallback"`
	Priority        int     `json:"priority" yaml:"priority"`
	Mapping         Mapping `json:"mapping" yaml:"mapping"`
}

// Mapping describes field locations for canonical values in a transport payload.
type Mapping struct {
	ProfileID     string `json:"profile_id" yaml:"profile_id"`
	ID            string `json:"id" yaml:"id"`
	EncryptedData string `json:"encrypted_data" yaml:"encrypted_data"`
}
