package mgmtrpc

import (
	"context"
	"strings"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
	"github.com/logoscore/logos-golang-channel-core/pkg/runtime"
)

// Server provides management RPC handlers over a ProfileStore abstraction.
type Server struct {
	Store runtime.ProfileStore
}

func NewServer(store runtime.ProfileStore) *Server {
	return &Server{Store: store}
}

func (s *Server) ensureStore() error {
	if s == nil || s.Store == nil {
		return coreerrors.New(coreerrors.CodeInvalidInput, "profile store is required")
	}
	return nil
}

func (s *Server) CreateProfile(ctx context.Context, channelID string, p profile.Profile) error {
	if err := s.ensureStore(); err != nil {
		return err
	}
	if err := profile.Validate(p); err != nil {
		return coreerrors.Wrap(coreerrors.CodeProfileInvalid, "invalid profile", err)
	}
	_, err := s.Store.Get(ctx, channelID, p.ProfileID)
	if err == nil {
		return coreerrors.New(coreerrors.CodeProfileInvalid, "profile already exists")
	}
	if err := s.Store.Put(ctx, channelID, p); err != nil {
		return coreerrors.Wrap(coreerrors.CodeInternal, "store create profile", err)
	}
	return s.ValidateAllProfiles(ctx, channelID)
}

func (s *Server) ReadProfile(ctx context.Context, channelID, profileID string) (profile.Profile, error) {
	if err := s.ensureStore(); err != nil {
		return profile.Profile{}, err
	}
	p, err := s.Store.Get(ctx, channelID, profileID)
	if err != nil {
		return profile.Profile{}, coreerrors.Wrap(coreerrors.CodeProfileNotFound, "profile not found", err)
	}
	return p, nil
}

func (s *Server) UpdateProfile(ctx context.Context, channelID string, p profile.Profile) error {
	if err := s.ensureStore(); err != nil {
		return err
	}
	if err := profile.Validate(p); err != nil {
		return coreerrors.Wrap(coreerrors.CodeProfileInvalid, "invalid profile", err)
	}
	if err := s.Store.Put(ctx, channelID, p); err != nil {
		return coreerrors.Wrap(coreerrors.CodeInternal, "store update profile", err)
	}
	return s.ValidateAllProfiles(ctx, channelID)
}

func (s *Server) DeleteProfile(ctx context.Context, channelID, profileID string) error {
	if err := s.ensureStore(); err != nil {
		return err
	}
	if err := s.Store.Delete(ctx, channelID, profileID); err != nil {
		return coreerrors.Wrap(coreerrors.CodeInternal, "store delete profile", err)
	}
	return s.ValidateAllProfiles(ctx, channelID)
}

func (s *Server) ActivateProfile(ctx context.Context, channelID, profileID string) error {
	if err := s.ensureStore(); err != nil {
		return err
	}
	profiles, err := s.Store.List(ctx, channelID)
	if err != nil {
		return coreerrors.Wrap(coreerrors.CodeInternal, "store list profiles", err)
	}
	found := false
	for i := range profiles {
		if profiles[i].ProfileID == profileID {
			profiles[i].Enabled = true
			found = true
		}
		if err := s.Store.Put(ctx, channelID, profiles[i]); err != nil {
			return coreerrors.Wrap(coreerrors.CodeInternal, "store activate profile", err)
		}
	}
	if !found {
		return coreerrors.New(coreerrors.CodeProfileNotFound, "profile not found")
	}
	return s.ValidateAllProfiles(ctx, channelID)
}

func (s *Server) ValidateProfile(ctx context.Context, channelID string, p profile.Profile) error {
	_ = channelID
	if err := s.ensureStore(); err != nil {
		return err
	}
	return profile.Validate(p)
}

func (s *Server) ValidateAllProfiles(ctx context.Context, channelID string) error {
	if err := s.ensureStore(); err != nil {
		return err
	}
	if strings.TrimSpace(channelID) == "" {
		return coreerrors.New(coreerrors.CodeInvalidInput, "channelID is required")
	}
	profiles, err := s.Store.List(ctx, channelID)
	if err != nil {
		return coreerrors.Wrap(coreerrors.CodeInternal, "store list profiles", err)
	}
	if err := profile.ValidateMany(profiles); err != nil {
		return err
	}
	if err := profile.ValidateSet(profiles); err != nil {
		return err
	}
	return nil
}
