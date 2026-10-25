// Package errors adds the channel error codes to module-core's typed error.
// Error, New, Wrap and Code are module-core's, so codes from either package
// read the same way.
package errors

import moderrors "github.com/logoscore/logos-golang-module-core/pkg/errors"

// Channel codes.
const (
	CodeProfileNotFound  = "ERR_PROFILE_NOT_FOUND"
	CodeProfileAmbiguous = "ERR_PROFILE_AMBIGUOUS"
	CodeProfileInvalid   = "ERR_PROFILE_INVALID"
	CodeSyncTimeout      = "ERR_SYNC_TIMEOUT"
	CodeSyncRejected     = "ERR_SYNC_REJECTED"
	CodeCanonicalInvalid = "ERR_CANONICAL_INVALID"
)

// Shared codes, from module-core.
const (
	CodeInvalidInput   = moderrors.CodeInvalidInput
	CodeNotImplemented = moderrors.CodeNotImplemented
	CodeInternal       = moderrors.CodeInternal
)

// Error is a machine-readable error for runtime and RPC surfaces.
type Error = moderrors.Error

// New returns an Error with code and message.
func New(code, message string) error { return moderrors.New(code, message) }

// Wrap returns an Error with code and message wrapping err (New when err is
// nil).
func Wrap(code, message string, err error) error { return moderrors.Wrap(code, message, err) }

// Code returns the code of the first Error in err's chain, CodeInternal for
// any other error, and "" for nil.
func Code(err error) string { return moderrors.Code(err) }
