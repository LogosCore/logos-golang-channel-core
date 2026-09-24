package errors

import (
	stderrs "errors"
	"fmt"
)

const (
	CodeProfileNotFound  = "ERR_PROFILE_NOT_FOUND"
	CodeProfileAmbiguous = "ERR_PROFILE_AMBIGUOUS"
	CodeProfileInvalid   = "ERR_PROFILE_INVALID"
	CodeSyncTimeout      = "ERR_SYNC_TIMEOUT"
	CodeSyncRejected     = "ERR_SYNC_REJECTED"
	CodeCanonicalInvalid = "ERR_CANONICAL_INVALID"
	CodeInvalidInput     = "ERR_INVALID_INPUT"
	CodeNotImplemented   = "ERR_NOT_IMPLEMENTED"
	CodeInternal         = "ERR_INTERNAL"
)

// Error is a machine-readable error for runtime and RPC surfaces.
type Error struct {
	Code    string
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func New(code, message string) error {
	return &Error{Code: code, Message: message}
}

func Wrap(code, message string, err error) error {
	if err == nil {
		return New(code, message)
	}
	return &Error{Code: code, Message: message, Err: err}
}

func Code(err error) string {
	if err == nil {
		return ""
	}
	var typed *Error
	if stderrs.As(err, &typed) {
		return typed.Code
	}
	return CodeInternal
}
