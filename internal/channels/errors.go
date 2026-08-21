package channels

import (
	"errors"
	"fmt"
)

var (
	ErrAccountUnavailable = errors.New("channels: account unavailable for requested session")
	ErrCapacityLimited    = errors.New("channels: capacity limited")
)

func AccountUnavailablef(format string, args ...any) error {
	if format == "" {
		return ErrAccountUnavailable
	}
	return fmt.Errorf("%w: "+format, append([]any{ErrAccountUnavailable}, args...)...)
}

func CapacityLimitedf(format string, args ...any) error {
	if format == "" {
		return ErrCapacityLimited
	}
	return fmt.Errorf("%w: "+format, append([]any{ErrCapacityLimited}, args...)...)
}
