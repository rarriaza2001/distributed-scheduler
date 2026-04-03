package store

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrNotFound is returned when a job row does not exist.
	ErrNotFound = errors.New("store: job not found")

	// ErrPreconditionFailed is returned when an update affects zero rows
	// (optimistic version mismatch or status guard).
	ErrPreconditionFailed = errors.New("store: optimistic lock or status precondition failed")

	// ErrIdempotencyConflict is returned when inserting a job with an
	// idempotency_key that already exists.
	ErrIdempotencyConflict = errors.New("store: idempotency key conflict")

	// errUnexpectedNilRow guards internal scan bugs (should not happen).
	errUnexpectedNilRow = errors.New("store: unexpected nil row")
)

func mapNotFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
