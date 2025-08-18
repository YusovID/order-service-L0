package storage

import "errors"

var (
	ErrNoOrder    = errors.New("no order found")
	ErrEmptyOrder = errors.New("no items in order")
)
