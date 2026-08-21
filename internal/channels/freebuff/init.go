package freebuff

import "github.com/marktantongco/freebuff-gateway/internal/channels"

func init() {
	channels.RegisterBuiltin(func(r *channels.Registry) error {
		return r.Register(New())
	})
}
