package freebuff

import "freebuff-reverse/internal/channels"

func init() {
	channels.RegisterBuiltin(func(r *channels.Registry) error {
		return r.Register(New())
	})
}
