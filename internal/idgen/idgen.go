package idgen

import (
	"crypto/rand"
	"encoding/hex"
)

func New() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("idgen: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
