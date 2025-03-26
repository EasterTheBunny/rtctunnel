package crypt_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rtctunnel/rtctunnel/pkg/crypt"
)

func TestCrypt(t *testing.T) {
	t.Parallel()

	k1 := crypt.GenerateKeyPair()
	k2 := crypt.GenerateKeyPair()

	msg := []byte("Hello World")
	encrypted := k1.Encrypt(k2.Public, msg)
	decrypted, err := k2.Decrypt(k1.Public, encrypted)

	require.NoError(t, err)
	assert.Equal(t, msg, decrypted)
}
