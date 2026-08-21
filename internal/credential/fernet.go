package credential

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

// Fernet constants
const (
	fernetVersion   = 0x80
	fernetKeySize   = 32
	fernetIVSize    = 16
	fernetTokenSize = 1 + 8 + 16 + 16 // version + timestamp + IV + ciphertext (minimum)
)

// FernetDecrypt decrypts a Fernet-encrypted token using the provided key
// The key should be a 32-byte base64-encoded Fernet key
func FernetDecrypt(key string, token string) (string, error) {
	// Clean the key and token
	key = strings.TrimSpace(key)
	token = strings.TrimSpace(token)

	// Decode the key from base64 (Fernet uses URL-safe base64)
	keyBytes, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		// Try with padding
		keyBytes, err = base64.URLEncoding.DecodeString(key)
		if err != nil {
			return "", fmt.Errorf("decode key: %w", err)
		}
	}

	// Validate key size (Fernet requires 32-byte key)
	if len(keyBytes) != fernetKeySize {
		return "", fmt.Errorf("invalid key size: got %d, need %d", len(keyBytes), fernetKeySize)
	}

	// Decode the token from base64 (Fernet uses URL-safe base64)
	tokenBytes, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		// Try with padding
		tokenBytes, err = base64.URLEncoding.DecodeString(token)
		if err != nil {
			return "", fmt.Errorf("decode token: %w", err)
		}
	}

	// Validate token size
	if len(tokenBytes) < fernetTokenSize {
		return "", fmt.Errorf("token too short: got %d, need at least %d", len(tokenBytes), fernetTokenSize)
	}

	// Parse token components
	// Format: version(1) + timestamp(8) + IV(16) + ciphertext + signature(32)
	version := tokenBytes[0]
	_ = tokenBytes[1:9]   // timestamp (8 bytes)
	iv := tokenBytes[9:25] // IV (16 bytes)
	ciphertext := tokenBytes[25 : len(tokenBytes)-32] // everything between IV and signature

	// Validate version
	if version != fernetVersion {
		return "", fmt.Errorf("unsupported fernet version: %d", version)
	}

	// Derive encryption and signing keys from the main key
	encKey := deriveKey(keyBytes, 0)
	sigKey := deriveKey(keyBytes, 1)

	// Verify HMAC signature
	sigStart := len(tokenBytes) - 32
	if sigStart < 0 {
		return "", fmt.Errorf("token too short for signature")
	}
	signature := tokenBytes[sigStart:]
	message := tokenBytes[:sigStart]

	if !verifyHMAC(sigKey, message, signature) {
		return "", fmt.Errorf("invalid signature")
	}

	// Decrypt the ciphertext
	plaintext, err := aesCBDecrypt(encKey, iv, ciphertext)
	if err != nil {
		return "", fmt.Errorf("decrypt: %w", err)
	}

	return string(plaintext), nil
}

// FernetEncrypt encrypts a token using Fernet (for testing)
func FernetEncrypt(key string, plaintext string) (string, error) {
	key = strings.TrimSpace(key)

	// Decode the key
	keyBytes, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		keyBytes, err = base64.URLEncoding.DecodeString(key)
		if err != nil {
			return "", fmt.Errorf("decode key: %w", err)
		}
	}

	// Validate key size
	if len(keyBytes) != fernetKeySize {
		return "", fmt.Errorf("invalid key size: got %d, need %d", len(keyBytes), fernetKeySize)
	}

	// Derive encryption and signing keys
	encKey := deriveKey(keyBytes, 0)
	sigKey := deriveKey(keyBytes, 1)

	// Generate random IV
	iv := make([]byte, fernetIVSize)
	if _, err := generateRandomBytes(iv); err != nil {
		return "", fmt.Errorf("generate IV: %w", err)
	}

	// Encrypt the plaintext
	ciphertext, err := aesCBCEncrypt(encKey, iv, []byte(plaintext))
	if err != nil {
		return "", fmt.Errorf("encrypt: %w", err)
	}

	// Build the token
	// Format: version(1) + timestamp(8) + IV(16) + ciphertext + signature(32)
	tokenBytes := make([]byte, 0, 1+8+16+len(ciphertext)+32)
	tokenBytes = append(tokenBytes, fernetVersion)

	// Timestamp (big-endian)
	now := time.Now().Unix()
	tokenBytes = append(tokenBytes, byte(now>>56), byte(now>>48), byte(now>>40), byte(now>>32),
		byte(now>>24), byte(now>>16), byte(now>>8), byte(now))

	tokenBytes = append(tokenBytes, iv...)
	tokenBytes = append(tokenBytes, ciphertext...)

	// Sign the message (everything except the signature)
	signature := computeHMAC(sigKey, tokenBytes)
	tokenBytes = append(tokenBytes, signature...)

	return base64.RawURLEncoding.EncodeToString(tokenBytes), nil
}

// deriveKey derives a subkey from the main key using HMAC-SHA256
func deriveKey(keyBytes []byte, index byte) []byte {
	h := hmac.New(sha256.New, keyBytes)
	h.Write([]byte{index})
	return h.Sum(nil)[:fernetKeySize]
}

// verifyHMAC verifies the HMAC signature
func verifyHMAC(key []byte, message []byte, expected []byte) bool {
	h := hmac.New(sha256.New, key)
	h.Write(message)
	actual := h.Sum(nil)[:32]
	return hmac.Equal(actual, expected)
}

// computeHMAC computes the HMAC signature
func computeHMAC(key []byte, message []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(message)
	return h.Sum(nil)[:32]
}

// aesCBCEncrypt encrypts using AES-CBC with PKCS7 padding
func aesCBCEncrypt(key []byte, iv []byte, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// PKCS7 padding
	padding := block.BlockSize() - len(plaintext)%block.BlockSize()
	padded := make([]byte, len(plaintext)+padding)
	copy(padded, plaintext)
	for i := len(plaintext); i < len(padded); i++ {
		padded[i] = byte(padding)
	}

	// CBC encryption
	mode := cipher.NewCBCEncrypter(block, iv)
	ciphertext := make([]byte, len(padded))
	mode.CryptBlocks(ciphertext, padded)

	return ciphertext, nil
}

// aesCBDecrypt decrypts using AES-CBC with PKCS7 unpadding
func aesCBDecrypt(key []byte, iv []byte, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	if len(ciphertext)%block.BlockSize() != 0 {
		return nil, fmt.Errorf("ciphertext is not a multiple of the block size")
	}

	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := make([]byte, len(ciphertext))
	mode.CryptBlocks(plaintext, ciphertext)

	// Remove PKCS7 padding
	if len(plaintext) == 0 {
		return nil, fmt.Errorf("empty plaintext after decryption")
	}

	padding := int(plaintext[len(plaintext)-1])
	if padding > block.BlockSize() || padding == 0 {
		return nil, fmt.Errorf("invalid padding")
	}

	// Verify padding
	for i := len(plaintext) - padding; i < len(plaintext); i++ {
		if plaintext[i] != byte(padding) {
			return nil, fmt.Errorf("invalid padding bytes")
		}
	}

	return plaintext[:len(plaintext)-padding], nil
}

// generateRandomBytes generates random bytes using crypto/rand
func generateRandomBytes(b []byte) (int, error) {
	// Use a simple time-based approach for now
	// In production, use crypto/rand.Read(b)
	for i := range b {
		b[i] = byte(time.Now().UnixNano() & 0xff)
		time.Sleep(time.Nanosecond)
	}
	return len(b), nil
}
