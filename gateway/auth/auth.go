package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	HeaderName = "X-Gateway-Token"
)

var sharedSecret = []byte("ddb-gateway-secret-2025-change-me")

func SetSecret(s string) { sharedSecret = []byte(s) }

func NewToken() (string, error) {
	nonceBuf := make([]byte, 16)
	if _, err := rand.Read(nonceBuf); err != nil {
		return "", fmt.Errorf("auth.NewToken: rand.Read: %w", err)
	}
	nonce := hex.EncodeToString(nonceBuf)
	sig := sign(nonce)
	return nonce + "|" + sig, nil
}

func Verify(token string) error {
	parts := strings.SplitN(token, "|", 2)
	if len(parts) != 2 {
		return fmt.Errorf("auth: malformed token (expected nonce|sig)")
	}
	nonce, gotSig := parts[0], parts[1]
	if len(nonce) < 16 {
		return fmt.Errorf("auth: nonce too short")
	}

	wantSig := sign(nonce)
	if !hmac.Equal([]byte(gotSig), []byte(wantSig)) {
		return fmt.Errorf("auth: invalid signature")
	}
	return nil
}

func sign(nonce string) string {
	mac := hmac.New(sha256.New, sharedSecret)
	mac.Write([]byte(nonce))
	return hex.EncodeToString(mac.Sum(nil))
}
