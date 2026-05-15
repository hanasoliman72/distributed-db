package auth

// auth.go
//
// Generates and verifies HMAC-SHA256 tokens that the API Gateway
// attaches to every request it forwards to a slave.
//
// Flow:
//   gateway builds token → attaches as X-Gateway-Token header → slave verifies
//
// Token format (pipe-delimited):
//   <unix_timestamp>|<nonce>|<HMAC-SHA256(secret, timestamp|nonce)>

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// HeaderName is the HTTP header slaves check.
	HeaderName = "X-Gateway-Token"
)

// sharedSecret is the HMAC key.  In production load this from an env var or
// secrets manager; never commit a real secret to source control.
var sharedSecret = []byte("ddb-gateway-secret-2025-change-me")

// SetSecret overrides the default secret (call once at startup from main).
func SetSecret(s string) { sharedSecret = []byte(s) }

// NewToken mints a fresh token with timestamp and HMAC signature.
func NewToken() (string, error) {
	ts := strconv.FormatInt(time.Now().Unix(), 10)

	nonceBuf := make([]byte, 8)
	if _, err := rand.Read(nonceBuf); err != nil {
		return "", fmt.Errorf("auth.NewToken: rand.Read: %w", err)
	}
	nonce := hex.EncodeToString(nonceBuf)

	sig := sign(ts, nonce)
	return ts + "|" + nonce + "|" + sig, nil
}

// Verify returns nil if the token is well-formed and has a valid HMAC.
// Returns a descriptive error otherwise.
func Verify(token string) error {
	parts := strings.SplitN(token, "|", 3)
	if len(parts) != 3 {
		return fmt.Errorf("auth: malformed token")
	}
	ts, nonce, gotSig := parts[0], parts[1], parts[2]

	wantSig := sign(ts, nonce)
	if !hmac.Equal([]byte(gotSig), []byte(wantSig)) {
		return fmt.Errorf("auth: invalid signature")
	}
	return nil
}

// sign computes HMAC-SHA256(secret, ts+"|"+nonce) and returns the hex string.
func sign(ts, nonce string) string {
	mac := hmac.New(sha256.New, sharedSecret)
	mac.Write([]byte(ts + "|" + nonce))
	return hex.EncodeToString(mac.Sum(nil))
}
