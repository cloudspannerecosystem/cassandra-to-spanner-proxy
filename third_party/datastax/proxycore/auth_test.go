package proxycore

import (
	"bytes"
	"testing"
)

func TestEvaluateChallenge(t *testing.T) {
	// Create a passwordAuth instance for testing
	auth := &passwordAuth{
		authId:   "authId",
		username: "username",
		password: "password",
	}

	tests := []struct {
		name          string
		token         []byte
		expectedToken []byte
		expectError   bool
	}{
		{
			name:          "Correct token",
			token:         []byte("PLAIN-START"),
			expectedToken: []byte("authId\x00username\x00password"),
			expectError:   false,
		},
		{
			name:        "Nil token",
			token:       nil,
			expectError: true,
		},
		{
			name:        "Incorrect token",
			token:       []byte("WRONG-START"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := auth.EvaluateChallenge(tt.token)
			if (err != nil) != tt.expectError {
				t.Fatalf("expected error: %v, got: %v", tt.expectError, err)
			}
			if !tt.expectError && !bytes.Equal(token, tt.expectedToken) {
				t.Fatalf("expected token: %v, got: %v", tt.expectedToken, token)
			}
		})
	}
}
