package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"desktop/internal/db"
)

const pbkdf2Iterations = 100000
const saltSize = 32

type Session struct {
	Username  string
	Token     string
	CreatedAt time.Time
}

type Manager struct {
	db       *db.DB
	mu       sync.RWMutex
	sessions map[string]*Session // token -> session
}

func NewManager(database *db.DB) *Manager {
	return &Manager{db: database, sessions: make(map[string]*Session)}
}

// pbkdf2SHA256 derives a key using PBKDF2-HMAC-SHA256, matching Python's hashlib.pbkdf2_hmac("sha256", ...).
func pbkdf2SHA256(password, salt []byte, iterations int) []byte {
	// PBKDF2 with HMAC-SHA256
	keyLen := 32 // SHA-256 output size
	numBlocks := (keyLen + sha256.Size - 1) / sha256.Size
	dk := make([]byte, 0, numBlocks*sha256.Size)

	for block := 1; block <= numBlocks; block++ {
		// U1 = PRF(password, salt || INT_32_BE(i))
		mac := hmac.New(sha256.New, password)
		mac.Write(salt)
		mac.Write([]byte{byte(block >> 24), byte(block >> 16), byte(block >> 8), byte(block)})
		u := mac.Sum(nil)
		result := make([]byte, len(u))
		copy(result, u)

		// U2..Uc
		for i := 2; i <= iterations; i++ {
			mac = hmac.New(sha256.New, password)
			mac.Write(u)
			u = mac.Sum(nil)
			for j := range result {
				result[j] ^= u[j]
			}
		}
		dk = append(dk, result...)
	}
	return dk[:keyLen]
}

// hashPassword generates a PBKDF2-SHA256 hash compatible with Python's implementation.
// Format: hex(salt + hash) where salt is 32 bytes and hash is 32 bytes.
func hashPassword(password string) (string, error) {
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	h := pbkdf2SHA256([]byte(password), salt, pbkdf2Iterations)
	combined := append(salt, h...)
	return hex.EncodeToString(combined), nil
}

// verifyPassword checks a password against a stored PBKDF2-SHA256 hash.
func verifyPassword(password, storedHex string) bool {
	raw, err := hex.DecodeString(storedHex)
	if err != nil || len(raw) < saltSize {
		return false
	}
	salt := raw[:saltSize]
	storedHash := raw[saltSize:]
	h := pbkdf2SHA256([]byte(password), salt, pbkdf2Iterations)
	return hmac.Equal(h, storedHash)
}

func (m *Manager) Register(username, password string) error {
	if username == "" || password == "" {
		return fmt.Errorf("username and password are required")
	}
	pwHash, err := hashPassword(password)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}
	_, err = m.db.Exec(
		"INSERT INTO users (username, password_hash, created_at) VALUES (?, ?, ?)",
		username, pwHash, time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("create user: %w", err)
	}
	return nil
}

func (m *Manager) Login(username, password string) (string, error) {
	if username == "" || password == "" {
		return "", fmt.Errorf("username and password are required")
	}
	var hash string
	err := m.db.QueryRow("SELECT password_hash FROM users WHERE username = ?", username).Scan(&hash)
	if err != nil {
		return "", fmt.Errorf("invalid username or password")
	}
	if !verifyPassword(password, hash) {
		return "", fmt.Errorf("invalid username or password")
	}
	token, err := generateToken()
	if err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	m.mu.Lock()
	m.sessions[token] = &Session{Username: username, Token: token, CreatedAt: time.Now()}
	m.mu.Unlock()

	m.db.Exec("UPDATE users SET last_login = ? WHERE username = ?",
		time.Now().UTC().Format(time.RFC3339), username)

	return token, nil
}

func (m *Manager) Verify(token string) (string, error) {
	m.mu.RLock()
	session, ok := m.sessions[token]
	m.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("invalid or expired token")
	}
	return session.Username, nil
}

func (m *Manager) Logout(token string) {
	m.mu.Lock()
	delete(m.sessions, token)
	m.mu.Unlock()
}

func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
