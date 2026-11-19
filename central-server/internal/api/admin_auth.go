package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"golang.org/x/crypto/bcrypt"
)

const (
	adminAuthCookieName    = "dota_admin_session"
	adminSessionTTL        = 24 * time.Hour
	adminMaxFailedAttempts = 5
	adminLockoutWindow     = 60 * time.Second
)

type loginAttemptWindow struct {
	failures    int
	windowStart time.Time
	lockedUntil time.Time
}

type persistedAdminAuth struct {
	PasswordHash string `json:"password_hash"`
}

// EnableAdminPasswordAuth enables admin authentication backed by a persisted bcrypt password hash.
func (s *Server) EnableAdminPasswordAuth(path string) {
	s.adminAuthMu.Lock()
	defer s.adminAuthMu.Unlock()

	s.adminAuthEnabled = true
	s.adminAuthPath = path
	if s.adminSessions == nil {
		s.adminSessions = make(map[string]time.Time)
	}
	if s.adminLoginAttempts == nil {
		s.adminLoginAttempts = make(map[string]*loginAttemptWindow)
	}

	if path != "" {
		if raw, err := os.ReadFile(path); err == nil {
			var cfg persistedAdminAuth
			if uErr := json.Unmarshal(raw, &cfg); uErr == nil && strings.TrimSpace(cfg.PasswordHash) != "" {
				s.adminPasswordHash = []byte(strings.TrimSpace(cfg.PasswordHash))
				return
			}
		}
	}

	if envSecret := strings.TrimSpace(os.Getenv("ADMIN_SECRET")); envSecret != "" && len(s.adminPasswordHash) == 0 {
		if hash, err := bcrypt.GenerateFromPassword([]byte(envSecret), bcrypt.DefaultCost); err == nil {
			s.adminPasswordHash = hash
		}
	}
}

// HasAdminPassword reports whether an admin password (or ADMIN_SECRET) has been configured.
func (s *Server) HasAdminPassword() bool {
	s.adminAuthMu.Lock()
	hasHash := len(s.adminPasswordHash) > 0
	s.adminAuthMu.Unlock()
	return hasHash || strings.TrimSpace(os.Getenv("ADMIN_SECRET")) != ""
}

// SetAdminPassword updates the admin password hash, revokes existing sessions, and optionally persists it to disk.
func (s *Server) SetAdminPassword(plain string, persist bool) error {
	clean := strings.TrimSpace(plain)
	if len(clean) < 6 {
		return errors.New("admin password must be at least 6 characters")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(clean), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	s.adminAuthMu.Lock()
	s.adminAuthEnabled = true
	s.adminPasswordHash = hash
	s.adminSessions = make(map[string]time.Time)
	if s.adminLoginAttempts == nil {
		s.adminLoginAttempts = make(map[string]*loginAttemptWindow)
	}
	authPath := s.adminAuthPath
	s.adminAuthMu.Unlock()

	if persist && authPath != "" {
		if err := os.MkdirAll(filepath.Dir(authPath), 0o755); err != nil {
			return err
		}
		data, err := json.MarshalIndent(persistedAdminAuth{PasswordHash: string(hash)}, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(authPath, data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) isAdminAuthRequired() bool {
	s.adminAuthMu.Lock()
	enabled := s.adminAuthEnabled
	s.adminAuthMu.Unlock()
	return enabled || strings.TrimSpace(os.Getenv("ADMIN_SECRET")) != ""
}

func (s *Server) isRequestAdminAuthenticated(c *fiber.Ctx) bool {
	if !s.isAdminAuthRequired() {
		return true
	}

	now := time.Now()
	cookieTok := strings.TrimSpace(c.Cookies(adminAuthCookieName))
	bearerTok := extractBearerToken(c)
	headerSecret := strings.TrimSpace(c.Get("X-Admin-Secret"))

	s.adminAuthMu.Lock()
	for k, exp := range s.adminSessions {
		if now.After(exp) {
			delete(s.adminSessions, k)
		}
	}
	for _, tok := range []string{cookieTok, bearerTok} {
		if tok != "" {
			if exp, ok := s.adminSessions[tok]; ok && now.Before(exp) {
				s.adminAuthMu.Unlock()
				return true
			}
		}
	}
	hashCopy := append([]byte(nil), s.adminPasswordHash...)
	s.adminAuthMu.Unlock()

	for _, candidate := range []string{bearerTok, headerSecret} {
		if candidate == "" {
			continue
		}
		if envSecret := strings.TrimSpace(os.Getenv("ADMIN_SECRET")); envSecret != "" {
			if subtle.ConstantTimeCompare([]byte(candidate), []byte(envSecret)) == 1 {
				return true
			}
		}
		if len(hashCopy) > 0 && bcrypt.CompareHashAndPassword(hashCopy, []byte(candidate)) == nil {
			return true
		}
	}
	return false
}

func (s *Server) handleAdminAuthStatus(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"auth_required":       s.isAdminAuthRequired(),
		"password_configured": s.HasAdminPassword(),
		"authenticated":       s.isRequestAdminAuthenticated(c),
	})
}

func (s *Server) handleAdminAuthLogin(c *fiber.Ctx) error {
	ip := c.IP()
	now := time.Now()

	s.adminAuthMu.Lock()
	if s.adminLoginAttempts == nil {
		s.adminLoginAttempts = make(map[string]*loginAttemptWindow)
	}
	if s.adminSessions == nil {
		s.adminSessions = make(map[string]time.Time)
	}
	if w, ok := s.adminLoginAttempts[ip]; ok && now.Before(w.lockedUntil) {
		s.adminAuthMu.Unlock()
		return c.Status(http.StatusTooManyRequests).JSON(fiber.Map{
			"error": "too many failed login attempts — try again in 60 seconds",
		})
	}
	hashCopy := append([]byte(nil), s.adminPasswordHash...)
	s.adminAuthMu.Unlock()

	if !s.HasAdminPassword() {
		return c.Status(http.StatusForbidden).JSON(fiber.Map{
			"error":               "Admin password not configured — run 'password' in the central-server terminal to set one",
			"password_configured": false,
		})
	}

	var body struct {
		Password string `json:"password"`
	}
	if err := json.Unmarshal(c.Body(), &body); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON body"})
	}

	candidate := strings.TrimSpace(body.Password)
	valid := false
	if candidate != "" {
		if envSecret := strings.TrimSpace(os.Getenv("ADMIN_SECRET")); envSecret != "" {
			if subtle.ConstantTimeCompare([]byte(candidate), []byte(envSecret)) == 1 {
				valid = true
			}
		}
		if !valid && len(hashCopy) > 0 && bcrypt.CompareHashAndPassword(hashCopy, []byte(candidate)) == nil {
			valid = true
		}
	}

	if !valid {
		s.adminAuthMu.Lock()
		w, ok := s.adminLoginAttempts[ip]
		if !ok || now.Sub(w.windowStart) > adminLockoutWindow {
			w = &loginAttemptWindow{windowStart: now}
			s.adminLoginAttempts[ip] = w
		}
		w.failures++
		if w.failures >= adminMaxFailedAttempts {
			w.lockedUntil = now.Add(adminLockoutWindow)
			s.adminAuthMu.Unlock()
			return c.Status(http.StatusTooManyRequests).JSON(fiber.Map{
				"error": "too many failed login attempts — try again in 60 seconds",
			})
		}
		s.adminAuthMu.Unlock()
		return c.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "invalid admin password"})
	}

	tokBytes := make([]byte, 32)
	if _, err := rand.Read(tokBytes); err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "failed creating session token"})
	}
	token := hex.EncodeToString(tokBytes)

	s.adminAuthMu.Lock()
	delete(s.adminLoginAttempts, ip)
	s.adminSessions[token] = now.Add(adminSessionTTL)
	s.adminAuthMu.Unlock()

	c.Cookie(&fiber.Cookie{
		Name:     adminAuthCookieName,
		Value:    token,
		Path:     "/",
		HTTPOnly: true,
		SameSite: "Strict",
		MaxAge:   int(adminSessionTTL.Seconds()),
	})
	return c.JSON(fiber.Map{"authenticated": true})
}

func (s *Server) handleAdminAuthLogout(c *fiber.Ctx) error {
	tok := strings.TrimSpace(c.Cookies(adminAuthCookieName))
	if tok == "" {
		tok = extractBearerToken(c)
	}
	if tok != "" {
		s.adminAuthMu.Lock()
		delete(s.adminSessions, tok)
		s.adminAuthMu.Unlock()
	}
	c.Cookie(&fiber.Cookie{
		Name:     adminAuthCookieName,
		Value:    "",
		Path:     "/",
		HTTPOnly: true,
		SameSite: "Strict",
		MaxAge:   -1,
	})
	return c.JSON(fiber.Map{"authenticated": false})
}
