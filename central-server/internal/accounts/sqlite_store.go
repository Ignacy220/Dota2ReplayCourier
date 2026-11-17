package accounts

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"golang.org/x/crypto/scrypt"
	_ "modernc.org/sqlite"
)

const (
	XPPerMatch     = 50
	sessionTTL     = 90 * 24 * time.Hour
	scryptN        = 32768
	scryptR        = 8
	scryptP        = 1
	scryptKeyLen   = 32
	saltByteLength = 16
)

var (
	ErrInvalidNickname   = errors.New("nickname must be 3-32 characters using letters, numbers, underscores, or hyphens")
	ErrWeakPassword      = errors.New("password must be at least 6 characters")
	ErrNicknameTaken     = errors.New("nickname is already taken")
	ErrInvalidLogin      = errors.New("invalid nickname or password")
	ErrUnauthorizedToken = errors.New("invalid or expired session token")
	ErrInvalidFaction    = errors.New("faction must be \"radiant\" or \"dire\"")

	nicknameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{3,32}$`)
)

// Faction values. A user's faction is chosen once at registration and can never be changed.
// Accounts without a faction (NULL) are neutral and do not contribute to either side.
const (
	FactionRadiant = "radiant"
	FactionDire    = "dire"
)

// NormalizeFaction validates a faction string. Empty input means "no faction" (neutral).
func NormalizeFaction(raw string) (string, error) {
	f := strings.ToLower(strings.TrimSpace(raw))
	switch f {
	case "":
		return "", nil
	case FactionRadiant, FactionDire:
		return f, nil
	default:
		return "", ErrInvalidFaction
	}
}

// UserProfile represents the authenticated user's own account fields stored in SQLite.
type UserProfile struct {
	ID            int64           `json:"-"`
	Nickname      string          `json:"nickname"`
	Faction       string          `json:"faction"` // "radiant" | "dire" | "" (neutral)
	XP            uint64          `json:"xp"`
	MatchesParsed uint64          `json:"matches_parsed"`
	OtherData     json.RawMessage `json:"other_data"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// FactionStanding aggregates the XP and membership of one faction.
type FactionStanding struct {
	XP      uint64 `json:"xp"`
	Members int64  `json:"members"`
}

// FactionStandings is the Radiant vs Dire war score (sum of member XP per side).
type FactionStandings struct {
	Radiant FactionStanding `json:"radiant"`
	Dire    FactionStanding `json:"dire"`
}

// Store manages user accounts, scrypt password hashes + per-user salts, sessions, and server-side XP in SQLite.
type Store struct {
	db *sql.DB
}

// NewStore opens or initializes the SQLite database at dbPath (default: data/accounts.db).
func NewStore(dbPath string) (*Store, error) {
	if dbPath == "" {
		dbPath = filepath.Join("data", "accounts.db")
	}
	if dir := filepath.Dir(dbPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create sqlite dir: %w", err)
		}
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}
	// Single writer connection keeps SQLite lock-free under concurrent HTTP requests.
	db.SetMaxOpenConns(1)

	s := &Store{db: db}
	if err := s.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the underlying SQLite database.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) initSchema() error {
	pragmas := []string{
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA busy_timeout = 5000;`,
		`PRAGMA foreign_keys = ON;`,
	}
	for _, p := range pragmas {
		if _, err := s.db.Exec(p); err != nil {
			return fmt.Errorf("sqlite pragma (%s): %w", p, err)
		}
	}

	schema := `
CREATE TABLE IF NOT EXISTS users (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	nickname TEXT NOT NULL UNIQUE COLLATE NOCASE,
	pass_hash TEXT NOT NULL,
	salt TEXT NOT NULL,
	faction TEXT CHECK (faction IN ('radiant', 'dire')),
	xp INTEGER NOT NULL DEFAULT 0,
	matches_parsed INTEGER NOT NULL DEFAULT 0,
	other_data TEXT NOT NULL DEFAULT '{}',
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
	token_hash TEXT PRIMARY KEY,
	user_id INTEGER NOT NULL,
	created_at DATETIME NOT NULL,
	expires_at DATETIME NOT NULL,
	FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("sqlite init schema: %w", err)
	}

	// Migrate databases created before factions existed: existing accounts get NULL (neutral).
	hasFaction, err := s.columnExists("users", "faction")
	if err != nil {
		return err
	}
	if !hasFaction {
		if _, err := s.db.Exec(`ALTER TABLE users ADD COLUMN faction TEXT CHECK (faction IN ('radiant', 'dire'))`); err != nil {
			return fmt.Errorf("sqlite migrate faction column: %w", err)
		}
	}
	if _, err := s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_users_faction ON users(faction)`); err != nil {
		return fmt.Errorf("sqlite faction index: %w", err)
	}
	return nil
}

func (s *Store) columnExists(table, column string) (bool, error) {
	rows, err := s.db.Query(`SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return false, fmt.Errorf("sqlite table_info(%s): %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}
	return false, rows.Err()
}

// Register creates a new user account with a random per-user salt and scrypt hash, and returns a session token + profile.
// faction must be "radiant", "dire", or empty (neutral). It is permanent: no API exists to change it afterwards.
func (s *Store) Register(ctx context.Context, nickname, password, faction string) (string, *UserProfile, error) {
	nick := strings.TrimSpace(nickname)
	if !nicknameRegex.MatchString(nick) {
		return "", nil, ErrInvalidNickname
	}
	if len(password) < 6 {
		return "", nil, ErrWeakPassword
	}
	side, err := NormalizeFaction(faction)
	if err != nil {
		return "", nil, err
	}
	var sideVal any
	if side != "" {
		sideVal = side
	}

	saltBytes := make([]byte, saltByteLength)
	if _, err := rand.Read(saltBytes); err != nil {
		return "", nil, err
	}
	saltHex := hex.EncodeToString(saltBytes)

	hashBytes, err := scrypt.Key([]byte(password), saltBytes, scryptN, scryptR, scryptP, scryptKeyLen)
	if err != nil {
		return "", nil, err
	}
	hashHex := hex.EncodeToString(hashBytes)

	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO users (nickname, pass_hash, salt, faction, xp, matches_parsed, other_data, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 0, 0, '{}', ?, ?)`,
		nick, hashHex, saltHex, sideVal, now, now,
	)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return "", nil, ErrNicknameTaken
		}
		return "", nil, err
	}

	userID, err := res.LastInsertId()
	if err != nil {
		return "", nil, err
	}

	token, err := s.createSession(ctx, userID)
	if err != nil {
		return "", nil, err
	}

	profile, err := s.GetUserByID(ctx, userID)
	if err != nil {
		return "", nil, err
	}
	return token, profile, nil
}

// Login verifies nickname and password against the stored salt and pass_hash and returns a new session token + profile.
func (s *Store) Login(ctx context.Context, nickname, password string) (string, *UserProfile, error) {
	nick := strings.TrimSpace(nickname)
	if nick == "" || password == "" {
		return "", nil, ErrInvalidLogin
	}

	var userID int64
	var storedHashHex, storedSaltHex string
	err := s.db.QueryRowContext(ctx,
		`SELECT id, pass_hash, salt FROM users WHERE nickname = ?`,
		nick,
	).Scan(&userID, &storedHashHex, &storedSaltHex)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, ErrInvalidLogin
	}
	if err != nil {
		return "", nil, err
	}

	saltBytes, err := hex.DecodeString(storedSaltHex)
	if err != nil {
		return "", nil, ErrInvalidLogin
	}
	expectedHashBytes, err := hex.DecodeString(storedHashHex)
	if err != nil {
		return "", nil, ErrInvalidLogin
	}

	actualHashBytes, err := scrypt.Key([]byte(password), saltBytes, scryptN, scryptR, scryptP, scryptKeyLen)
	if err != nil {
		return "", nil, ErrInvalidLogin
	}
	if subtle.ConstantTimeCompare(actualHashBytes, expectedHashBytes) != 1 {
		return "", nil, ErrInvalidLogin
	}

	token, err := s.createSession(ctx, userID)
	if err != nil {
		return "", nil, err
	}

	profile, err := s.GetUserByID(ctx, userID)
	if err != nil {
		return "", nil, err
	}
	return token, profile, nil
}

// Authenticate resolves a raw Bearer token to the authenticated user's profile.
func (s *Store) Authenticate(ctx context.Context, rawToken string) (*UserProfile, error) {
	token := strings.TrimSpace(rawToken)
	if token == "" {
		return nil, ErrUnauthorizedToken
	}

	tokenHash := hashToken(token)
	var userID int64
	var expiresAt time.Time
	err := s.db.QueryRowContext(ctx,
		`SELECT user_id, expires_at FROM sessions WHERE token_hash = ?`,
		tokenHash,
	).Scan(&userID, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUnauthorizedToken
	}
	if err != nil {
		return nil, err
	}
	if time.Now().UTC().After(expiresAt) {
		_, _ = s.db.ExecContext(ctx, `DELETE FROM sessions WHERE token_hash = ?`, tokenHash)
		return nil, ErrUnauthorizedToken
	}

	return s.GetUserByID(ctx, userID)
}

// Logout revokes a single session token.
func (s *Store) Logout(ctx context.Context, rawToken string) error {
	token := strings.TrimSpace(rawToken)
	if token == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE token_hash = ?`, hashToken(token))
	return err
}

// GetUserByID fetches only the specified user's own fields.
func (s *Store) GetUserByID(ctx context.Context, userID int64) (*UserProfile, error) {
	var p UserProfile
	var otherDataStr string
	var faction sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT id, nickname, faction, xp, matches_parsed, other_data, created_at, updated_at FROM users WHERE id = ?`,
		userID,
	).Scan(&p.ID, &p.Nickname, &faction, &p.XP, &p.MatchesParsed, &otherDataStr, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUnauthorizedToken
	}
	if err != nil {
		return nil, err
	}
	if faction.Valid {
		p.Faction = faction.String
	}
	if otherDataStr == "" || !json.Valid([]byte(otherDataStr)) {
		otherDataStr = "{}"
	}
	p.OtherData = json.RawMessage(otherDataStr)
	return &p, nil
}

// GetFactionStandings sums member XP per faction. Neutral (NULL faction) accounts are excluded.
func (s *Store) GetFactionStandings(ctx context.Context) (FactionStandings, error) {
	var out FactionStandings
	rows, err := s.db.QueryContext(ctx,
		`SELECT faction, COALESCE(SUM(xp), 0), COUNT(*) FROM users WHERE faction IS NOT NULL GROUP BY faction`,
	)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var faction string
		var standing FactionStanding
		if err := rows.Scan(&faction, &standing.XP, &standing.Members); err != nil {
			return out, err
		}
		switch faction {
		case FactionRadiant:
			out.Radiant = standing
		case FactionDire:
			out.Dire = standing
		}
	}
	return out, rows.Err()
}

// AwardMatchXP atomically increments xp and matches_parsed on the server for verified accepted matches
// and returns (xpAwarded, updatedUserProfile, error).
func (s *Store) AwardMatchXP(ctx context.Context, userID int64, acceptedMatches int) (uint64, *UserProfile, error) {
	if acceptedMatches <= 0 {
		p, err := s.GetUserByID(ctx, userID)
		return 0, p, err
	}

	xpAwarded := uint64(acceptedMatches) * XPPerMatch
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx,
		`UPDATE users SET xp = xp + ?, matches_parsed = matches_parsed + ?, updated_at = ? WHERE id = ?`,
		xpAwarded, acceptedMatches, now, userID,
	)
	if err != nil {
		return 0, nil, err
	}
	profile, err := s.GetUserByID(ctx, userID)
	if err != nil {
		return 0, nil, err
	}
	return xpAwarded, profile, nil
}

func (s *Store) createSession(ctx context.Context, userID int64) (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	rawToken := hex.EncodeToString(buf)
	now := time.Now().UTC()
	expiresAt := now.Add(sessionTTL)

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sessions (token_hash, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)`,
		hashToken(rawToken), userID, now, expiresAt,
	)
	if err != nil {
		return "", err
	}
	return rawToken, nil
}

func hashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
