// Package store owns the SQLite file and every write the service performs.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/google/uuid"
)

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB
}

var (
	// ErrRolloutConflict means a record with the same namespace, workload and
	// targetVersion already exists and carries different content.
	ErrRolloutConflict = errors.New("rollout conflict")
	// ErrRolloutNotFound means no committed rollout matches the triple.
	ErrRolloutNotFound = errors.New("rollout not found")
)

// Probe is the readiness probe recorded for every batch.
type Probe struct {
	Type             string `json:"type"`
	Path             string `json:"path"`
	Port             int    `json:"port"`
	TimeoutSeconds   int    `json:"timeoutSeconds"`
	SuccessThreshold int    `json:"successThreshold"`
	FailureThreshold int    `json:"failureThreshold"`
}

// ProbeResult is the observed readiness of one batch. Failed batches may carry a reason.
type ProbeResult struct {
	Batch      int    `json:"batch"`
	Ready      bool   `json:"ready"`
	FailedPods int    `json:"failedPods"`
	Reason     string `json:"reason,omitempty"`
}

// Rollout is the immutable record of one rolling upgrade registration.
type Rollout struct {
	ID                  string        `json:"id"`
	Namespace           string        `json:"namespace"`
	Workload            string        `json:"workload"`
	TargetVersion       string        `json:"targetVersion"`
	BatchSize           int           `json:"batchSize"`
	MaxUnavailableRatio float64       `json:"maxUnavailableRatio"`
	CompletedBatches    int           `json:"completedBatches"`
	Probe               Probe         `json:"probe"`
	ProbeResults        []ProbeResult `json:"probeResults"`
	Rollback            bool          `json:"rollback"`
}

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	// A busy timeout lets concurrent writers wait on the write lock instead of
	// failing; the single pooled connection below serialises transactions so a
	// read inside a write transaction never has to upgrade its lock.
	dsn := path
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	dsn += sep + "_pragma=busy_timeout(5000)"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable wal: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Ping reports whether the storage layer is usable.
func (s *Store) Ping() error { return s.db.Ping() }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// CreateRollout registers r for its namespace/workload/targetVersion triple.
// The whole check-and-insert runs in one transaction. On the first valid
// submission r is given a stable ID and created is true. An identical repeat
// leaves storage untouched, replaces *r with the stored record and returns
// created=false; a repeat with different content returns ErrRolloutConflict.
func (s *Store) CreateRollout(ctx context.Context, r *Rollout) (created bool, err error) {
	payload, err := canonicalPayload(r)
	if err != nil {
		return false, err
	}
	r.ID = uuid.NewString()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin rollout tx: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
INSERT INTO rollouts (id, namespace, workload, target_version, payload)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(namespace, workload, target_version) DO NOTHING`,
		r.ID, r.Namespace, r.Workload, r.TargetVersion, payload)
	if err != nil {
		return false, fmt.Errorf("insert rollout: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("insert rollout rows: %w", err)
	}
	if affected == 1 {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit rollout: %w", err)
		}
		return true, nil
	}

	existingID, existingPayload, err := selectRollout(ctx, tx, r.Namespace, r.Workload, r.TargetVersion)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit rollout lookup: %w", err)
	}
	if existingPayload == string(payload) {
		existing, err := decodeRollout(existingID, existingPayload)
		if err != nil {
			return false, err
		}
		*r = *existing
		return false, nil
	}
	return false, ErrRolloutConflict
}

// GetRollout returns the committed record for the triple, or ErrRolloutNotFound.
// Only committed rows are visible.
func (s *Store) GetRollout(ctx context.Context, namespace, workload, targetVersion string) (*Rollout, error) {
	id, payload, err := selectRollout(ctx, s.db, namespace, workload, targetVersion)
	if err != nil {
		return nil, err
	}
	return decodeRollout(id, payload)
}

const rolloutColumns = `SELECT id, payload FROM rollouts WHERE namespace = ? AND workload = ? AND target_version = ?`

type rolloutQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func selectRollout(ctx context.Context, q rolloutQuerier, namespace, workload, targetVersion string) (id, payload string, err error) {
	row := q.QueryRowContext(ctx, rolloutColumns, namespace, workload, targetVersion)
	if scanErr := row.Scan(&id, &payload); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return "", "", ErrRolloutNotFound
		}
		return "", "", fmt.Errorf("select rollout: %w", scanErr)
	}
	return id, payload, nil
}

func decodeRollout(id, payload string) (*Rollout, error) {
	var r Rollout
	if err := json.Unmarshal([]byte(payload), &r); err != nil {
		return nil, fmt.Errorf("decode rollout: %w", err)
	}
	r.ID = id
	return &r, nil
}

func canonicalPayload(r *Rollout) ([]byte, error) {
	copyWithoutID := *r
	copyWithoutID.ID = ""
	payload, err := json.Marshal(&copyWithoutID)
	if err != nil {
		return nil, fmt.Errorf("encode rollout: %w", err)
	}
	return payload, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rollouts (
	id             TEXT PRIMARY KEY,
	namespace      TEXT NOT NULL,
	workload       TEXT NOT NULL,
	target_version TEXT NOT NULL,
	payload        TEXT NOT NULL,
	created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rollouts_triple
	ON rollouts(namespace, workload, target_version);
`
