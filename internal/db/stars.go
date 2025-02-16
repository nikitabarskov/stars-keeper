package db

import (
	"context"
	"crypto/sha512"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/go-github/v69/github"
)

type Star struct {
	id           string
	starredAt    time.Time
	repositoryId int64
	body         string
}

func NewStar(starredRepository *github.StarredRepository) (*Star, error) {
	input := fmt.Sprintf(
		"%d:%s",
		starredRepository.GetRepository().GetID(),
		starredRepository.GetStarredAt().Format(time.RFC3339),
	)
	hash := sha512.New()
	_, err := hash.Write([]byte(input))
	if err != nil {
		return nil, fmt.Errorf("failed to save starred repositories to database: %s", err)
	}
	id := hex.EncodeToString(hash.Sum([]byte(input)))

	body, err := json.Marshal(starredRepository)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal starred repository: %s", err)
	}

	return &Star{
		id:           id,
		starredAt:    starredRepository.GetStarredAt().Time,
		repositoryId: starredRepository.GetRepository().GetID(),
		body:         string(body),
	}, nil
}

type StarsStorage struct {
	db *sql.DB
}

const createStarTable = `
CREATE TABLE IF NOT EXISTS stars (
	id TEXT PRIMARY KEY,
	starred_at TEXT NOT NULL,
	repository_id TEXT NOT NULL,
	body BLOB NOT NULL
)
`

func NewStarsStorage(db *sql.DB) (*StarsStorage, error) {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	migrations := []string{
		createStarTable,
	}
	for _, migration := range migrations {
		_, err = tx.Exec(migration)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &StarsStorage{db: db}, nil
}

const createOrUpdateStar = `
INSERT INTO stars (id, starred_at, repository_id, body)
VALUES (?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	id=excluded.id,
	starred_at=excluded.starred_at,
	repository_id=excluded.repository_id,
	body=excluded.body
`

func (s *StarsStorage) CreateOrUpdate(ctx context.Context, star *Star) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(
		createOrUpdateRepo,
		star.id,
		star.starredAt.Format(time.RFC3339),
		star.repositoryId,
		star.body,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}
