package db

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/google/go-github/v69/github"
)

type Repo struct {
	id          int64
	description string
	topics      []string
	body        string
}

func NewRepo(repository *github.Repository) (*Repo, error) {
	body, err := json.Marshal(repository)
	if err != nil {
		return nil, err
	}
	return &Repo{
		id:          repository.GetID(),
		description: repository.GetDescription(),
		topics:      repository.Topics,
		body:        string(body),
	}, nil
}

type ReposStorage struct {
	db *sql.DB
}

const createRepoTable = `
CREATE TABLE IF NOT EXISTS repositories (
	id TEXT PRIMARY KEY,
	body BLOB NOT NULL
)
`

const alterRepoTableAddDescription = `
ALTER TABLE repositories ADD COLUMN description TEXT DEFAULT NULL
`

const alterRepoTableAddTopics = `
ALTER TABLE repositories ADD COLUMN topics BLOB DEFAULT NULL
`

func NewReposStorage(db *sql.DB) (*ReposStorage, error) {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	migrations := []string{
		// createRepoTable,
		// alterRepoTableAddDescription,
		// alterRepoTableAddTopics,
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

	return &ReposStorage{db: db}, nil
}

const createOrUpdateRepo = `
INSERT INTO repositories (id, body)
VALUES (?, ?)
ON CONFLICT(id) DO UPDATE SET
	id=excluded.id,
	body=excluded.body
`

func (r *ReposStorage) CreateOrUpdate(ctx context.Context, repo *Repo) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(
		createOrUpdateRepo,
		repo.id,
		repo.body,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}
