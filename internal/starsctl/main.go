package starsctl

import (
	"context"
	"crypto/sha512"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/go-github/v69/github"
	"golang.org/x/sync/errgroup"
)

type StarsCtl struct {
	client   *github.Client
	username string
	db       *sql.DB
	closer   func() error
}

const createStarsTable = `
CREATE TABLE IF NOT EXISTS stars (
	id TEXT PRIMARY KEY NOT NULL,
	repository_id INTEGER NOT NULL,
	node_id TEXT NOT NULL,
	starred_at TEXT NOT NULL
)
`

const createRepositoriesTable = `
CREATE TABLE IF NOT EXISTS repositories (
	id TEXT PRIMARY KEY NOT NULL,
	name TEXT NOT NULL,
	owner TEXT NOT NULL,
	number_of_stars INTEGER NOT NULL
)
`

func NewStarsCtl(ctx context.Context, token string) (*StarsCtl, error) {
	client := github.NewClient(nil).WithAuthToken(token)

	configDir, err := os.UserConfigDir()
	if err != nil {
		log.Fatalf("could not get user home directory: %v", err)
	}

	starsKeeperDir := fmt.Sprintf("%s/stars-keeper", configDir)
	log.Printf("configuration folder: %v", starsKeeperDir)

	err = os.MkdirAll(starsKeeperDir, 0755)
	if err != nil {
		log.Fatalf("unexpected error: %s", err)
	}

	dbPath := fmt.Sprintf("%s/%s", starsKeeperDir, "main.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		file, err := os.OpenFile(dbPath, os.O_CREATE|os.O_RDWR, 0644)
		defer func() {
			err := file.Close()
			if err != nil {
				log.Fatalf("unexpected error: %v", err)
			}
		}()
		if err != nil {
			log.Fatalf("could not create database file: %v", err)
		}
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise database: %v", err)
	}

	migrations := []string{
		createStarsTable,
		createRepositoriesTable,
	}

	for migrationIndex, migration := range migrations {
		if _, err := tx.Exec(migration); err != nil {
			return nil, fmt.Errorf("failed to initialise database: %v", err)
		}
		log.Printf("running a transaction: %d out of %d", migrationIndex, len(migrations))
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to initialise database: %v", err)
	}

	return &StarsCtl{
		client: client,
		db:     db,
	}, nil
}

func (s *StarsCtl) Close() error {
	return s.db.Close()
}

func (s *StarsCtl) Update(ctx context.Context) error {
	starredRepositories := make(chan *github.StarredRepository)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(starredRepositories)
		return s.fetchStarredRepositories(ctx, starredRepositories)
	})
	eg.Go(func() error {
		return s.dumpRepositories(ctx, starredRepositories)
	})
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	return nil
}

func (s *StarsCtl) fetchStarredRepositories(ctx context.Context, starredRepositories chan *github.StarredRepository) error {
	opt := &github.ActivityListStarredOptions{
		ListOptions: github.ListOptions{
			PerPage: 50,
		},
	}
	user, _, err := s.client.Users.Get(ctx, "")
	if err != nil {
		return err
	}
	for {
		sr, resp, err := s.client.Activity.ListStarred(ctx, user.GetLogin(), opt)
		if err != nil {
			return err
		}
		for _, repo := range sr {
			select {
			case starredRepositories <- repo:
			case <-ctx.Done():
				return nil
			}
		}
		if resp.NextPage == 0 {
			return nil
		}
		opt.Page = resp.NextPage
	}
}
func (s *StarsCtl) dumpRepositories(ctx context.Context, starredRepositories chan *github.StarredRepository) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	totalNumberOfRepositories := 0

	const insertStarredRepository = `
	INSERT INTO stars (id, repository_id, node_id, starred_at)
	VALUES (?, ?, ?, ?)
	ON CONFLICT(id) DO UPDATE SET
			id=excluded.id,
			node_id=excluded.node_id,
			repository_id=excluded.repository_id,
			starred_at=excluded.starred_at
	`

	for starredRepository := range starredRepositories {
		if starredRepository.GetRepository() == nil {
			return fmt.Errorf("a star does not have an assigned repository, try again")
		}

		input := fmt.Sprintf(
			"%s:%s",
			*starredRepository.GetRepository().NodeID,
			starredRepository.GetStarredAt().Format(time.RFC3339),
		)
		hash := sha512.New()
		_, err := hash.Write([]byte(input))
		if err != nil {
			return fmt.Errorf("failed to save starred repositories to database: %s", err)
		}
		id := hex.EncodeToString(hash.Sum([]byte(input)))

		_, err = tx.Exec(
			insertStarredRepository,
			id,
			starredRepository.GetRepository().NodeID,
			starredRepository.GetRepository().ID,
			starredRepository.GetStarredAt().Format(time.RFC3339),
		)
		if err != nil {
			return fmt.Errorf("failed to save starred repositories to database: %s", err)
		}
		totalNumberOfRepositories += 1
	}

	err = tx.Commit()
	if err != nil {
		if err = tx.Rollback(); err != nil {
			return fmt.Errorf("failed to save starred repositories to database: %s", err)
		}
		return fmt.Errorf("failed to save starred repositories to database: %s", err)
	}
	fmt.Printf("Total number of repositories saved: %d\n", totalNumberOfRepositories)
	return nil
}
