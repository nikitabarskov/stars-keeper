package main

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

	_ "modernc.org/sqlite"
)

// TODO
// 1. Next step is to save the repository information
//   - [ ] Fetch repository name
//   - [ ] Fetch description, tags, language of the repository
//   - [ ] Fetch the content of README.md if present
// 2. Keep the results in SQLite database instance
// 3. Use viper for configuration management (token for GitHub / ...)
// 4. Add CLI
// 5. Think about structure
// 6. ...

func main() {
	ctx := context.Background()

	githubToken, err := getGitHubToken()
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}

	db, err := initializeDatabase(ctx)
	defer func() {
		err := db.Close()
		if err != nil {
			log.Fatalf("unexpected error: %v", err)
		}
	}()

	if err != nil {
		log.Fatalf("can not initialise database: %s", err)
	}

	starredRepositories := make(chan *github.StarredRepository)

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		log.Printf("start fetching repositories")
		err := fetchStarredRepositories(ctx, githubToken, starredRepositories)
		close(starredRepositories)
		if err != nil {
			return err
		}
		log.Printf("successfully fetched repositories")
		return nil
	})

	eg.Go(func() error {
		log.Printf("start saving repositories")
		err := saveRepositoriesToDb(ctx, db, starredRepositories)
		if err != nil {
			return err
		}
		log.Printf("successfully saved repositories")
		return nil
	})

	if err := eg.Wait(); err != nil {
		log.Fatalf("failed to fetch starred repositories: %v", err)
	}
}

func getGitHubToken() (string, error) {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		return "", fmt.Errorf("GITHUB_TOKEN environment variable not set")
	}
	return token, nil
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

func initializeDatabase(ctx context.Context) (*sql.DB, error) {
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

	return db, nil
}

func fetchStarredRepositories(ctx context.Context, token string, starredRepositories chan *github.StarredRepository) error {
	client := github.NewClient(nil).WithAuthToken(token)
	opt := &github.ActivityListStarredOptions{
		ListOptions: github.ListOptions{
			PerPage: 50,
		},
	}
	for {
		sr, response, err := client.Activity.ListStarred(ctx, "nikitabarskov", opt)
		if err != nil {
			return err
		}
		for _, r := range sr {
			if r != nil {
				starredRepositories <- r
			}
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
	}
	return nil
}

const insertStarredRepository = `
INSERT INTO stars (id, repository_id, node_id, starred_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	id=excluded.id,
	node_id=excluded.node_id,
	repository_id=excluded.repository_id,
	starred_at=excluded.starred_at
`

func saveRepositoriesToDb(ctx context.Context, db *sql.DB, starredRepositories chan *github.StarredRepository) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

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
	}

	err = tx.Commit()
	if err != nil {
		if err = tx.Rollback(); err != nil {
			return fmt.Errorf("failed to save starred repositories to database: %s", err)
		}
		return fmt.Errorf("failed to save starred repositories to database: %s", err)
	}
	return nil
}
