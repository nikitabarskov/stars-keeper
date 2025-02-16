package starsctl

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/google/go-github/v69/github"
	"github.com/nikitabarskov/stars-keeper/internal/db"
	starsctldb "github.com/nikitabarskov/stars-keeper/internal/db"
	"golang.org/x/sync/errgroup"
)

type StarsCtl struct {
	client *github.Client
	db     *sql.DB
	stars  *db.StarsStorage
	repos  *db.ReposStorage
}

func NewStarsCtl(ctx context.Context, token string) (*StarsCtl, error) {
	client := github.NewClient(nil).WithAuthToken(token)

	configDir, err := os.UserConfigDir()
	if err != nil {
		return nil, fmt.Errorf("could not get user home directory: %v", err)
	}

	starsKeeperDir := fmt.Sprintf("%s/stars-keeper", configDir)
	log.Printf("configuration folder: %v", starsKeeperDir)

	err = os.MkdirAll(starsKeeperDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("unexpected error: %s", err)
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
			return nil, fmt.Errorf("could not create database file: %v", err)
		}
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}

	stars, err := starsctldb.NewStarsStorage(db)
	if err != nil {
		return nil, fmt.Errorf("could not create stars storage: %v", err)
	}

	repos, err := starsctldb.NewReposStorage(db)
	if err != nil {
		return nil, fmt.Errorf("could not create repos storage: %v", err)
	}

	return &StarsCtl{
		client: client,
		stars:  stars,
		repos:  repos,
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
	totalNumberOfRepositories := 0
	for starredRepository := range starredRepositories {
		if starredRepository.GetRepository() == nil {
			return fmt.Errorf("a star does not have an assigned repository, try again")
		}

		star, err := starsctldb.NewStar(starredRepository)
		if err != nil {
			return err
		}
		err = s.stars.CreateOrUpdate(ctx, star)
		if err != nil {
			return err
		}
		repo, err := starsctldb.NewRepo(starredRepository.GetRepository())
		if err != nil {
			return err
		}
		err = s.repos.CreateOrUpdate(ctx, repo)
		if err != nil {
			return err
		}
		totalNumberOfRepositories += 1
	}
	fmt.Printf("Total number of repositories saved: %d\n", totalNumberOfRepositories)
	return nil
}

func (s *StarsCtl) Purge(ctx context.Context) error {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return fmt.Errorf("could not get user home directory: %v", err)
	}

	starsKeeperDir := fmt.Sprintf("%s/stars-keeper", configDir)
	log.Printf("configuration folder: %v", starsKeeperDir)

	err = os.MkdirAll(starsKeeperDir, 0755)
	if err != nil {
		return fmt.Errorf("unexpected error: %s", err)
	}

	dbPath := fmt.Sprintf("%s/%s", starsKeeperDir, "main.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil
	}

	err = os.Remove(dbPath)
	if err != nil {
		return fmt.Errorf("unexpected error: %s", err)
	}

	return nil
}
