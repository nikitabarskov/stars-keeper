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

type StarredRepository struct {
	StarredRepository *github.StarredRepository
	Readme            *github.RepositoryContent
}

func (s *StarsCtl) Fetch(ctx context.Context) (chan *github.StarredRepository, error) {
	ch := make(chan *github.StarredRepository)
	user, _, err := s.client.Users.Get(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch user information: %v", err)
	}

	opt := &github.ActivityListStarredOptions{
		ListOptions: github.ListOptions{
			PerPage: 50,
		},
	}

	go func() {
		for {
			starred, response, err := s.client.Activity.ListStarred(ctx, user.GetLogin(), opt)
			if err != nil {
				break
			}

			for _, star := range starred {
				ch <- star
			}

			if response.NextPage == 0 {
				close(ch)
				break
			}
			opt.Page = response.NextPage
		}
	}()

	return ch, nil
}

func (s *StarsCtl) Update(ctx context.Context) error {
	starredRepositories := make(chan *StarredRepository)
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

func (s *StarsCtl) fetchStarredRepositories(ctx context.Context, starredRepositories chan *StarredRepository) error {
	opt := &github.ActivityListStarredOptions{
		ListOptions: github.ListOptions{
			PerPage: 100,
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
		for _, starredRepository := range sr {
			sr := new(StarredRepository)
			if repo := starredRepository.GetRepository(); repo != nil {
				if owner := repo.GetOwner(); owner != nil {
					readme, _, err := s.client.Repositories.GetReadme(ctx, *owner.Login, *repo.Name, nil)
					if err != nil {
						return err
					}
					sr.Readme = readme
				}
			}
			sr.StarredRepository = starredRepository
			select {
			case starredRepositories <- sr:
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
func (s *StarsCtl) dumpRepositories(ctx context.Context, starredRepositories chan *StarredRepository) error {
	for starredRepository := range starredRepositories {
		if starredRepository.StarredRepository == nil {
			return fmt.Errorf("a star does not have an assigned repository, try again")
		}
		star, err := starsctldb.NewStar(starredRepository.StarredRepository)
		if err != nil {
			return err
		}

		err = s.stars.CreateOrUpdate(ctx, star)
		if err != nil {
			return err
		}

		repo, err := starsctldb.NewRepo(starredRepository.StarredRepository.Repository)
		if err != nil {
			return err
		}

		err = s.repos.CreateOrUpdate(ctx, repo)
		if err != nil {
			return err
		}
	}
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
