package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/nikitabarskov/stars-keeper/internal/starsctl"

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

	ctl, err := starsctl.NewStarsCtl(ctx, githubToken)
	defer ctl.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = ctl.Update(ctx)
	if err != nil {
		log.Fatalf("failed to update stars: %v", err)
	}
}

func getGitHubToken() (string, error) {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		return "", fmt.Errorf("GITHUB_TOKEN environment variable not set")
	}
	return token, nil
}
