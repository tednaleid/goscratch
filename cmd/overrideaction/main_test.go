package main

import (
	"context"
	"fmt"
	"github.com/urfave/cli/v3"
	"os"
	"testing"
)

func TestEchoServerCommand(t *testing.T) {
	// Mock action for the echoserver command
	mockEchoserverAction := func(ctx context.Context, command *cli.Command) error {
		fmt.Println("in echoserver")
		return nil
	}

	// Create a map for command-specific options, with the mock action for echoserver
	commandOptions := map[string][]CommandOption{
		"echoserver": {WithAction(mockEchoserverAction)},
	}

	app := setupCommand(commandOptions)

	// Simulate CLI arguments for the echoserver command
	args := []string{"mycli", "echoserver", "--port", "8080"}

	// Set the arguments to the app for the test
	app.Writer = os.Stdout // Ensure output can be seen in test logs if needed
	app.ErrWriter = os.Stderr
	app.Run(context.Background(), args)
}
