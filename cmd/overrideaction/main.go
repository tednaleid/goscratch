package main

import (
	"context"
	"fmt"
	"github.com/urfave/cli/v3"
	"os"
)

// CommandOption type for configuring commands
type CommandOption func(*cli.Command)

// setupCommand configures the cli.Commands with specific options for each command
func setupCommand(commandOptions map[string][]CommandOption) *cli.Command {
	app := &cli.Command{
		Commands: []*cli.Command{
			{
				Name:  "echoserver",
				Usage: "Starts an echo server",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:    "port",
						Aliases: []string{"p"},
						Value:   8080,
						Usage:   "Port to run the echo server on",
					},
				},
				Action: func(ctx context.Context, command *cli.Command) error {
					// Default action to start the server
					fmt.Println("Starting echo server...")
					return nil
				},
			},
			{
				Name:  "consume",
				Usage: "Consume messages from a Kafka topic",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "broker",
						Aliases:  []string{"b"},
						Required: true,
						Usage:    "Kafka broker address",
					},
					&cli.StringFlag{
						Name:     "topic",
						Aliases:  []string{"t"},
						Required: true,
						Usage:    "Kafka topic to consume",
					},
				},
				Action: func(ctx context.Context, command *cli.Command) error {
					// Default action to consume from Kafka
					fmt.Println("Consuming from Kafka topic...")
					return nil
				},
			},
		},
	}

	// Apply the options to the specific commands
	for _, command := range app.Commands {
		if opts, exists := commandOptions[command.Name]; exists {
			for _, opt := range opts {
				opt(command)
			}
		}
	}

	return app
}

func main() {
	mockEchoserverAction := func(ctx context.Context, command *cli.Command) error {
		fmt.Println("in echoserver")
		return nil
	}

	mockConsumeAction := func(ctx context.Context, command *cli.Command) error {
		fmt.Println("hello from consume")
		return nil
	}

	commandOptions := map[string][]CommandOption{
		"echoserver": {WithAction(mockEchoserverAction)},
		"consume":    {WithAction(mockConsumeAction)},
	}

	app := setupCommand(commandOptions)
	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Println("Error running app:", err)
	}
}

// WithAction is a helper function to create a CommandOption that sets the action of a command
func WithAction(action cli.ActionFunc) CommandOption {
	return func(c *cli.Command) {
		c.Action = action
	}
}
